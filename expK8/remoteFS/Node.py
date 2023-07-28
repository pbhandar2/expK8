import json 
from threading import Thread
from os import getenv
from pathlib import Path 
from paramiko import SSHClient, AutoAddPolicy


"""According to channel documentation, the call to recv_exit_status can hang indefinitely 
if the channel has not yet received any bytes from remote ndoe where command was run. If we
directly call recv(nbytes) and read bytes, we will corrupt the output while it would ensure 
that recv_exit_status never hands. So we use a new thread to call exec_command and if it hands
for a long time it can be killed. 

Ref: https://docs.paramiko.org/en/stable/api/channel.html (last checked 16/07/2023)
"""
class ExecCommandThread(Thread):
    def __init__(self, ssh_client, command_str_arr):
        Thread.__init__(self)
        self.ssh_client = ssh_client
        self.command_str_arr = command_str_arr

    def run(self):
        _, stdout, stderr = self.ssh_client.exec_command(" ".join(self.command_str_arr))
        self.stdout = stdout.read().decode("utf-8")
        self.stderr = stderr.read().decode("utf-8")
        self.exit_code = stdout.channel.recv_exit_status()


class RemoteRuntimeError(Exception):
    """Exception raised when the we receive a nonzero exit code from a remote process. 
    """
    def __init__(
            self, 
            remote_cmd: list,
            host_name: str, 
            exit_code: int, 
            stdout: str,
            stderr: str
    ) -> None:
        super().__init__("cmd: {} failed in host{} with exit code {} () \n stdout \n {} \n stderr \n {}".format(" ".join(remote_cmd), host_name, exit_code, stdout, stderr))


class Node:
    """Node represents a remote node that RemoteFS maintains a connection to.
    """
    def __init__(
            self,
            node_name: str,
            host_name: str,
            cred_dict: dict,
            mount_list: list 
    ) -> None:
        self.name = node_name 
        self.host = host_name 
        self._cred_dict = cred_dict 
        self._mount_list = mount_list 
        self._port = 22 if "port" not in self._cred_dict else self._cred_dict["port"]

        self._home = None 
        self._ssh = None 
        self._ssh_exception = None 
        self.root_partition = None 
        self._connect()
    

    def _get_block_device(
        self,
        device_name: str
    ) -> dict:
        """Get the information of a block device in remote node. 

        Args:
            device_name: Device name to get information from in remote node. 
        """
        block_device_info = {}
        block_device_list = self.get_block_devices()
        for cur_block_device_info in block_device_list["blockdevices"]:
            if cur_block_device_info["name"] == device_name:
                block_device_info = cur_block_device_info
                break 
        else:
            raise RuntimeError("Specified device {} does not exist.".format(device_name))
        return block_device_info
    

    def is_mounted(
        self,
        block_device_info: dict,
        mountpoint: str, 
        size_gb: int 
    ) -> bool:
        """Check if the specified mountpoint in the block device is mounted in remote node. 

        Return:
            mounted: Boolean indicating if the mountpoint exists in the block device in remote node. 
        """
        if "mountpoint" in block_device_info:
            if mountpoint == block_device_info["mountpoint"]:
                return True 
        
        if "children" not in block_device_info:
            return False
        
        mounted = False 
        partition_list = block_device_info["children"]
        for partition_info in partition_list:
            if partition_info["mountpoint"] == mountpoint:
                partition_size_str = partition_info["size"]
                if "M" in partition_size_str:
                    raise RuntimeError("The partition at mountpoint is too small. {}".format(partition_size_str))
                elif "G" in partition_size_str:
                    partition_size_gb = int(partition_size_str.replace("G", ""))
                
                if partition_size_gb >= size_gb:
                    mounted = True 
                    break 
                else:
                    raise RuntimeError("The partition at mountpoint is too small. {}".format(partition_size_str))
        
        return mounted 


    def _mount(self) -> None:
        """Create the required partitions with a new FS and mount it to the path specified in the 
        configuration. 
        """
        block_device_list = self.get_block_devices()
        for mount_info in self._mount_list:
            device_name = mount_info["device"]
            block_device_info = self._get_block_device(device_name)

            if "~" in mount_info["mountpoint"]:
                mount_point_without_home = mount_info["mountpoint"].replace("~/", "")
                mount_path ="{}/{}".format(self._home, mount_point_without_home)
            else:
                mount_path = mount_info["mountpoint"]

            size_gb = mount_info["size_gb"]
            mkdir_mount_path_cmd = ['mkdir', '-p', mount_path]
            print(mkdir_mount_path_cmd)
            stdout, stderr, exit_code = self.exec_command(mkdir_mount_path_cmd)
            if exit_code:
                raise RemoteRuntimeError(mkdir_mount_path_cmd, self.host, exit_code, stdout, stderr)

            print("Remote absolute mountpoint: {}".format(mount_path))
            if not self.is_mounted(block_device_info, mount_path, size_gb):
                mkfs_cmd = ["sudo", "mkfs", "-t", "ext4", "/dev/{}".format(device_name)]
                print(mkfs_cmd)
                stdout, stderr, exit_code = self.exec_command(mkfs_cmd)
                if exit_code:
                    raise RemoteRuntimeError(mkfs_cmd, self.host, exit_code, stdout, stderr)
                
                print("{} FS created!".format(mount_info))
                mount_cmd = ["sudo", "mount", "/dev/{}".format(device_name), mount_path]
                print(mount_cmd)
                stdout, stderr, exit_code = self.exec_command(mount_cmd)
                if exit_code:
                    raise RemoteRuntimeError(mount_cmd, self.host, exit_code, stdout, stderr)
                
                print("FS mounted at {}".format(mount_path))
            else:
                print("{} already setup!".format(mount_info))

            chown_cmd = ['sudo', 'chown', self._cred_dict['user'], mount_path]
            print(chown_cmd)
            stdout, stderr, exit_code = self.exec_command(chown_cmd)
            if exit_code:
                raise RemoteRuntimeError(chown_cmd, self.host, exit_code, stdout, stderr)


    def exec_command(
            self, 
            command_str_arr: list,
            timeout: float = None,
            num_retry: int = 5
    ) -> tuple:
        """Run a command in the node with a given name. 

        Args:
            host_name: host_name of the node where command will be executed. 
            command_str_arr: Array of str representing the command ['ls', '-lh'].
            timeout: Seconds to wait before before a remote command times out. 
        
        Return:
            The result of running the command on remote node represented by a tuple of (stdout, stderr, exit code). 

        Raises:
            SSHException: if the sever fails to execute the command
            ValueError: if no node with specified name is connected 
        """
        exit_code, stdout, stderr = None, "", ""
        for cur_num_retry in range(num_retry):
            thread = ExecCommandThread(self._ssh, command_str_arr)
            thread.start()
            thread.join(timeout)
            if not thread.is_alive():
                exit_code = thread.exit_code
                stdout = thread.stdout
                stderr = thread.stderr
                break 
            else:
                print("Thread timed out for command {}, retry remaining {}".format(command_str_arr, num_retry - 1 - cur_num_retry))
                thread.set()
                thread.join()
        else:
            raise RemoteRuntimeError(command_str_arr, self.host, exit_code, stdout, stderr)
        return stdout, stderr, exit_code
    

    def get_block_devices(self) -> dict:
        """Get a dictionary output of 'lsblk' command in remote node. 

        Return:
            lsblk_dict: Dictionary output of 'lsblk' command in remote node. 
        """
        lsblk_cmd = ["lsblk", "--json"]
        stdout, stderr, exit_code = self.exec_command(lsblk_cmd)
        if exit_code:
            raise RemoteRuntimeError(lsblk_cmd, self.host, exit_code, stdout, stderr)
        return json.loads(stdout)
    

    def check_connection(self) -> None:
        """Check if the SSH connection to the node is active. 

        Return:
            stats: Boolean to indicate if the SSH connection is active. 
        """
        try:
            transport = self._ssh.get_transport() 
            return transport and transport.is_active()
        except ValueError:
            return False 
    

    def _get_home_dir(self) -> None:
        """Get the home directory of this remote node. 

        Return:
            home_dir: Path of the home directory in this remote node. 
        
        Raise:
            RemoteRuntimeError: When a command fails in remote node. 
        """
        home_cmd = ["echo", "$HOME"]
        stdout, stderr, exit_code = self.exec_command(home_cmd)
        if exit_code:
            raise RemoteRuntimeError(home_cmd, self.host, exit_code, stdout, stderr)
        return stdout.rstrip()


    def _connect(self) -> None:
        """Connect to the remote host and setup all the mounts. 
        """
        try:
            self._ssh = SSHClient()
            self._ssh.set_missing_host_key_policy(AutoAddPolicy())
            if self._cred_dict["type"] == "env":
                self._ssh.connect(
                    self.host, 
                    self._port, 
                    username=self._cred_dict["user"], 
                    password=getenv(self._cred_dict["val"]))
            else:
                if "~" in self._cred_dict["val"]:
                    key_path = str(Path.home().joinpath(self._cred_dict["val"].replace("~/", "")))
                else:
                    key_path = str(Path(self._cred_dict["val"]))
                self._ssh.connect(self.host, username=self._cred_dict["user"], key_filename=key_path)
            print("Connected to host {}".format(self.host))
            self._home = self._get_home_dir()
            self._mount()
        except Exception as e:
            self._ssh_exception = e