from os import getenv
from json import loads 
from pathlib import Path 
from threading import Thread
from paramiko import SSHClient, AutoAddPolicy

from expK8.remoteFS.NodeException import BlockDeviceNotFound, NoValidPartitionFound, RemoteRuntimeError


"""According to channel documentation, the call to recv_exit_status can hang indefinitely 
if the channel has not yet received any bytes from remote node where command was run. If we
directly call recv(nbytes) and read bytes, we will corrupt the output while it would ensure 
that recv_exit_status never hands. So we use a new thread to call exec_command and if it takes too 
long to finish it can be killed. 

Ref: https://docs.paramiko.org/en/stable/api/channel.html (last checked 16/07/2023)
"""
class ExecCommandThread(Thread):
    def __init__(
        self, 
        ssh_client: SSHClient, 
        command_str_arr: list
    ) -> None:
        """Create a thread to execute a command in remote node and wait for its completion."""
        Thread.__init__(self)
        self.ssh_client = ssh_client
        self.command_str_arr = command_str_arr
        self.exit_code = -1 
        self.stdout = ''
        self.stderr = ''


    def run(self):
        """Run a command in remote node and wait for it to return."""
        _, stdout, stderr = self.ssh_client.exec_command(" ".join(self.command_str_arr))
        self.stdout = stdout.read().decode("utf-8")
        self.stderr = stderr.read().decode("utf-8")
        self.exit_code = stdout.channel.recv_exit_status()


class Node:
    """This class allows communication with a remote node it is connected to.

    Attributes:
        name: Name of the node, not necessarily unique for remote nodes.  
        host: Host name of the node, identifies a unique remote node. 
        machine_name: Name of the type of machine used as node. 

        _cred_dict: Dictionary of credentials.
        _mount_list: List of valid mounts 
        _port: Port used to connect to the remote node. 
        _home: String path of home directoy. 
        _ssh: SSHClient to connect to remote node. 
        _ssh_exception: Exception raised when connectign to remote node. 
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
        self.machine_name = self.host.split("-")[0]

        self._cred_dict = cred_dict 
        self._mount_list = mount_list 
        self._port = 22 if "port" not in self._cred_dict else self._cred_dict["port"]

        self._home = None
        self._ssh = None # SSH client to connect to remote host. 
        self._ssh_exception = None # Any exception raised when trying to connect to remote host. 
        self._connect()


    def _connect(self) -> None:
        """Connect to the remote host and setup all the mounts."""
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
            self._home = self._get_home_dir()
            self._mount()
        except Exception as e:
            self._ssh_exception = e
            print("Exception in connecting to node: {}, {}".format(self.host, e))


    def set_perm_for_power_tracking(self):
        """Set permissions in this node to allow for measuring of power usage."""
        chmod_cmd = "sudo chmod -R a+r /sys/class/powercap/intel-rapl"
        exit_code, stdout, stderr = self.exec_command(chmod_cmd.split(' '))
        if exit_code:
            raise RemoteRuntimeError(chmod_cmd, self.host, exit_code, stdout, stderr)
    

    def is_live(self):
        return not self._ssh_exception

    
    def format_path(
        self, 
        path_str: str
    ) -> str:
        """Track the use of ~ in path string submitted to the remote node and replace it with the path 
        of the home directory of remote node. 

        Args:
            path_str: The path in remote node. 
        
        Returns:
            new_path_str: The path with '~' replaced by the home directory. 
        """
        return path_str if '~' not in path_str else path_str.replace('~', self._home)
    

    def clone_git_repo(
        self,
        dir_path: str, 
        git_repo_url: str 
    ) -> None:
        """Clone a git repo in remote node. 
        
        Args:
            dir_path: Path to directory where repo is cloned. 
            git_repo_url: URL of the git repo. 
        """
        git_clone_cmd = "git clone {} {}".format(git_repo_url, dir_path)
        stdout, stderr, exit_code = self.exec_command(git_clone_cmd.split(' '))
        if exit_code:
            raise RemoteRuntimeError(git_clone_cmd, self.host, exit_code, stdout, stderr)


    def get_file_size(
        self, 
        path: str
    ) -> int:
        """Get size of file in this node. 

        Args:
            path: Path to file whose size we need. 

        Returns:
            size_byte: Size of file in bytes. 
        """
        if not self.file_exists(self.format_path(path)):
            return 0
        else:
            get_file_size_cmd = ["stat", "-c", "%s", self.format_path(path)]
            stdout, stderr, exit_code = self.exec_command(get_file_size_cmd)
            if exit_code:
                raise RemoteRuntimeError(get_file_size_cmd, self.host, exit_code, stdout, stderr)
            return int(stdout)
    

    def nonblock_exec_cmd(
        self,
        cmd: str 
    ) -> None:
        """Execute a command without waiting for return code.

        Args:
            cmd: The command to run in remote node. 
        """
        _, stdout, stderr = self._ssh.exec_command(' '.join(cmd))
    

    def get_file_list_in_dir(
        self,
        dir_path: str 
    ) -> list:
        assert self.dir_exists(dir_path), "{}: Not a directory {}".format(self.host, dir_path)
        find_cmd = ["find", self.format_path(dir_path)]
        stdout, stderr, exit_code = self.exec_command(find_cmd)
        if exit_code:
            raise RemoteRuntimeError(find_cmd, self.host, exit_code, stdout, stderr)


    def scp(
        self,
        local_path: str, 
        remote_path: str
    ) -> None:
        """Transfer local file to remote node.

        Args:
            local_path: Local path of file to upload. 
            remote_path: Target path in remote node. 
        """
        sftp = self._ssh.open_sftp()
        sftp.put(local_path, remote_path)
        sftp.close()


    def download(
        self,
        remote_path: str,
        local_path: str
    ) -> None:
        """Transfer local file to remote node.

        Args:
            local_path: Local path of file to upload. 
            remote_path: Target path in remote node. 
        """
        sftp = self._ssh.open_sftp()
        sftp.get(remote_path, local_path)
        sftp.close()


    def file_exists(
        self,
        node_path: str
    ) -> None:
        """Check if a given path exist in the remote node.
        
        Args:
            node_path: Path in the remote node.
        """
        check_path_cmd = ["test", "-f", self.format_path(node_path)]
        _, _, exit_code = self.exec_command(check_path_cmd)
        if exit_code == 1:
            return False 
        else:
            return True 
    

    def touch(
        self,
        path: str 
    ) -> None:
        """Touch a file in remote node. 

        Args:
            path: Path of file to be created using touch command. 
        """
        touch_cmd = ["touch", self.format_path(path)]
        stdout, stderr, exit_code = self.exec_command(touch_cmd)
        if exit_code:
            raise RemoteRuntimeError(touch_cmd, self.host, exit_code, stdout, stderr)


    def dir_exists(
        self,
        node_path: str
    ) -> None:
        """Check if a given path exist in the remote node.
        
        Args:
            node_path: Path in the remote node.
        """
        check_path_cmd = ["test", "-d", self.format_path(node_path)]
        _, _, exit_code = self.exec_command(check_path_cmd)
        if exit_code == 1:
            return False 
        else:
            return True 
    

    def create_random_file_nonblock(
        self,
        file_path: str, 
        file_size_mb
    ) -> None:
        """Creates a random file in this remote node. 

        Args:
            file_path: Path of file to be created. 
            file_size_byte: Size of file to be created. 
        """
        create_random_file_cmd = ["nohup",
                                    "dd",
                                    "if=/dev/urandom",
                                    "of={}".format(file_path),
                                    "bs=1M",
                                    "count={}".format(file_size_mb),
                                    "oflag=direct"]
        _, stdout, stderr = self._ssh.exec_command(" ".join(create_random_file_cmd))


    def chown(
        self, 
        path: str 
        ) -> None:
        """Change ownership of a remote directory. 

            Args:
                path: The path to change the ownership. 
            
            Raises:
                RemoteRuntimeError: If the chown command fails in remote node. 
        """
        chown_cmd = ["sudo", "chown", self._cred_dict["user"], path]
        stdout, stderr, exit_code = self.exec_command(chown_cmd)
        if exit_code:
            raise RemoteRuntimeError(chown_cmd, self.host, exit_code, stdout, stderr)
        

    def cat(
        self, 
        path: str 
        ) -> None:
        """Read contents of a file in Node. 

            Args:
                path: The path to read. 
            
            Raises:
                RemoteRuntimeError: If the chown command fails in remote node. 
        """
        chown_cmd = ["cat", path]
        stdout, stderr, exit_code = self.exec_command(chown_cmd)
        if exit_code:
            raise RemoteRuntimeError(chown_cmd, self.host, exit_code, stdout, stderr)
        return stdout.rstrip()


    def mkfs(
        self,
        fs_type: str,
        device_path: str 
    ) -> None:
        """Create a filesystem in a remote node in the specified block device. 

        Args:
            fs_type: The type of filesystem to create. Valid values are ext3, ext5, xfs. 
            device_path: Path to the block device where the filesystem will be created. 
        
        Raises:
            RemoteRuntimeError: If the mkfs command fails in the remote node. 
        """
        mkfs_cmd = ["yes", "|", "sudo", "mkfs", "-t", fs_type, device_path]
        stdout, stderr, exit_code = self.exec_command(mkfs_cmd)
        if exit_code:
            raise RemoteRuntimeError(mkfs_cmd, self.host, exit_code, stdout, stderr)


    def mkdir(
        self, 
        dir_path: str 
    ) -> None:
        """Create the specified directory. 

        Args:
            dir_path: Path of the directory to create. 
        """
        mkdir_cmd = ["mkdir", "-p", self.format_path(dir_path)]
        stdout, stderr, exit_code = self.exec_command(mkdir_cmd)
        if exit_code:
            raise RemoteRuntimeError(mkdir_cmd, self.host, exit_code, stdout, stderr)


    def mount(
        self, 
        block_device_path: str, 
        mount_path: str
    ) -> None: 
        """Mount a block device to a directory in this remote node. 

        Args:
            block_device_path: Path to the block device to mount. 
            mount_path: Target directory to mount the block device. 
        """
        mount_cmd = ["sudo", "mount", block_device_path, mount_path]
        stdout, stderr, exit_code = self.exec_command(mount_cmd)
        if exit_code:
            raise RemoteRuntimeError(mount_cmd, self.host, exit_code, stdout, stderr)


    def _get_block_device(
        self,
        device_name: str
    ) -> dict:
        """Get the information of a block device in remote node. 

        Args:
            device_name: Device name to get information from in remote node. 
        
        Raises:
            BlockDeviceNotFound: If there is no block device with specified name in this remote node. 
        """
        block_device_info = {}
        block_device_list = self.get_block_devices()
        for cur_block_device_info in block_device_list["blockdevices"]:
            if cur_block_device_info["name"] == device_name:
                block_device_info = cur_block_device_info
                break 
        else:
            raise BlockDeviceNotFound(self.host, device_name, [_["name"] for _ in block_device_list["blockdevices"]])
        return block_device_info
    

    def get_size_gb_from_size_str(
        self,
        size_str: str
    ) -> float:
        """Get the size in GB as a float from a size string.

        Args:
            size_str: The size string to convert to a float GB value.
        
        Return:
            float_gb: The size in GB as a float generated from a size string. 
        
        Raises:
            ValueError: If the size string could not be parsed to generate a size in GB as a float. 
        """
        partition_size_gb = 0.0
        if 'K' in size_str:
            partition_size_gb = float(size_str.replace("M", ""))/(1024.0**2)
        elif 'M' in size_str:
            partition_size_gb = float(size_str.replace("M", ""))/1024.0
        elif 'G' in size_str:
            partition_size_gb = float(size_str.replace("G", ""))
        elif 'T' in size_str:
            partition_size_gb = float(size_str.replace("T", "")) * 1024.0
        else:
            raise ValueError("Cannot parse the size string: {}".format(size_str))
        
        return partition_size_gb
    

    def get_mountpoint_info(
        self,
        mountpoint: str 
    ) -> dict:
        """Get the information of a mountpoint

        Args:
            mountpoint: Path where the block device is mounted. 
        
        Returns:
            info: Dictionary containing information of the mountpoint. 
        """
        mount_info = {}
        mount_path = self.format_path(mountpoint)
        block_device_list = self.get_block_devices()
        for cur_block_device_info in block_device_list["blockdevices"]:
            if cur_block_device_info["mountpoint"] == mount_path:
                mount_info = cur_block_device_info
                break 
            else:
                if "children" not in cur_block_device_info:
                    break 

                for partition_info in cur_block_device_info["children"]:
                    if partition_info["mountpoint"] == mount_path:
                        mount_info = partition_info
                        break 
        return mount_info

    
    def get_mountpoint_size_gb(
        self, 
        mountpoint: str 
    ) -> float:
        """Get the size of the partition/block device of the mountpoint. 

        Args:
            mountpoint: Path where the block device is mounted. 
        
        Returns:
            size_gb: Size in GB of the mountpoint. 
        """
        size_gb = 0 
        mount_path = self.format_path(mountpoint)
        block_device_list = self.get_block_devices()
        for cur_block_device_info in block_device_list["blockdevices"]:
            if cur_block_device_info["mountpoint"] == mount_path:
                size_gb = int(cur_block_device_info["size"]/(1024**3))
                break 
            else:
                if "children" not in cur_block_device_info:
                    break 

                for partition_info in cur_block_device_info["children"]:
                    if partition_info["mountpoint"] == mount_path:
                        size_gb = int(cur_block_device_info["size"]/(1024**3))
        return size_gb


    def _mount(self) -> None:
        """Create FS in devices and mount them to mountpoints.  

        Raises:
            BlockDeviceNotFound: If no block device with the specified name exists in the remote node. 
            NoValidPartitionFound: If no partition found to satify the requirement. 
        """
        for mount_info in self._mount_list:
            device_name = mount_info["device"]
            mount_size_gb = mount_info["size_gb"]
            mount_path = self.format_path(mount_info["mountpoint"])

            block_device_info = self._get_block_device(device_name)
            block_device_size_gb = int(block_device_info["size"]/(1024**3))
            if mount_size_gb > block_device_size_gb:
                raise NoValidPartitionFound(self.host, mount_info, block_device_info)
            
            self.mkdir(mount_path)
            self.chown(mount_path)

            if "children" not in block_device_info:
                if block_device_info["mountpoint"] is None:
                    block_device_path = "/dev/{}".format(device_name)
                    self.mkfs("ext4", block_device_path)
                    self.mount(block_device_path, mount_path)
                elif block_device_info["mountpoint"] == mount_path:
                    pass 
                else:
                    NoValidPartitionFound(self.host, mount_info, block_device_info)
            else:
                partition_list = block_device_info["children"]
                valid_partition_list = []
                for partition_info in partition_list:
                    partition_size_gb = int(partition_info["size"]/(1024**3))
                    if partition_size_gb < mount_size_gb:
                        continue 
                    
                    partition_mountpoint = partition_info["mountpoint"]
                    if partition_mountpoint is None:
                        valid_partition_list.append(partition_info)
                    elif partition_mountpoint == mount_path:
                        break
                else:
                    if len(valid_partition_list):
                        valid_partition = valid_partition_list[0]
                        block_device_path = "/dev/{}".format(valid_partition["name"])
                        self.mkfs("ext4", block_device_path)
                        self.mount(block_device_path, mount_path)
                    else:
                        raise NoValidPartitionFound(self.host, mount_info, block_device_info)
    

    def rm(
        self,
        path: str 
    ) -> None:
        """Remove a file. 

        Args:
            path: Path to remove. 
        """
        rm_cmd = ["sudo", "rm", "-rf", path]
        stdout, stderr, exit_code = self.exec_command(rm_cmd)
        if exit_code:
            raise RemoteRuntimeError(rm_cmd, self.host, exit_code, stdout, stderr)


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
        else:
            raise RemoteRuntimeError(command_str_arr, self.host, exit_code, stdout, stderr)
        return stdout, stderr, exit_code


    def get_block_devices(self) -> dict:
        """Get a dictionary output of 'lsblk' command in remote node. 

        Return:
            lsblk_dict: Dictionary output of 'lsblk' command in remote node. 
        """
        lsblk_cmd = ["lsblk", "-b", "--json"]
        stdout, stderr, exit_code = self.exec_command(lsblk_cmd)
        if exit_code:
            raise RemoteRuntimeError(lsblk_cmd, self.host, exit_code, stdout, stderr)
        return loads(stdout)
    

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
    

    def ps(
        self
    ) -> str:
        """Get details of the processes with similar commands.
        
        Return:
            stdout: Output of 'ps a' command in remote node. 
        """
        ps_cmd = ["ps", "a", "-u", self._cred_dict["user"]]
        stdout, stderr, exit_code = self.exec_command(ps_cmd)
        if exit_code:
            raise RemoteRuntimeError(ps_cmd, self.host, exit_code, stdout, stderr)
        
        return stdout.rstrip()


    def kill(
        self,
        pid: int
    ) -> bool:
        """Kill a process in this node.

        Args:
            pid: The PID to be killed. 
        """
        kill_cmd = ["sudo", "kill", "-9", str(pid)]
        stdout, stderr, exit_code = self.exec_command(kill_cmd)
        if exit_code:
            raise RemoteRuntimeError(kill_cmd, self.host, exit_code, stdout, stderr)
    

    def match_kill(
        self,
        match_substr: str 
    ) -> list:
        kill_list = []
        ps_output = self.ps()
        for ps_row in ps_output.split("\n"):
            if match_substr in ps_row:
                pid = int(ps_row.strip().split(' ')[0])
                self.kill(pid)
                ps_output.append(ps_row)
        return kill_list
    

    def find_all_files_in_dir(
            self,
            remote_dir_path: str 
    ) -> list:
        """Find all the files in this directory and all its subdirectories. 

        Args:
            remote_dir_path: Path of the remote directory to search. 
        
        Returns:
            Array of absolute paths of files in the directory and its subdirectories. 

        Raises:
            RemoteRuntimeError: Raised if remote command failed. 
        """
        find_files_cmd = ["find", remote_dir_path, "-type", "f"]
        stdout, stderr, exit_code = self.exec_command(find_files_cmd)
        if exit_code:
            raise RemoteRuntimeError(find_files_cmd, self.host, exit_code, stdout, stderr)
        return stdout.rstrip().split("\n")


    def local_path_map(
            self,
            remote_data_file_path: Path,
            remote_data_dir_path: Path,
            local_data_dir_path: Path = Path("/research2/mtc/cp_traces/pranav")
    ) -> Path:
        """Get local file path from remote file path.
        
        Args:
            remote_data_file_path: Path of data file in remote node. 
            remote_data_dir_path: Path of remote data directory. 
            local_data_dir_path: Path of local data directory. 
        
        Returns:
            local_path: The local path that maps to the provided remote path. 
        
        Raises:
            ValueError: Raised if the paths provided are not absolute, data directory is not part of the data file path
                            and local data directory does not exist. 
        """
        if not remote_data_dir_path.is_absolute() or not remote_data_dir_path.is_absolute():
            raise ValueError("All paths provided must be absolute. Paths -> {}, {}".format(remote_data_file_path, remote_data_dir_path))

        remote_data_dir_absolute_str = str(remote_data_dir_path.absolute())
        remote_data_file_path_absolute_str = str(remote_data_file_path.absolute())

        if not remote_data_dir_absolute_str in remote_data_file_path_absolute_str:
            raise ValueError("Remote file path {} does not contain remote data dir {}.".format(remote_data_file_path_absolute_str, remote_data_dir_absolute_str))

        if not local_data_dir_path.exists():
            raise ValueError("Local data directory {} does not exist.".format(local_data_dir_path))
        
        remote_file_path_subdir_str = remote_data_file_path_absolute_str.replace(remote_data_dir_absolute_str, '')
        if remote_file_path_subdir_str[0] == '/':
            remote_file_path_subdir_str = remote_file_path_subdir_str[1:]
        return local_data_dir_path.joinpath(remote_file_path_subdir_str)
    

    def sync_dir(
            self,
            remote_dir_path: str, 
            local_dir_path: str 
    ) -> None:
        remote_file_list = self.find_all_files_in_dir(remote_dir_path)
        for remote_file_path in remote_file_list:
            remote_data_path = Path(remote_file_path)
            local_path = self.local_path_map(remote_data_path, Path(self.format_path(remote_dir_path)), local_data_dir_path=Path(local_dir_path))
            
            remote_file_size = self.get_file_size(remote_file_path)
            local_file_size = local_path.stat().st_size if local_path.exists() else 0 
            if remote_file_size != local_file_size:
                print("Downloading {} to {} as file sizes are different {} vs {}.".format(remote_file_path, local_path, remote_file_size, local_file_size))
                local_path.parent.mkdir(exist_ok=True, parents=True)
                self.download(remote_file_path, local_path)
            else:
                print("File aready good {}, {}.".format(remote_file_path, local_path))