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
            remote_cmd: list[str],
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

        self._ssh = None 
        self.up = False 
        self._connect()


    def _mount(self):
        block_device_list = self.get_block_devices()
        for mount_info in self._mount_list:
            device_name = mount_info["device"]

            block_device_info = {}
            for cur_block_device_info in block_device_list["blockdevices"]:
                if cur_block_device_info["name"] == device_name:
                    block_device_info = cur_block_device_info
                    break 
            else:
                raise RuntimeError("Specified device {} does not exist.".format(device_name))
            
            # check if there is any non-root partition in this device that we can use 
            if "children" in block_device_info:
                for partition_info in block_device_info["children"]:
                    if partition_info["mountpoint"] == "/":
                        




    def exec_command(
            self, 
            command_str_arr: list[str],
            timeout: float = None,
            num_retry: int = 5
    ) -> tuple[str, str, int]:
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
    

    def get_block_devices(self):
        lsblk_cmd = ["lsblk", "--json"]
        stdout, stderr, exit_code = self.exec_command(lsblk_cmd)
        if exit_code:
            raise RemoteRuntimeError(lsblk_cmd, self.host, exit_code, stdout, stderr)
        return json.loads(stdout)
    

    def check_connection(self):
        try:
            transport = self._ssh.get_transport() 
            return transport and transport.is_active()
        except ValueError:
            return False 


    def _connect(self):
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
                key_path = str(Path(self._cred_dict["val"]).absolute())
                self._ssh.connect(self.host, username=self._cred_dict["user"], key_filename=key_path)
            self.up = True 
            self._mount()
        except Exception as e:
            print(e)
            pass 