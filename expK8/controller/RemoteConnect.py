from os import getenv 
from pathlib import Path 
from threading import Thread
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


class RemoteConnect:
    """RemoteConnect runs commands in compute nodes, and transfers data to/from data nodes and compute nodes.

    RemoteConnect takes a JSON configuration as an argument that includes details required to communicate with remote 
    resources. The example configuration below contains a data node we connect to using a password
    and a compute node we connect to using a public key file. Note that RemoteConnect does not take password as a text 
    input but accesses it through an environment variable. 

    {
        "creds": {
            "data": {
                "user": "pranav",
                "type": "env",
                "val": "DATA_PWD"
            },
            "compute": {
                "user": "pbhandar",
                "type": "file",
                "val": "/myhome/.ssh/mypubkey"
            }
        },
        "nodes": {
            "compute0": {
                "host": "compute0.somewhere.in.cloud.com",
                "cred": "compute"
            },
            "data0": {
                "host": "data0.somewhere.in.cloud.com",
                "cred": "data"
            }
        },
        "compute": ["compute0"],
        "data": ["data0"]
    }

    Typical usage example:
    remote = RemoteConnect(config)
    # read the data file in a remote node 
    data_from_host_name = remote_connect.cat(host_name, path_to_file_in_host_name)
    # transfer file from one remote node to another 
    remote_connect.scp(source_host_name, source_data_file_path, target_host_name, source_data_file_path)

    Attributes:
        _config: A dictionary with information such as credential, host_name, port of nodes to connect to. 
        tolerate_unresponsive_compute_node: A boolean indicating if we should avoid raising exception when we find an unresponsive compute node.
        tolerate_unresponsive_data_node: A boolean indicating if we should avoid raising exception when we find an unresponsive data node.
        _ssh: A dictionary using host names as key and SSHClient() as value.   
        _home_dir: A dictionary using host names as key and their corresponding path to home directory as value. 
        _unresponsive: A dictionary using host names as key and the exception raised when trying to connect to them as value. 
        _public_key: A dictionary using host names as key and the public key as value. 
        _ssh_key_filename: The name of ssh key file generated by expK8. ("expK8_id_rsa")
        _default_port: Default port number to use for SSH connection. (22)
    """
    def __init__(
        self, 
        config: dict,
        tolerate_unresponsive_compute_node: bool = True,
        tolerate_unresponsive_data_node: bool = True,
    ) -> None:
        """Initializes the instance based on a configuration dictionary.

        Args:
          config: Information needed to connect to remote resources. 
          tolerate_unresponsive_compute_node: Defines if we raise an exception if we find an unresponsive compute node. 
          tolerate_unresponsive_data_node: Defines if we raise an exception if we find an unresponsive data node. 
        """
        self._config = config
        self.tolerate_unresponsive_compute_node = tolerate_unresponsive_compute_node 
        self.tolerate_unresponsive_data_node = tolerate_unresponsive_data_node 

        self._ssh = {}
        self._home_dir = {}
        self._unresponsive = {}
        self._public_key = {}
        self._ssh_key_filename = "expK8_id_rsa"
        self._default_port = 22

        self._connect()

    
    def __del__(self) -> None:
        """Terminate connection with all the nodes. 
        """
        for node_name in self._ssh:
            self._ssh[node_name].close()


    def get_ssh_client(
            self, 
            host_name: str 
    ) -> SSHClient:
        """Return the SSHClient of the connection to a node of the given name.
        Args:
            host_name: Key to dictionary containing SSHClients. 
        
        Return:
            SSHClient of the connection to the node with the specified name. 
        
        Raises:
            ValueError: No SSHClient found for a node with the specified name. 
        """
        if host_name in self._ssh:
            ssh_client = self._ssh[host_name]
        else:
            raise ValueError("No node with name {} is connected {}".format(host_name, self._ssh))
        return ssh_client
    

    def exec_command(
            self, 
            host_name: str,  
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
        ssh_client = self.get_ssh_client(host_name)
        exit_code, stdout, stderr = None, "", ""
        for cur_num_retry in range(num_retry):
            thread = ExecCommandThread(ssh_client, command_str_arr)
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
            raise RemoteRuntimeError(command_str_arr, host_name, exit_code, stdout, stderr)
        return stdout, stderr, exit_code


    def rm(
            self,
            node_name: str,
            path: str
    ) -> None:
        """Remove a file in a remote node. 

        Args:
            node_name: the remote node where a file will be removed
            path: path of file to be removed 
        
        Raises:
            RemoteRunetimeError: if the 'cat' command fails in remote node
        """
        ssh_client = self.get_ssh_client(node_name)
        sftp_client = ssh_client.open_sftp()
        sftp_client.remove(path)


    def cat(
            self,
            host_name: str,
            path: str
    ) -> str:
        """This function executes 'cat' on a remote file and returns the content.

        Args:
            host_name: host_name of the node where 'cat' will be executed. 
            path: path of file in remote node to 'cat' 
        
        Return:
            content: the string content of the file in remote node 
        
        Raises:
            RemoteRunetimeError: if the 'cat' command fails in remote node
        """
        cat_cmd = ["cat", path]
        stdout, stderr, exit_code = self.exec_command(host_name, cat_cmd)
        if exit_code:
            raise RemoteRuntimeError(cat_cmd, host_name, exit_code, stdout, stderr)
        return stdout.rstrip()
    

    def _add_authorized_public_key(
            self,
            host_name: str,
            public_key_str: str
    ) -> None:
        """Add a public key to a list of authorized keys in a node. 

        Args:
            node_name: the node whose authorized keys file is being updated 
            public_key_str: the public key string being added to the list of authorized keys 
        """
        authorized_key_file_path =  self.get_authorized_key_file_path(host_name)
        add_to_unauthorized_key_cmd = ["echo", public_key_str, ">>", authorized_key_file_path]
        stdout, stderr, exit_code = self.exec_command(host_name, add_to_unauthorized_key_cmd)
        if exit_code:
            raise RemoteRuntimeError(add_to_unauthorized_key_cmd, host_name, exit_code, stdout, stderr)


    def _get_authorized_keys(
            self,
            host_name: str
    ) -> list[str]:
        """Get the string of the authorized keys of a remote node. 
        
        Args:
            host_name: The host_name of node whose authorized key we are reading. 
        
        Return:
            List of authorized keys in the node. 
        """
        authorized_key_file_path = str(Path(self._home_dir[host_name]).joinpath(".ssh", "authorized_keys").absolute())
        if not self.remote_path_exists(host_name, authorized_key_file_path):
            return []
        authorized_key_str = self.cat(host_name, authorized_key_file_path)
        authorized_key_list = []
        for authorized_key_line in authorized_key_str.split("\n"):
            if "#" not in authorized_key_line:
                authorized_key_list.append(authorized_key_line)
        return authorized_key_list
    

    def _check_authorized_key(
            self, 
            host_name: str, 
            public_key_str: str 
    ) -> bool:
        """Checks if a public key is in the list of authorized key of a node. 

        Args:
            node_name: the node whose authorized keys we are checking 
            public_key_str: the public key we want to know is authorized 
        
        Return:
            A boolean indicating if the public key is authorized in the node. 
        """
        authorized_key_list = self._get_authorized_keys(host_name)
        return any([public_key_str in authorized_key for authorized_key in authorized_key_list])
    

    def lsblk(
            self,
            hostname: str
    ) -> None:
        """Return a JSON output of command 'lsblk' in remote node. 

        Args:
            hostname: The hostname of the node for which we get list of block devices. 

        Return:
            A JSON output of command 'lsblk'. 
        """
        lsblk_cmd = ["lsblk", "--json"]
        stdout, stderr, exit_code = self.exec_command(hostname, lsblk_cmd)
        if exit_code:
            raise RemoteRuntimeError(lsblk_cmd, hostname, exit_code, stdout, stderr)

        return json.loads(stdout)
    

    def get_node_status(self) -> dict:
        """ Get node status of each node specified in the configuration. 
        
            Returns:
                a dictionary with the node name as the key and status as value 
        """
        node_status_dict = {}
        for node_name in self._ssh:
            node_status_dict[node_name] = self._check_if_node_connected(node_name)
        return node_status_dict


    def get_private_key_path(
            self, 
            host_name: str
    ) -> str:
        """ Get the path of the private key file generated by expK8 to make remote nodes communicate with each other. 
        Args:
            node_name: name of the node for which we are generating path of SSH file  
        
        Return:
            the absolute path of the SSH key file in the remote node 
        """
        full_ssh_key_path = Path(self._home_dir[host_name]).joinpath(".ssh/{}".format(self._ssh_key_filename))
        return str(full_ssh_key_path.absolute())
    

    def get_public_key_path(
            self, 
            host_name: str
    ) -> str:
        """Get the path of the private key file generated by expK8 to make remote nodes communicate with each other. 
        Args:
            host_name: the host_name of the node for which we need the public key. 
        
        Return:
            the absolute path of the SSH key file in the remote node 
        """
        full_ssh_key_path = Path(self._home_dir[host_name]).joinpath(".ssh/{}.pub".format(self._ssh_key_filename))
        return str(full_ssh_key_path.absolute())
    

    def get_authorized_key_file_path(
            self, 
            host_name: str
    ) -> str:
        """ Get the path of file containing authorized keys. 
        Args:
            host_name: The hostname of the node whose authorized key file path we want. 
        
        Return:
            the absolute path of the file containing authorized keys. 
        """
        full_authorized_key_path = Path(self._home_dir[host_name]).joinpath(".ssh/authorized_keys")
        return str(full_authorized_key_path.absolute())


    def remote_path_exists(
            self, 
            host_name: str, 
            path: str 
    ) -> bool:
        """Check if a path exists in a remote node. 

        Args:
            host_name: the host_name of the remote node to connect to. 
            path: path in remote node to be checked 
        
        Returns:
            a boolean indicating whether the path exists in the remote node 
        """
        sftp_client = self._ssh[host_name].open_sftp()
        try:
            sftp_client.stat(path)
            return True 
        except IOError:
            return False 


    def get_node_name_from_host_name(
            self, 
            host_name: str
    ) -> str:
        """Get the name of the node to which the host name belongs. 

        Args:
            host_name: The hostname for which we are looking for the node name. 
        
        Return:
            The name of the node corresponding to the host name. 
        """
        for node_name in self._config["nodes"]:
            if self._config["nodes"][node_name]["host"] == host_name:
                return node_name 
        return ""


    def scp(
            self,
            source_host_name: str, 
            source_path: str, 
            target_host_name: str,
            target_path: str,
            timeout: float = None,
            num_retry: int = 2
    ) -> None:
        """Move file from a path in source node to a path in target node. 

            Args:
                source_host_name: The hostname of the source node. 
                source_path: Path of file in source node. 
                target_host_name: The hostname of target node. 
                target_path: Path of file in target node. 
                timeout: Seconds to wait before before a remote command times out. 
            
            Raises:
                ValueError: Raised if either public key or source path do not exist in source node 
        """
        if not self.remote_path_exists(source_host_name, self.get_public_key_path(source_host_name)):
            raise ValueError("No public key file {} in node {}".format(self.get_public_key_path(source_host_name), source_host_name))
        if not self.remote_path_exists(source_host_name, source_path):
            raise ValueError("No source file {} in node {}".format(source_path, source_host_name))
        
        source_public_key_str = self._public_key[source_host_name]
        if not self._check_authorized_key(target_host_name, source_public_key_str):
            self._add_authorized_public_key(target_host_name, source_public_key_str)

        scp_cmd = ["scp", "-i", self.get_private_key_path(source_host_name)]
        target_node_name = self.get_node_name_from_host_name(target_host_name)
        cred_name = self._config["nodes"][target_node_name]["cred"]
        if "port" in self._config["creds"][cred_name]:
            scp_cmd += ["-P", str(self._config["creds"][cred_name]["port"])]
        scp_cmd.append(source_path)

        target_data_str = "{}@{}:{}".format(self._config["creds"][cred_name]["user"], target_host_name, target_path)
        scp_cmd.append(target_data_str)

        stdout, stderr, exit_code = self.exec_command(source_host_name, scp_cmd, timeout=timeout, num_retry=num_retry)        
        if not self.remote_path_exists(target_host_name, target_path) or exit_code:
            raise RemoteRuntimeError(scp_cmd, source_host_name, exit_code, stdout, stderr)


    def _generate_ssh_key(
            self, 
            host_name: str
    ) -> None:
        """Generate a SSH key on the node.
        
        Args:
            host_name: The hostname of the node where SSH key is to be generated. 
        
        Raises:
            RemoteRunetimeError: if the 'ssh-keygen' command fails on the remote node 
        """
        ssh_keygen_create_cmd = ["ssh-keygen", "-t", "rsa", "-C", host_name, "-f", self.get_private_key_path(host_name), "-P", "", "-q"]
        stdout, stderr, exit_code = self.exec_command(host_name, ssh_keygen_create_cmd)
        if exit_code:
            raise RemoteRuntimeError(ssh_keygen_create_cmd, host_name, exit_code, stdout, stderr)

    
    def _check_if_node_connected(
            self, 
            node_name: str
    ) -> bool:
        """Check if the node with the given name is connected. 
        
        Args:
            node_name: the name of node to check for connection.
        
        Return:
            A boolean indicated if the node with the specified name is connected. 
        """
        try:
            ssh_client = self.get_ssh_client(node_name)
            transport = ssh_client.get_transport() if ssh_client else None
            return transport and transport.is_active()
        except ValueError:
            return False 
        
    
    def _init_mount(
            self,
            host_name: str,
            mount_config_list: list[dict]
    ) -> None:
        """Mount all the devices listed in the configuration to the correct mount point in remote host. 

        Args:
            host_name: name of the host where mounts are to be setup 
            mount_config_list: list of dictionaries containing details of each mountpoint to setup 
        """
        block_device_list = self.lsblk(host_name)
        print(block_device_list)
        for mount_config in mount_config_list:
            # check if device exists 

            # check if there a partition of the required size that is not root 
            pass 
            

        
    
    def _init_ssh(
            self, 
            host_name: str,
            user_name: str, 
            cred_type: str, 
            cred_value: str, 
            port: int = -1
    ) -> None:
        """Initiate an SSH connection with a remote node. 

        Args:
            host_name: host_name of the node to connect to. 
            user_name: user_name to use when establishing SSH connection.
            cred_type: Type of credential ("env" for environment variable and "file" for public key file).
            cred_value: Environment variable name if type is "env" and path to public key file if type is "file".
            port: Optional to specify the port to connect to.

        Raises:
            ValueError: Raised if we find an unrecognized credential type. 
            RuntimeError: Raised if we failed to connect to a node. 
        """
        try:
            ssh = SSHClient()
            ssh.set_missing_host_key_policy(AutoAddPolicy())
            if cred_type == "env":
                if port >= 0:
                    ssh.connect(host_name, port, username=user_name, password=getenv(cred_value))
                else:
                    ssh.connect(host_name, username=user_name, password=getenv(cred_value))
            elif cred_type == "file":
                key_path = str(Path(cred_value).absolute())
                ssh.connect(host_name, username=user_name, key_filename=key_path)
            else:
                raise ValueError("Unrecognized credential type {}. Can only be 'env' or 'file'".format(cred_type))
            
            if host_name in self._ssh:
                raise ValueError("Multiple SSH connection to node with hostname {}".format(host_name))
            
            sftp_client = ssh.open_sftp()
            sftp_client.chdir(".")
            home_dir = sftp_client.getcwd()
            self._ssh[host_name] = ssh 
            self._home_dir[host_name] = home_dir

            if not self.remote_path_exists(host_name, self.get_public_key_path(host_name)):
                self._generate_ssh_key(host_name)
            self._public_key[host_name] = self.cat(host_name, self.get_public_key_path(host_name))
        except Exception as e:
            if (host_name in self._config["compute"] and not self.tolerate_unresponsive_compute_node) or \
                (host_name in self._config["data"] and not self.tolerate_unresponsive_data_node):
                raise RuntimeError("Exception: {}, Could not connect to host {}".format(e, host_name))
            else:
                self._unresponsive[host_name] = e


    def _connect(self) -> None:
        """Connect to each node in the configuration if not already connected. 
        """
        for node_name in self._config["nodes"]:
            if self._check_if_node_connected(node_name):
                continue 

            host_name = self._config["nodes"][node_name]["host"]
            cred_name = self._config["nodes"][node_name]["cred"]
            cred_dict = self._config["creds"][cred_name]

            port = 22 
            if "port" in cred_dict:
                port = cred_dict["port"]

            self._init_ssh(host_name, 
                            cred_dict["user"], 
                            cred_dict["type"], 
                            cred_dict["val"], 
                            port=port)
        
            if "mount" in self._config["nodes"][node_name]:
                mount_config = self._config["mount"][self._config["nodes"][node_name]["mount"]]
                self._init_mount(host_name, mount_config)