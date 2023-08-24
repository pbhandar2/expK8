from paramiko.channel import Channel
from paramiko import SSHClient, AutoAddPolicy

from expK8.remoteFS.Node import Node 


class RemoteFS:
    """RemoteFS provides a single FS-like interface to manage data to simulataneously manage multiple nodes. 

    Attributes:
        _config: The dictionary with configuration parameters for RemoteFS.
        _nodes: List of objects of Node class representing a remote node that RemoteFS is connected to.  
    """
    def __init__(
            self,
            config: dict
    ) -> None:
        self._config = config 
        self._nodes = []
        self._init_nodes()


    def __del__(self) -> None:
        """Terminate connection with all the nodes. 
        """
        for node in self._nodes:
            node._ssh.close()
    

    def chown(
        self,
        host_name: str, 
        path: str
    ) -> None:
        self.get_node(host_name).chown(path)
    

    def invoke_shell(
        self, 
        host_name: str
    ) -> Channel:
        """Get a channel to a SSH session in a remote host. 

        Args:
            host_name: Host name to get a shell channel to. 
        
        Return:
            channel: A channel to communicate to a shell in remote host. 
        """
        return self.get_node(host_name)._ssh.invoke_shell()
    

    def get_file_size(
        self, 
        host_name: str, 
        file_path: str 
    ) -> None:
        """Get the size of file in remote node. 

        Args:
            host_name: Host name of remote node. 
            file_path: Path of file whose size we need. 

        Returns:
            size: Size of file in remote node in bytes. 
        """
        return self.get_node(host_name).get_file_size(file_path)


    def create_random_file_nonblock(
        self,
        host_name: str,
        file_path: str,
        file_size_byte: int 
    ) -> None:
        """Create a file with random data in remote node. 

        Args:
            host_name: Host name of remote node. 
            file_path: Path of file in remote node. 
            file_size_byte: Size of file to be created in remote node. 
        """
        return self.get_node(host_name).create_random_file_nonblock(file_path, file_size_byte)
    

    def get_node(
        self, 
        host_name: str 
    ) -> None:
        """Get the node object given the name of the host.
        
        Args:
            host_name: Name of the host to find the Node object with. 
        
        Returns:
            node: The node object with matching host name, None if no matching host name found. 
        """
        node = None 
        for fs_node in self._nodes:
            if fs_node.host == host_name:
                node = fs_node 
                break 
        return node 
    

    def all_up(self) -> bool:
        """ Check if all the nodes are connected. """
        return all([node.check_connection() for node in self._nodes]) if len(self._nodes) else False
    

    def get_all_host_names(self) -> list:
        """Get the host name of all nodes in the FS.
        
        Returns:
            host_name_arr: Array of host names of remote nodes in the configuration of the FS. 
        """
        return [n.host for n in self._nodes]


    def get_all_live_host_names(self) -> list:
        """Get the host name of all nodes in the FS.
        
        Returns:
            host_name_arr: Array of host names of remote nodes in the configuration of the FS. 
        """
        live_host_names = []
        for n in self._nodes:
            if n._ssh_exception is None:
                live_host_names.append(n.host)
        return live_host_names
    

    def file_exists(
        self, 
        host_name: str, 
        file_path: str 
    ) -> None:
        """Check if a file exists in a remote node. 

        Args:
            host_name: Name of the remote host. 
            file_path: Path to check in remote host. 

        Return:
            exists_bool: Boolean indicating if a file exists with the given path in remote node. 
        """
        return self.get_node(host_name).file_exists(file_path)


    def dir_exists(
        self, 
        host_name: str, 
        file_path: str 
    ) -> None:
        """Check if a dir exists in a remote node. 

        Args:
            host_name: Name of the remote host. 
            dir_path: Path to check in remote host. 

        Return:
            exists_bool: Boolean indicating if a file exists with the given path in remote node. 
        """
        return self.get_node(host_name).dir_exists(file_path)


    def _init_nodes(self) -> None:
        """Initiate SSH connection with all nodes in the configuration.
        """
        for node_name in self._config["nodes"]:
            node_dict = self._config["nodes"][node_name]
            host_name = node_dict["host"]

            cred_name = node_dict["cred"]

            mount_list = []
            if "mount" in node_dict:
                mount_name = node_dict["mount"]
                mount_list = self._config["mounts"][mount_name]

            cred_obj = self._config["creds"][cred_name]
            
            self._nodes.append(Node(node_name, host_name, cred_obj, mount_list))