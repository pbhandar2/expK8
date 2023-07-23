from paramiko import SSHClient, AutoAddPolicy

from expK8.remoteFS.Node import Node 


class RemoteFS:
    """RemoteOS provides a single FS-like interface to manage data in multiple remote nodes at once.

    Attributes:
        _config: The dictionary with configuration parameters for RemoteFS. 
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
    

    def all_up(self):
        return all([node.check_connection() for node in self._nodes]) if len(self._nodes) else False


    def _init_nodes(self):
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




