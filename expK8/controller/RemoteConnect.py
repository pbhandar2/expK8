from os import getenv 
from pathlib import Path 
from paramiko import SSHClient, AutoAddPolicy


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
    # transfer data from one node to another 
    remote.move("source.host.in.internet.com", "target.host.in.internet.com", "file_in_source_host", "file_in_target_host")
    # running a command on a node 
    remote.run("compute.node.com", "dir_in_compute_node", "command to run")

    Attributes:
        _config: A dictionary with information such as credential, hostname, port of nodes to connect to. 
        _data: A dictionary using hostnames of data nodes as key and SSHClient() connected to the hostnames as value.  
        _compute: A dictionary using hostnames of compute nodes as key and SSHClient() connected to the hostnames as value.   
        _unresponsive: A dictionary using hostname of unresponsive nodes as key and the exception raised when trying to connect to a node as value. 
        tolerate_unresponsive_compute_node: A boolean indicating if we should avoid raising exception when we find an unresponsive compute node.
        tolerate_unresponsive_data_node: A boolean indicating if we should avoid raising exception when we find an unresponsive data node.
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

        self._data = {}
        self._compute = {}
        self._unresponsive = {}

        self._connect()

    
    def __del__(self) -> None:
        """ Terminate connection with all the nodes. """
        if self._data:
            for hostname in self._data:
                self._data[hostname].close()
        
        if self._compute:
            for hostname in self._compute:
                self._compute[hostname].close()

    
    def get_ssh_client(
            self, 
            node_name: str
    ) -> SSHClient:
        """Return the SSHClient of the connection to a node of the given name.
            Args:
                node_name: the key to dictionary containing the SSHClient. 
            
            Return:
                SSHClient of the connection to the node with the specified name. 
            
            Raises:
                ValueError: No SSHClient to a node with the specified name. 
        """
        if node_name in self._compute:
            ssh_client = self._compute[node_name]
        elif node_name in self._data:
            ssh_client = self._data[node_name]
        else:
            raise ValueError("No node with name {} is connected".format(node_name))
        return ssh_client
    
    
    def _check_if_node_connected(
            self, 
            node_name: str
    ) -> bool:
        """ Check if the node with the given name is connected. 
        
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
    

    def get_node_status(self) -> dict:
        """ Get node status of each node specified in the configuration. 
        
            Returns:
                a dict with the node name as the key and status as value 
        """
        node_status_dict = {}
        for node_name in self._compute:
            node_status_dict[node_name] = self._check_if_node_connected(node_name)
        
        for node_name in self._data:
            node_status_dict[node_name] = self._check_if_node_connected(node_name)
        
        return node_status_dict


    def _connect(self) -> None:
        """ Connect to each node in the configuration if not already connected. 

            Raises:
                ValueError: unrecognized credential type 
                RuntimeError: failed to connect to a compute or data node while set to not tolerate such failures
        """
        for node_name in self._config["nodes"]:
            if not self._check_if_node_connected(node_name):
                hostname = self._config["nodes"][node_name]["host"]
                cred_name = self._config["nodes"][node_name]["cred"]
                cred = self._config["creds"][cred_name]

                try:
                    ssh = SSHClient()
                    ssh.set_missing_host_key_policy(AutoAddPolicy())
                    if cred["type"] == "env":
                        if "port" in cred:
                            ssh.connect(hostname, cred["port"], username=cred["user"], password=getenv(cred["val"])) 
                        else:
                            ssh.connect(hostname, username=cred["user"], password=getenv(cred["val"])) 
                    elif cred["type"] == "file":
                        key_path = Path(cred["val"])
                        ssh.connect(hostname, username=cred["user"], key_filename=str(key_path.absolute()))
                    else:
                        raise ValueError("Unrecognized credential type {}. Can only be 'env' or 'file'".format(cred["type"]))

                    if node_name in self._config["compute"]:
                        self._compute[node_name] = ssh 
                    elif node_name in self._config["data"]:
                        self._data[node_name] = ssh 
                except Exception as e:
                    if (node_name in self._config["compute"] and not self.tolerate_unresponsive_compute_node) or \
                        (node_name in self._config["data"] and not self.tolerate_unresponsive_data_node):
                        raise RuntimeError("Exception: {}, Could not connect to host {}".format(e, hostname))
                    else:
                        self._unresponsive[hostname] = e