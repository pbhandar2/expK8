from json import load 
from pathlib import Path 

from expK8.remoteFS.Node import Node


class NodeFactory:
    def __init__(
        self, 
        node_config_file_path: str
    ) -> None:
        self._load_config(node_config_file_path)
        self._load_nodes()


    def _load_config(
            self, 
            config_file_path: str
    ) -> None:
        self.config_file_path = Path(config_file_path)
        with self.config_file_path.open("r") as config_file_handle:
            self.config = load(config_file_handle)
        
    
    def _load_nodes(self) -> None: 
        self.nodes = []
        for node_name in self.config["nodes"]:
            node_info = self.config["nodes"][node_name]
            node = Node(
                    node_info["host"], 
                    node_info["host"], 
                    self.config["creds"][node_info["cred"]], 
                    self.config["mounts"][node_info["mount"]])
            self.nodes.append(node)
    

    def get_node_list(self) -> list:
        return self.nodes 