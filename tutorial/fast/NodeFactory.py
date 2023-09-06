"""NodeFactory manages a set of remote nodes. 
"""

from json import load, dumps 
from pathlib import Path 
from pandas import DataFrame
from Setup import is_replay_running, test_cachebench, check_file

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
    

    def print_node_status(self) -> None:
        node_status_arr = []
        for node in self.nodes:
            node_status = {
                'host': node.host,
                'disk_file': 0,
                'nvm_file': 0,
                'replay': 0, 
                'cachelib_test': 0,
                'cydonia_test': 0
            }
            node_status["replay"] = is_replay_running(node)

            if not node_status["replay"]:
                node_status["disk_file"] = check_file(node, "~/disk/disk.file", 950000)
                node_status["disk_file"] = check_file(node, "~/nvm/disk.file", 1)
                node_status["cachelib_test"] = test_cachebench(node)
                node_status["cydonia_test"] = node.dir_exists("~/disk/CacheLib/phdthesis")

            node_status_arr.append(node_status)
            print(dumps(node_status, indent=2))
        
        status_df = DataFrame(node_status_arr)
        print(status_df)
        
