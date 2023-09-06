from json import load 
from pathlib import Path 
from pandas import DataFrame

from Setup import check_storage_file, test_cachebench, install_cachebench, setup_cydonia, is_replay_running, is_replay_test_running
from Runner import has_complete_experiment_output
from expK8.remoteFS.Node import Node


class NodeFactory:
    def __init__(
        self, 
        node_config_file_path: str
    ) -> None:
        self.config_file_path = Path(node_config_file_path)
        with self.config_file_path.open("r") as config_file_handle:
            self.config = load(config_file_handle)
        
        self.nodes = []
        for node_name in self.config["nodes"]:
            node_info = self.config["nodes"][node_name]
            node = Node(
                    node_info["host"], 
                    node_info["host"], 
                    self.config["creds"][node_info["cred"]], 
                    self.config["mounts"][node_info["mount"]])
            self.nodes.append(node)
    

    def print_node_status(self) -> None:
        node_status_arr = []
        for node in self.nodes:
            node_status_arr.append({
                "node": node.host.split(".")[0],
                "disk_mb": node.get_file_size("~/disk/disk.file")//(1024*1024),
                "nvm_mb": node.get_file_size("~/nvm/disk.file")//(1024*1024),
                "replay": is_replay_running(node),
                "complete": has_complete_experiment_output(node)
            })
            print(node_status_arr[-1])
        status_df = DataFrame(node_status_arr)
        print(status_df)

    
    def check_disk_file(
        self,
        node: Node
    ) -> None:
        found = False 
        node_type = node.machine_name
        mounts = self.config["mounts"]
        for mount in mounts:
            if "/disk" in mount["mountpoint"]:
                mount_info = mount 
                break 
        
        return found 


    def get_node(self, host_name: str) -> Node:
        for node in self.nodes:
            if node.host == host_name:
                return node 
        raise ValueError("No node found with host name {}".format(host_name))


    def get_free_node(
        self, 
        node_type: str 
    ) -> str:
        """Get a free node of the specified type for block trace replay. 

        Args:
            node_type: str 
                The type of node we want. 
        
        Returns:
            host_name: str 
                The host name of the free node, empty string if no free node of the specified type is found. 
        """
        free_node_host_name = ''
        for node in self.nodes:
            if node.machine_name != node_type:
                continue 

            if is_replay_running(node):
                continue 
            
            if not test_cachebench(node):
                install_cachebench(node)
                if not test_cachebench(node):
                    print("CacheBench not ready!")
                    continue 

            if not setup_cydonia(node):
                print("Cydonia not ready!")
                continue 

            if check_storage_file(node, "~/disk", "disk.file", 950*1000) != 1:
                print("Disk file not ready!")
                continue 

            if check_storage_file(node, "~/nvm", "disk.file", 1) != 1:
                print("NVM file not ready!")
                continue 

            print("{} is ready to run experiments.".format(node.host))
            free_node_host_name = node.host
            break 

        return free_node_host_name


if __name__ == "__main__":
    node_factory = NodeFactory("../fast24/config.json")
    node_factory.print_node_status()