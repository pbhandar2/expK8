from json import load, dumps 

from SetupNode import setup_node
from ReplayDB import ReplayDB
from expK8.remoteFS.Node import Node, RemoteRuntimeError


def check_storage_file(
    node: Node,
    mountpoint: str,
    path_relative_to_mountpoint: str,
    min_file_size_mb: int 
) -> int:
    """Check if a required storage file is correctly setup and create one otherwise.

    Args:
        node: Node where file is checked. 
        mountpoint: Mountpoint where file is created. 
        path_relative_to_mountpoint: Path relative to mountpoint of the storage file. 
        min_file_size_mb: Minimum size of file in MB

    Returns:
        status: Current status where -1 means in progress, 0 means error and 1 means done.
    """
    mount_info = node.get_mountpoint_info(mountpoint)
    if not mount_info:
        print(node.get_block_devices())
        print("No valid mount found!")
        return 0 
    
    create_file_ps_row = None
    ps_output = node.ps()
    for ps_row in ps_output.split('\n'):
        if "dd" not in ps_row:
            continue 
        
        if node.format_path(mountpoint) not in ps_row:
            continue 
        
        if path_relative_to_mountpoint not in ps_row:
            continue 
        
        create_file_ps_row = ps_row 
        break 
    
    create_file_live = create_file_ps_row is not None 
    if create_file_live:
        return -1 
    else:
        file_path = "{}/{}".format(mountpoint, path_relative_to_mountpoint)
        current_file_size_byte = node.get_file_size("{}/{}".format(mountpoint, path_relative_to_mountpoint))
        if current_file_size_byte//(1024*1024) < min_file_size_mb:
            print("current file size {} and min file size {}".format(current_file_size_byte//(1024*1024), min_file_size_mb))
            node.create_random_file_nonblock(file_path, min_file_size_mb)
            return -1 
        else:
            return 1
            

def setup_storage_devices(node_config_file_path: str) -> dict:
    """Setup files in storage devices to perform IO.

    Args:
        node_config_file_path: Path to file with configuration of remote nodes. 
    
    Returns:
        setup_status: Dictionary of setup status for each node. 
    """
    with open(node_config_file_path, "r") as config_handle:
        config_dict = load(config_handle)
        creds = config_dict["creds"]
        mounts = config_dict["mounts"]
        nodes = config_dict["nodes"]
    
    setup_status = {}
    for node_name in nodes:
        node_info = nodes[node_name]
        node = Node(node_info["host"], 
                        node_info["host"], 
                        creds[node_info["cred"]], 
                        mounts[node_info["mount"]])
        setup_status[node.host] = {
            "backing": check_storage_file(node, "~/disk", "disk.file", 980*1000),
            "nvm": check_storage_file(node, "~/nvm", "disk.file", 1000)
        }
    
    print(dumps(setup_status, indent=4))
    return setup_status


if __name__ == "__main__":
    setup_storage_devices("config.json")