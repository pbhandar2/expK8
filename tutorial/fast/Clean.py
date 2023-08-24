from pathlib import Path 
from json import loads
from collections import defaultdict

from expK8.remoteFS.Node import Node 

from Config import Config 
from NodeFactory import NodeFactory
from ReplayOutputDB import ReplayOutputDB


CONFIG = Config()
NODE_FACTORY = NodeFactory("../fast24/config.json")


def has_complete_experiment(node: Node) -> bool:
    file_exist_arr = [node.file_exists(replay_output_file) for replay_output_file in CONFIG.replay_output_file_list]
    return all(file_exist_arr)


def rm_tree(path):
    """Pathlib implementation of recursively removing all contents and the 
    directory. 
    
    Args:
    path: The path of directory to remove.
    """
    pth = Path(path)
    for child in pth.glob('*'):
        if child.is_file():
            child.unlink()
        else:
            rm_tree(child)
    pth.rmdir()


def get_local_block_trace_path(remote_block_trace_path: str) -> str:
    """Get local block trace path given the remote block trace path.
    
    Args:
        remote_block_trace_path: Path of trace in remote node. 
    
    Returns:
        local_block_trace_path: Local path of trace. 
    """
    remote_block_trace_file_name = Path(remote_block_trace_path).stem
    split_file_name = remote_block_trace_file_name.split("-")
    if len(split_file_name) > 2:
        workload_type, workload_name, sample_type, sample_params = split_file_name
        local_block_trace_path = CONFIG.sample_trace_path.joinpath(sample_type, workload_type, workload_name, "{}.csv".format(sample_params))
    else:
        workload_type, workload_name = split_file_name
        local_block_trace_path = CONFIG.trace_path.joinpath(workload_type, "{}.csv".format(workload_name))
    return local_block_trace_path


def get_replay_params(node: Node) -> dict:
    """Get the replay parameters from the configuration file in the node.
    
    Args:
        node: Node from which replay parameters are extracted.
    
    Returns:
        params: Dictionary of replay parameters.
    """
    config_json_str = node.cat("/run/replay/config.json")
    config_json = loads(config_json_str)
    cache_config = config_json["cache_config"]
    replay_config = config_json["test_config"]["blockReplayConfig"]
    
    replay_params = {}
    replay_params["num_async_threads"] = replay_config["asyncIOReturnTrackerThreads"]
    replay_params["num_block_threads"] = replay_config["blockRequestProcesserThreads"]
    replay_params["max_pending_block_requests"] = replay_config["maxPendingBlockRequestCount"]
    replay_params["t1_size_mb"] = cache_config["cacheSizeMB"]

    replay_params["t2_size_mb"] = 0 
    if "nvmCacheSizeMB" in cache_config:
        replay_params["t2_size_mb"] = cache_config["nvmCacheSizeMB"]
    replay_params["replay_rate"] = 1
    if "replayRate" in replay_config:
        replay_params["replay_rate"] = replay_config["replayRate"]
    replay_params["iteration"] = 0 
    if "iteration" in replay_config:
        replay_params["iteration"] = replay_config["iteration"]
    replay_params["block_trace_path"] = get_local_block_trace_path(replay_config["traces"][0])

    block_trace_file_name = Path(replay_config["traces"][0]).stem
    if len(block_trace_file_name.split("-")) > 2:
        replay_params["sample"] = 1 
    else:
        replay_params["sample"] = 0 
    
    return replay_params


def is_replay_running(node: Node) -> bool:
        """Check if a node has a replay process running. 

        Args:
            node: The node to check if replay is running. 

        Returns:
            running: Boolean indicating if any replay processes is found running. 
        """
        running = False 
        ps_output = node.ps()
        """There are 3 processes running per block trace replay so we need to check and kill them all. 
            1. nohup - The nohup processing running the TraceReplay.py python script. 
            2. python script - The python script that runs trace replay and tracks
                memory, cpu and power usage. 
            3. c++ binary - The CacheBench binary running block trace replay. 
        """
        for ps_row in ps_output.split("\n"):
            if check_if_replay_process(ps_row):
                running = True 
                break
        return running 


def check_if_replay_process(ps_row: str) -> bool:
    """Check if a row from output of "ps" command is related to trace replay.

    Args:
        ps_row: A row from output of 'ps au' command. 
    
    Returns:
        is_replay: Boolean indicating if this process belong to trace replay. 
    """
    is_replay_python_script = CONFIG.replay_python_script_substring in ps_row
    is_replay_cachebench_binary = CONFIG.replay_cachebench_binary_substring in ps_row
    return is_replay_python_script or is_replay_cachebench_binary


def kill_trace_replay(node: Node) -> None:
    """Kill the block trace replay running in remote node.

    Args:
        node: Node where the process is to be killed. 
    """
    ps_output = node.ps()
    """There are 3 processes running per block trace replay so we need to check and kill them all. 
        1. nohup - The nohup processing running the TraceReplay.py python script. 
        2. python script - The python script that runs trace replay and tracks
            memory, cpu and power usage. 
        3. c++ binary - The CacheBench binary running block trace replay. 
    """
    for ps_row in ps_output.split("\n"):
        if check_if_replay_process(ps_row):
            pid = int(ps_row.strip().split(' ')[0])
            node.kill(pid)


def kill_ghost_replay() -> None:
    """Kill ghost replays. Ghost replays are trace replay that is running without all the 
    correct files being present."""
    node_list = NODE_FACTORY.get_node_list()

    for node in node_list:
        if not is_replay_running(node):
            print("Replay not running in node {}".format(node.host))
            continue 

        print("Replay running in node {}".format(node.host))
        if not node.file_exists("/run/replay/config.json"):
            kill_trace_replay(node)
            print("Killed ghost replay!")
        
        if not node.file_exists("/run/replay/tsstat_0.out"):
            kill_trace_replay(node)
            print("Killed ghost replay!")


def clean_ghost_replay_output() -> None:
    """Delete ghost replay output directories. A replay output directory that was initialized but the 
    experiment was never completed for some reason."""
    node_list = NODE_FACTORY.get_node_list()
    host_name_list = [node.host for node in node_list]

    output_db = ReplayOutputDB()
    output_file_list = output_db.get_all_output_files()

    live_experiment_host_map = defaultdict(list)
    for output_file_path in output_file_list:
        if output_file_path.name != "host":
            continue 
        
        if len(list(output_file_path.parent.iterdir())) == 1:
            with output_file_path.open("r")  as host_file_handle:
                host_name = host_file_handle.read() 
            print("Live experiment {} in host {}".format(output_file_path.absolute(), host_name))
            live_experiment_host_map[host_name].append(output_file_path.parent)
    
    for host_name in live_experiment_host_map:
        cur_host_replay_list = live_experiment_host_map[host_name]
        if host_name not in host_name_list:
            print("host name not found {}, a ghost experiment".format(host_name))
            continue 
        
        cur_node = None 
        for node in node_list:
            if node.host == host_name:
                cur_node = node 
                break
        
        replay_params = get_replay_params(cur_node)
        replay_output_dir = Path(output_db.get_output_dir_path(node.machine_name, replay_params))

        for cur_replay_output_dir in cur_host_replay_list:
            if cur_replay_output_dir == replay_output_dir:
                print("Found {}".format(replay_output_dir))
            else:
                if replay_output_dir.exists():
                    rm_tree(replay_output_dir)
                    print("Deleted {}".format(replay_output_dir))
                
  
def transfer_completed_experiments():
    node_list = NODE_FACTORY.get_node_list()
    for node in node_list:
        if has_complete_experiment(node):
            print("Node {} has a complete experiment.".format(node))


if __name__ == "__main__":
    kill_ghost_replay()
    clean_ghost_replay_output()
    transfer_completed_experiments()
