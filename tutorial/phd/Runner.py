from pathlib import Path 
from json import load, loads 
from argparse import ArgumentParser 

from Setup import test_cachebench, install_cachebench, setup_cydonia
from expK8.remoteFS.Node import Node 


def has_complete_experiment_output(
        node: Node 
) -> bool:
    remote_output_file_path = "/run/replay/stat_0.out"
    return node.file_exists(remote_output_file_path)


def transfer_block_trace(
        node: Node,
        replay_params: dict 
) -> str:
    """Transfer block trace to remote node for replay. 
    
    Args:
        node: Remote node to transfer block trace to. 
        replay_params: Dictionary of replay parameters. 
    
    Returns:
        remote_block_trace_path: Path in remote node where block trace was transfered.
    """

    local_block_trace_size_byte = Path(replay_params["block_trace_path"]).expanduser().stat().st_size
    remote_block_trace_path = get_remote_block_trace_path(replay_params)
    remote_block_trace_size_byte = node.get_file_size(remote_block_trace_path)

    if local_block_trace_size_byte != remote_block_trace_size_byte:
        node.scp(replay_params["block_trace_path"], remote_block_trace_path)
    
    remote_block_trace_size_byte = node.get_file_size(remote_block_trace_path)
    assert local_block_trace_size_byte == remote_block_trace_size_byte, \
        "Remote {} and local {} block trace have differnet sizes.".format(remote_block_trace_size_byte, local_block_trace_size_byte)

    return remote_block_trace_path


def transfer_complete_experiment(
        node: Node 
) -> bool:
    """Transfer output from trace replay in remote node.
    
    Args:
        node: Node where trace replay was run. 
    
    Returns:
        complete: Boolean indicating if the output was sucessfully transfered. 
    """
    stat_file_path = "/run/replay/stat_0.out"
    ts_stat_file_path = "/run/replay/tsstat_0.out"
    config_file_path = "/run/replay/config.json"
    stdout_file_path = "/run/replay/stdout.dump"
    stderr_file_path = "/run/replay/stderr.dump"
    power_file_path = "/run/replay/power.csv"
    usage_file_path = "/run/replay/usage.csv"

    config_json_str = node.cat(config_file_path)
    config_dict = loads(config_json_str)

    remote_block_trace_file_name = Path(config_dict["test_config"]['blockReplayConfig']['traces']).stem 
    split_file_name = remote_block_trace_file_name.split("-")
    if len(split_file_name) > 2:
        workload_name, workload_type, sample_type, sample_params_str = split_file_name
    else:
        workload_type, workload_name = split_file_name


def run_block_trace_replay(
        node: Node,
        replay_params: dict
) -> None:
    print("Running experiment {} in node {}".format(replay_params, node.host))
    if has_complete_experiment_output(node):
        print("Node {} has completed experiment output.".format(node.host))

        return 

    remote_block_trace_path = transfer_block_trace(node, replay_params)
    replay_cmd_str = get_replay_cmd(
                        remote_block_trace_path, 
                        replay_params["t1_size_mb"], 
                        replay_params["t2_size_mb"],
                        replay_params["replay_rate"])

    print(replay_cmd_str)
    node.nonblock_exec_cmd(replay_cmd_str.split(' '))





def get_replay_cmd(
        remote_block_trace_path: str, 
        t1_size_mb: int, 
        t2_size_mb: int, 
        replay_rate: int 
) -> str:
    replay_cmd = "nohup python3 ~/disk/CacheLib/phdthesis/scripts/fast24/TraceReplay.py "
    replay_cmd += "{} ".format(remote_block_trace_path)
    replay_cmd += "{} ".format(t1_size_mb)
    if t2_size_mb > 0:
        replay_cmd += " --t2_size_mb {}".format(t2_size_mb)
    
    if replay_rate > 1:
        replay_cmd += " --replay_rate {}".format(replay_rate)
    
    replay_cmd += " >> /run/replay/replay.log 2>&1"
    return replay_cmd


def get_experiment_name_str(params: dict) -> str:
    return "q={}_bt={}_at={}_t1={}_t2={}_rr={}_it={}".format(
        params["max_pending_block_requests"],
        params["num_block_threads"],
        params["num_async_threads"],
        params["t1_size_mb"],
        params["t2_size_mb"],
        params["replay_rate"],
        params["iteration"])


def get_remote_block_trace_path(replay_params: dict) -> str:
    block_trace_path = Path(replay_params["block_trace_path"])
    if replay_params['sample']:
        sample_params_str = block_trace_path.stem 
        workload_name = block_trace_path.parent.name 
        workload_type = block_trace_path.parent.parent.name 
        sample_type = block_trace_path.parent.parent.parent.name
        remote_file_name = "{}-{}-{}-{}.csv".format(workload_type, workload_name, sample_type, sample_params_str)
    else:
        workload_name = block_trace_path.stem 
        workload_type = block_trace_path.parent.name 
        remote_file_name = "{}-{}.csv".format(workload_type, workload_name)
    
    return "/run/replay/{}".format(remote_file_name)



    
    



















NODE_LIST_FILE = "../fast24/config.json"
REPLAY_DATA_DIR = "/research2/mtc/cp_traces/pranav/replay"
REPLAY_META_DIR = "/research2/mtc/cp_traces/pranav/meta/replay"


def get_config_dict(node: Node) -> dict:
    config_file_path = "/dev/shm/config.json"
    if not node.file_exists(config_file_path):
        return None 

    stdout, stderr, exit_code = node.exec_command("cat {}".format(config_file_path).split(' '))
    if exit_code:
        return None
    else:
        return loads(stdout.rstrip())


def cleanup_trace_replay(node: Node) -> bool:
    config_dict = get_config_dict(node)
    if config_dict:
        pass 


def get_experiment_name_str(
    t1_size_mb: int,
    t2_size_mb: int,
    replay_rate: int, 
    max_pending_block_requests: int,
    num_block_threads: int,
    num_async_threads: int,
    iteration: int 
) -> str:
    return "q={}_bt={}_at={}_t1={}_t2={}_rr={}_it={}".format(
        max_pending_block_requests,
        num_block_threads,
        num_async_threads,
        t1_size_mb,
        t2_size_mb,
        replay_rate,
        iteration)


def get_workload_name(local_block_trace_path: str) -> str:
    local_path = Path(local_block_trace_path)
    trace_file_name = local_path.stem 

    if '_' in trace_file_name:
        workload_name = local_path.parent.name 
        sample_type = local_path.parent.parent.name 
        workload_set_name = local_path.parent.parent.parent.name
        workload_name = "{}-{}-{}-{}".format(workload_set_name, sample_type, workload_name, trace_file_name)
    else:
        workload_name = "{}-{}".format(local_path.parent.name, trace_file_name)
    return workload_name
    

def run_experiment(
    node: Node,
    local_block_trace_path: str,
    remote_block_trace_path: str,
    t1_size_mb: int, 
    t2_size_mb: int, 
    replay_rate: int, 
    max_pending_block_requests: int,
    num_block_threads: int,
    num_async_threads: int,
    iteration: int 
) -> bool:
    """Run block trace replay on a remote node.
    
    Args:
        node: The node where block trace replay is to run. 
        local_block_trace_path: The path of trace in local node. 
        remote_block_trace_path: The path of trace in remote node. 
        t1_size_mb: Size of tier-1 cache in MB. 
        t2_size_mb: Size of tier-2 cache in MB. 
        replay_rate: Value to divide inter-arrival times by to accelerate trace replay. 
        max_pending_block_request: Maximum number of pending block requests. 
        num_block_threads: Number of threads for block request processing. 
        num_async_threads: Number of threads for async backing IO processing. 
        iteration: This is the nth iteration of the experiment, starts from 0.
    
    Returns:
        running: Boolean indicating if new block trace replay was started. 
    """
    local_block_trace_size_bytes = Path(local_block_trace_path).expanduser().stat().st_size
    remote_block_trace_size_bytes = node.get_file_size(remote_block_trace_path)

    if local_block_trace_size_bytes != remote_block_trace_size_bytes:
        node.scp(local_block_trace_path, remote_block_trace_path)
    
    remote_block_trace_size_bytes = node.get_file_size(remote_block_trace_path)
    assert local_block_trace_size_bytes == remote_block_trace_size_bytes, \
        "Local {} and remote trace {} not the same size.".format(local_block_trace_size_bytes, remote_block_trace_size_bytes)
    
    experiment_name_str = get_experiment_name_str(t1_size_mb, t2_size_mb, replay_rate, max_pending_block_requests, 
                                                    num_block_threads, num_async_threads, iteration)
    workload_name_str = get_workload_name(local_block_trace_path)
    replay_output_dir = Path(REPLAY_DATA_DIR).joinpath(node.machine_name, workload_name_str, experiment_name_str)

    if replay_output_dir.exists():
        print("Already started!")
        return 0 
    else:
        print("Now running experiment")
        replay_cmd = "nohup python3 ~/disk/CacheLib/phdthesis/scripts/fast24/TraceReplay.py "
        replay_cmd += "{} ".format(remote_block_trace_path)
        replay_cmd += "{} ".format(t1_size_mb)
        if t2_size_mb > 0:
            replay_cmd += " --t2_size_mb {}".format(t2_size_mb)
        
        if replay_rate > 1:
            replay_cmd += " --replay_rate {}".format(replay_rate)
        
        replay_cmd += " >> /dev/shm/replay.log 2>&1"
        print("Replaying: {}".format(replay_cmd))

        return 0 
    
        node.nonblock_exec_cmd(replay_cmd.split(' '))
        replay_output_dir.mkdir(exist_ok=True, parents=True)
        with replay_output_dir.joinpath("host").open("w+") as write_handle:
            write_handle.write("{}\n".format(node.host))
            write_handle.write(replay_cmd)
        print("Replay started with cmd: {}".format(replay_cmd))


def is_node_ready_for_experiment(node: Node) -> bool:
    """Check if node is ready for experiment. 
    
    Args:
        node: Node where experiment is to be run. 
    
    Returns:
        ready: Boolean indicating whether node is ready to run experiment. 
    """
    disk_file_path = "~/disk/disk.file"
    disk_file_size_byte = node.get_file_size(disk_file_path)
    if disk_file_size_byte < 950 * 1e9:
        return False, "Disk file size ({}) error.".format(disk_file_size_byte)
    
    nvm_file_path = "~/nvm/disk.file"
    nvm_file_size_byte = node.get_file_size(nvm_file_path)
    if nvm_file_size_byte == 0:
        return False, "NVM file size ({}/{}) error.".format(nvm_file_size_byte)
    
    if not test_cachebench(node):
        install_cachebench(node)
        if not test_cachebench(node):
            return False, "CacheBench setup failed."
    
    if not setup_cydonia(node):
        return False, "Cydonia setup failed."

    return True, "Running."


def is_replay_running(node: Node) -> bool:
    """Check if replay is running in a node.
    
    Args:
        node: Node where experiment might be running. 
    
    Returns:
        running: Boolean indicating if trace replay is already running in the node. 
    """
    running = False 
    ps_output = node.ps()
    for ps_row in ps_output.split('\n'):
        if "bin/cachebench" in ps_row:
            running = True 
            break 
    return running


def runner(args):
    with open(NODE_LIST_FILE, "r") as config_handle:
        config_dict = load(config_handle)
        creds = config_dict["creds"]
        mounts = config_dict["mounts"]
        nodes = config_dict["nodes"]
    
    node_info = nodes[args.node_name]
    node = Node(
            node_info["host"], 
            node_info["host"], 
            creds[node_info["cred"]], 
            mounts[node_info["mount"]])

    if is_replay_running(node):
        print("An experiment already running.")
        return 0 
    
    replay_output_file = "/dev/shm/stat_0.out"
    if node.get_file_size(replay_output_file):
        print("Completed experiment exists. Cleanup first.")
        return 0

    is_node_ready, read_msg = is_node_ready_for_experiment(node)
    if is_node_ready:
        print("Ready for experiment!")
        run_experiment(
            node,
            args.local_block_trace_path,
            args.remote_trace_path,
            args.t1_size_mb, 
            args.t2_size_mb, 
            args.replay_rate, 
            args.max_pending_block_requests,
            args.num_block_threads,
            args.num_async_threads,
            args.iteration)
    else:
        print("Not ready for experiment. {}".format(read_msg))


if __name__ == "__main__":
    parser = ArgumentParser(description="Run block trace replay in a remote node.")

    parser.add_argument("node_name", 
                        type=str, 
                        help="The name of the node to run replay experiment.")

    parser.add_argument("local_block_trace_path",
                        type=str,
                        help="Local path of block trace to replay.")

    parser.add_argument("remote_trace_path",
                        type=str,
                        help="The path of block trace in remote node.")

    parser.add_argument("t1_size_mb",
                        type=int,
                        help="Size of tier-1 cache in MB.")

    parser.add_argument("t2_size_mb",
                        type=int,
                        help="Size of tier-2 cache in MB.")

    parser.add_argument("--replay_rate",
                        default=1,
                        type=int,
                        help="The replay rate used for trace acceleration.")

    parser.add_argument("--max_pending_block_requests",
                        default=128,
                        type=int,
                        help="Maximum pending block requests in the system.")

    parser.add_argument("--num_block_threads",
                        default=16,
                        type=int,
                        help="The number of threads used for processing block requests.")

    parser.add_argument("--num_async_threads",
                        default=16,
                        type=int,
                        help="Number of threads used for async request processing.")

    parser.add_argumnet("--iteration",
                        default=0,
                        type=int,
                        help="The itearation of the experiment.")

    args = parser.parse_args()
    runner(args)