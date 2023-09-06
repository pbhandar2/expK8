from argparse import ArgumentParser 
from copy import deepcopy
from pathlib import Path 
from time import sleep 
from pandas import DataFrame

from expK8.remoteFS.Node import Node 

from Config import Config 
from NodeFactory import NodeFactory
from ReplayOutputDB import ReplayOutputDB
from ExperimentFactory import ExperimentFactory
from Clean import is_replay_running, get_replay_params
from Setup import setup_cydonia, update_cachebench_repo


def transfer_complete_experiment(
        node: Node 
) -> bool:
    """Transfer replay output from a remote node.
    
    Args:
        node: Node where replay was run.
    
    Returns:
        transfer_done: Boolean indicating if data was transfered from remote node to local. 
    """
    transfer_done = False 
    config = Config()
    replay_output_file_status = [node.file_exists(output_file) for output_file in config.replay_output_file_list]
    if all(replay_output_file_status):
        replay_params = get_replay_params(node)

        replay_db = ReplayOutputDB()
        output_path = replay_db.get_output_dir_path(node.machine_name, replay_params)

        for remote_output_file_path in config.replay_output_file_list:
            remote_output_file_name = Path(remote_output_file_path).name
            local_output_file_path = output_path.joinpath(remote_output_file_name)

            remote_file_size_byte = node.get_file_size(remote_output_file_path)
            local_file_size_byte = 0 
            if local_output_file_path.exists():
                local_file_size_byte = local_output_file_path.stat().st_size

            if remote_file_size_byte != local_file_size_byte:
                print("File size {},{} did not match. Downloading {} to {}".format(
                                                                            remote_file_size_byte, 
                                                                            local_file_size_byte, 
                                                                            remote_output_file_path, 
                                                                            local_output_file_path))
                node.download(remote_output_file_path, local_output_file_path)
            
            remote_file_size_byte = node.get_file_size(remote_output_file_path)
            local_file_size_byte = 0 
            if local_output_file_path.exists():
                local_file_size_byte = local_output_file_path.stat().st_size

            assert remote_file_size_byte == local_file_size_byte, \
                "The output file size of remote file {} {} and local file {} {} did not match.".format(
                                                                                                remote_output_file_path,
                                                                                                remote_file_size_byte,
                                                                                                local_output_file_path, 
                                                                                                local_file_size_byte)
            node.rm(remote_output_file_path)
        transfer_done = True 
    
    return transfer_done


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


def get_remote_block_trace_path(replay_params: dict) -> str:
    """Get the path of block trace file during trace replay given its parameters. 

    Args:
        replay_params: Dictionary of replay parameters. 

    Returns:
        remote_block_trace_path: Path of block trace in remote node where replay is run. 
    """
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


def has_complete_experiment_output(
        node: Node 
) -> bool:
    config = Config()
    return all([node.file_exists(output_file) for output_file in config.replay_output_file_list])


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


def run_block_trace_replay(
        node: Node,
        replay_params: dict
) -> None:
    """Run trace replay with the given paramaters and node. 
    
    Args:
        node: Node where trace replay is to be run. 
        replay_params: Dictionary of replay parameters. 
    """
    if has_complete_experiment_output(node):
        print("Node {} has completed experiment output.".format(node.host.split('.')[0]))
        return 

    remote_block_trace_path = transfer_block_trace(node, replay_params)
    replay_cmd_str = get_replay_cmd(
                        remote_block_trace_path, 
                        replay_params["t1_size_mb"], 
                        replay_params["t2_size_mb"],
                        replay_params["replay_rate"])

    print("Node: {}, replay_cmd: {}".format(node.host.split('.')[0], replay_cmd_str))
    node.nonblock_exec_cmd(replay_cmd_str.split(' '))


def get_free_node(
        node_list: list, 
        machine_type: str
) -> Node:
    """Get a node available for trace replay.
    
    Args:
        node_list: List of nodes from which to find an available node. 
        machine_type: Type of machine we want for replay. 

    Returns:
        free_node: Node available for trace replay.
    """
    free_node = None 
    for node in node_list:
        if is_replay_running(node):
            continue 

        if has_complete_experiment_output(node):
            if not transfer_complete_experiment(node):
                continue 
        
        if node.machine_name != machine_type: 
            continue 

        free_node = node 
    return free_node


def pre_replay_sanity_check(node: Node) -> None:
    """Make sure the packages and output directories are updated in the node before starting replay.
    
    Args:
        node: Node where replay will be run.
    """
    node.chown("/run")
    node.mkdir("/run/replay")
    node.chown("/run/replay")
    setup_cydonia(node)
    update_cachebench_repo(node)


def runFAST24(machine_type: str) -> None:
    """Run experiments for FAST 24 for a specific machine type.
    
    Args:
        machine_type: The name of the machine type where to run experiments. 
    """
    exp_status_arr = []
    exp_output_db = ReplayOutputDB()
    node_factory = NodeFactory("../fast24/config.json")

    exp_factory = ExperimentFactory()
    exp_arr = exp_factory.generate_experiments()
    for exp in exp_arr:
        exp_status = deepcopy(exp)

        # Do we have the block trace that we need to replay? 
        block_trace_path = Path(exp['block_trace_path']) 
        if not block_trace_path.exists():
            exp_status["status"] = "Trace not found."
            exp_status["block_trace_path"] = "/".join(exp_status["block_trace_path"].split("/")[-3:])
            exp_status_arr.append(exp_status)
            continue 

        # Is this experiment already running or completed? 
        output_dir = exp_output_db.get_output_dir_path(machine_type, exp)
        if output_dir.exists():
            output_file_list = list(output_dir.iterdir())
            with output_dir.joinpath("host").open("r") as host_file_handle:
                host = host_file_handle.read()
            
            if len(output_file_list) == 1:
                # Live experiment
                exp_status["status"] = "Running in {}".format(host.split(".")[0])
                exp_status["block_trace_path"] = "/".join(exp_status["block_trace_path"].split("/")[-3:])
                exp_status_arr.append(exp_status)
            else:
                # Complete experiment 
                exp_status["status"] = "Completed in {} with {} files.".format(host.split(".")[0], len(output_file_list))
                exp_status["block_trace_path"] = "/".join(exp_status["block_trace_path"].split("/")[-3:])
                exp_status_arr.append(exp_status)
            
            continue 

        free_node = get_free_node(node_factory.get_node_list(), machine_type)
        if free_node is None:
            print("No free node available to run additional experiments.")
            break 

        pre_replay_sanity_check(free_node)
        run_block_trace_replay(free_node, exp)
        sleep(5)

        if is_replay_running(free_node):
            exp_output_db.init_output_dir(machine_type, exp, free_node.host)
            exp_status["status"] = "Started in {}".format(free_node.host)
            exp_status["block_trace_path"] = "/".join(exp_status["block_trace_path"].split("/")[-3:])
            exp_status_arr.append(exp_status)
        else:
            exp_status["status"] = "Failed in {}".format(free_node.host)
            exp_status["block_trace_path"] = "/".join(exp_status["block_trace_path"].split("/")[-3:])
            exp_status_arr.append(exp_status)

        df = DataFrame(exp_status_arr)
        print(df.to_string()) 

    df = DataFrame(exp_status_arr)
    print(df.to_string()) 
    
        
if __name__ == "__main__":
    parser = ArgumentParser("Run block trace replay in remote nodes.")

    parser.add_argument("machine_type", type=str, help="Type of machine to run replays on.")

    args = parser.parse_args()

    runFAST24(args.machine_type)