from json import load 
from pathlib import Path 
from argparse import ArgumentParser

from SetupStorageDevices import check_storage_file
from SetupPackages import test_cachebench, setup_cydonia, install_cachebench

from SetupNode import setup_node
from ReplayDB import ReplayDB
from expK8.remoteFS.Node import Node, RemoteRuntimeError


replay_db = ReplayDB("/research2/mtc/cp_traces/pranav/replay/")

with open("./experiments/sample_cp-test_w66.json", "r") as experiment_file_handle:
    experiment_list = load(experiment_file_handle)



def check_for_complete_experiment(
    node: Node 
) -> None:
    experiment_output_path = "/dev/shm/stat_0.out"
    return node.file_exists(experiment_output_path)


def is_replay_running(
    node: Node
) -> bool:
    running = False 
    ps_output = node.ps()
    for ps_row in ps_output.split('\n'):
        if "bin/cachebench" in ps_row:
            running = True 
            break 
    return running


def run_experiment(host_name: str) -> bool:
    with open("config.json", "r") as config_handle:
        config_dict = load(config_handle)
        creds = config_dict["creds"]
        mounts = config_dict["mounts"]
        nodes = config_dict["nodes"]

    node_info = nodes[host_name]
    node = Node(
            node_info["host"], 
            node_info["host"], 
            creds[node_info["cred"]], 
            mounts[node_info["mount"]])
    host_name = node.host 
    machine_name = node.machine_name

    if is_replay_running(node):
        print("Replay already running so quit.")
        return 0 
    print("No replay running on this host.")


    replay_output_file = "/dev/shm/tracereplay/stat_0.out"
    if node.file_exists(replay_output_file):
        print("Incomplete experiment in the node.")
        return

    disk_status = check_storage_file(node, "~/disk", "disk.file", 980*1000)
    if not node.file_exists("~/nvm/disk.file"):
        nvm_status = check_storage_file(node, "~/nvm", "disk.file", 1000)
    else:
        nvm_status = 1

    cachelib_status = test_cachebench(node)
    if cachelib_status == 0:
        install_cachebench(node)
        cachelib_status = test_cachebench(node)
    cydonia_status = setup_cydonia(node)

    if all([disk_status, nvm_status, cachelib_status, cydonia_status]):
        print("{} ready for experiments")
        run_next_experiment(node)
    else:
        print([disk_status, nvm_status, cachelib_status, cydonia_status])
        print("Not ready!")


def get_remote_block_trace_path(
    replay_db: ReplayDB,
    local_block_trace_path: str 
) -> str:
    """Get the remote block trace path given a local path of a block trace file.

    Args:
        local_block_trace_path: Path of local block trace file to be transfered to remote node. 
    
    Returns:
        remote_block_trace_path: The corresponsind remote block trace path for the local block trace path. 
    """
    return "/dev/shm/{}".format(replay_db.get_remote_file_name(local_block_trace_path))


def run_next_experiment(node: Node):
    host_name = node.host
    machine_name = node.machine_name
    for experiment_entry in experiment_list:
        if replay_db.has_replay_started(node.machine_name, experiment_entry):
            continue 
        
        replay_rate = 1 if "replayRate" not in experiment_entry["kwargs"] else experiment_entry["kwargs"]["replayRate"]
        t1_size_mb = experiment_entry["t1_size_mb"]
        t2_size_mb = 0 if "nvmCacheSizeMB" not in experiment_entry["kwargs"] else experiment_entry["kwargs"]["nvmCacheSizeMB"]
        block_trace_path = replay_db.get_full_block_trace_path_from_relative_path(experiment_entry["block_trace_path"])

        chmod_cmd = "sudo chmod -R a+r /sys/class/powercap/intel-rapl"
        exit_code, stdout, stderr = node.exec_command(chmod_cmd.split(' '))

        # make sure the latest version of the package is running 
        remote_trace_path = get_remote_block_trace_path(replay_db, block_trace_path)
        local_block_trace_size_bytes = Path(block_trace_path).expanduser().stat().st_size
        remote_block_trace_size_bytes = node.get_file_size(remote_trace_path)

        print("{}: Local trace {}={}, remote trace {}={}".format(host_name, 
            block_trace_path, 
            local_block_trace_size_bytes, 
            remote_trace_path, 
            remote_block_trace_size_bytes))

        if local_block_trace_size_bytes != remote_block_trace_size_bytes:
            node.scp(block_trace_path, remote_trace_path)
            print("{}: Transfer local path {} to remote path {}.".format(host_name, block_trace_path, remote_trace_path))
        else:
            print("{}: Corresponding remote path {} already exists for local path {}.".format(host_name, remote_trace_path, block_trace_path))
        
        replay_db.mark_replay_started(machine_name, host_name, block_trace_path, replay_rate, t1_size_mb, t2_size_mb)
        replay_cmd = "nohup python3 ~/disk/CacheLib/phdthesis/scripts/fast24/TraceReplay.py {} {} >> /dev/shm/replay.log 2>&1".format(
            remote_trace_path, 
            t1_size_mb)

        if t2_size_mb > 0:
            replay_cmd += " --t2_size_mb {}".format(t2_size_mb)
        
        if replay_rate > 1:
            replay_cmd += " --replay_rate {}".format(replay_rate)

        print("{}: Replay cmd:{}, block trace size: {}, path: {}".format(host_name,
            replay_cmd, 
            int(node.get_file_size(str(block_trace_path.absolute()))//(1024**2)), 
            block_trace_path))

        node.nonblock_exec_cmd(replay_cmd.split(' '))
        break


if __name__ == "__main__":
    parser = ArgumentParser(description="Run block trace replay in remote nodes.")
    parser.add_argument('host_name', type=str, help="The host to run experiments in.")
    args = parser.parse_args()

    run_experiment(args.host_name)