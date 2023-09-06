from json import load, dumps, loads
from pathlib import Path 
from RunExperiment import is_replay_running
from ReplayDB import ReplayDB
from expK8.remoteFS.Node import Node, RemoteRuntimeError


replay_db = ReplayDB("/research2/mtc/cp_traces/pranav/replay/")



def get_config_dict(
    node: Node 
) -> dict:
    config_file_path = "/dev/shm/config.json"
    if not node.file_exists(config_file_path):
        return None 

    stdout, stderr, exit_code = node.exec_command("cat {}".format(config_file_path).split(' '))
    if exit_code:
        return None
    else:
        return loads(stdout.rstrip())


def replay_has_completed(
    node: Node 
) -> None:
    """Replay has completed. 

    Args:
        node: The remote node. 
    """
    output_dir = "/dev/shm"
    stat_file_path = "{}/stat_0.out".format(output_dir)
    ts_stat_file_path = "{}/tsstat_0.out".format(output_dir)
    config_file_path = "{}/config.json".format(output_dir)
    power_file_path = "{}/power.csv".format(output_dir)
    usage_file_path = "{}/usage.csv".format(output_dir)
    stderr_file_path = "{}/stderr.dump".format(output_dir)
    stdout_file_path = "{}/stdout.dump".format(output_dir)

    # get the output dir of the experiment 
    config_dict = get_config_dict(node)

    if config_dict is None:
        return {}
    else:
        return config_dict


def check_for_complete_experiment(
    node: Node 
) -> None:
    experiment_output_path = "/dev/shm/stat_0.out"
    return node.file_exists(experiment_output_path)


def track_all_experiments(config_file_path: str) -> dict:
    with open(config_file_path, "r") as config_handle:
        config_dict = load(config_handle)
        creds = config_dict["creds"]
        mounts = config_dict["mounts"]
        nodes = config_dict["nodes"]

    status = {}
    for node_name in nodes:
        node_info = nodes[node_name]
        node = Node(node_info["host"], 
                        node_info["host"], 
                        creds[node_info["cred"]], 
                        mounts[node_info["mount"]])
        
        is_replay_running_flag = is_replay_running(node)
        has_output_file = check_for_complete_experiment(node)
        cur_status = {
            "running": is_replay_running, 
            "has_output_file": has_output_file
        }
        status[node.host] = cur_status

        if not is_replay_running_flag and check_for_complete_experiment(node):
            print("{}: Experiment compelted handle output".format(node.host))
            config_dict = replay_has_completed(node) 
            if config_dict:
                output_dir = replay_db.get_output_dir_path_from_config_dict(node.machine_name, config_dict)
                output_dir.mkdir(exist_ok=True, parents=True)
                remote_output_dir = "/dev/shm"

                stat_file_path = "{}/stat_0.out".format(output_dir)
                ts_stat_file_path = "{}/tsstat_0.out".format(output_dir)
                config_file_path = "{}/config.json".format(output_dir)
                power_file_path = "{}/power.csv".format(output_dir)
                usage_file_path = "{}/usage.csv".format(output_dir)
                stderr_file_path = "{}/stderr.dump".format(output_dir)
                stdout_file_path = "{}/stdout.dump".format(output_dir)

                remote_stat_file_path = "{}/stat_0.out".format(remote_output_dir)
                remote_ts_stat_file_path = "{}/tsstat_0.out".format(remote_output_dir)
                remote_config_file_path = "{}/config.json".format(remote_output_dir)
                remote_power_file_path = "{}/power.csv".format(remote_output_dir)
                remote_usage_file_path = "{}/usage.csv".format(remote_output_dir)
                remote_stderr_file_path = "{}/stderr.dump".format(remote_output_dir)
                remote_stdout_file_path = "{}/stdout.dump".format(remote_output_dir)

                node.download(remote_stat_file_path, stat_file_path)
                node.download(remote_ts_stat_file_path, ts_stat_file_path)
                node.download(remote_config_file_path, config_file_path)
                node.download(remote_power_file_path, power_file_path)
                node.download(remote_usage_file_path, usage_file_path)
                node.download(remote_stderr_file_path, stderr_file_path)
                node.download(remote_stdout_file_path, stdout_file_path)

                if Path(stat_file_path).exists():
                    node.rm(remote_stat_file_path)
                    print("Replay data transfered to {}. THis node should now be free".format(output_dir))
                    print("The status of current experiment is {}".format(check_for_complete_experiment(node)))
                else:
                    print("Download seems to have failed.")
        else:
            print("{}: replay running: {}, output file: {} Node does not have complete experiment".format(node.host, is_replay_running_flag, has_output_file))
   

if __name__ == "__main__":
    track_all_experiments("config.json")
    replay_db.find_complete_experiment()