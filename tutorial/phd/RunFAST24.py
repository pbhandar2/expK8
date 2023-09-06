from argparse import ArgumentParser 
from pathlib import Path 
from pandas import DataFrame 
from copy import deepcopy 
from time import sleep 

from ExperimentFactory import ExperimentFactory
from NodeFactory import NodeFactory
from DB import DB 
from Runner import run_block_trace_replay
from Setup import is_replay_running, setup_cydonia, install_cachebench


def runFAST24(machine_type):
    exp_output_db = DB()
    node_factory = NodeFactory("../fast24/config.json")
    exp_factory = ExperimentFactory()
    exp_arr = exp_factory.generate_experiments()

    exp_status_arr = []
    for exp in exp_arr:
        exp_status = deepcopy(exp)

        block_trace_path = Path(exp['block_trace_path']) 
        if not block_trace_path.exists():
            exp_status["status"] = "Trace not found."
            exp_status_arr.append(exp_status)
            continue 
        
        output_dir = exp_output_db.get_output_dir_path(machine_type, exp)
        if output_dir.exists():
            with output_dir.joinpath("host").open("r") as host_file_handle:
                host = host_file_handle.read()
            exp_status["status"] = "Already Started in host {}".format(host)
            exp_status_arr.append(exp_status)
            continue 

        free_node_host_name = node_factory.get_free_node(machine_type)
        if not free_node_host_name:
            print("No free node available to run additional experiments.")
            break 
        
        free_node = node_factory.get_node(free_node_host_name)
        free_node.chown("/run")
        free_node.mkdir("/run/replay")
        free_node.chown("/run/replay")
        
        setup_cydonia(free_node)
        install_cachebench(free_node)

        run_block_trace_replay(free_node, exp)

        sleep(5)
        if is_replay_running(free_node):
            exp_output_db.init_output_dir(machine_type, exp, free_node.host)
            exp_status["status"] = "Started in host {}".format(free_node.host)
            exp_status_arr.append(exp_status)
        else:
            exp_status["status"] = "Did not start in host {}".format(free_node.host)
            exp_status_arr.append(exp_status)

        df = DataFrame(exp_status_arr)
        print(df.to_string())  

    df = DataFrame(exp_status_arr)
    print(df.to_string())  
    df.to_csv("status.csv", index=False)      


if __name__ == "__main__":
    parser = ArgumentParser("Run set of replays for FAST 24.")
    parser.add_argument("machine_type", type=str, help="Type of machine to run replays on.")
    args = parser.parse_args()
    runFAST24(args.machine_type)