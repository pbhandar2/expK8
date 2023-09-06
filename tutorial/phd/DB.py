from pathlib import Path 
from expK8.remoteFS.Node import Node, RemoteRuntimeError


def node_has_complete_experiment(node: Node) -> bool:
    pass 


def get_experiment_name_str(params: dict) -> str:
    return "q={}_bt={}_at={}_t1={}_t2={}_rr={}_it={}".format(
        params["max_pending_block_requests"],
        params["num_block_threads"],
        params["num_async_threads"],
        params["t1_size_mb"],
        params["t2_size_mb"],
        params["replay_rate"],
        params["iteration"])


class DB:
    def __init__(self) -> None:
        self.workload_type_arr = ["cp", "test"]
        self.source = Path("/research2/mtc/cp_traces/pranav/replay/")
    




    def transfer_complete_experiment(
            self,
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

        output_file_list = [stat_file_path, 
                                ts_stat_file_path, 
                                config_file_path, 
                                stdout_file_path, 
                                stderr_file_path, 
                                power_file_path, 
                                usage_file_path]
        
        for output_file in output_file_list:
            pass 


    def init_output_dir(
            self,
            machine_name: str, 
            params: dict,
            host_name: str 
    ) -> None:
        output_dir_path = self.get_output_dir_path(machine_name, params)
        if output_dir_path.exists():
            raise ValueError("Output directory {} already exists!".format(output_dir_path))
        output_dir_path.mkdir(exist_ok=True, parents=True)
        with output_dir_path.joinpath("host").open("w+") as host_file_handle:
            host_file_handle.write(host_name)
    

    def get_output_dir_path(
        self,
        machine_name: str, 
        params: dict 
    ) -> Path: 
        """Get the path to directory where output of experiment with given parameters are stored. 

        Args:
            machine_name: Name of machine where trace is replayed. 
            params: Dictionary containing parameters of the experiment. 
        
        Return:
            output_dir_path: The directory containing the output of block trace replay.
        """
        block_trace_path = Path(params["block_trace_path"])
        output_dir_path = self.source.joinpath(machine_name)
        if params['sample']:
            sample_params_str = block_trace_path.stem 
            workload_name = block_trace_path.parent.name 
            workload_type = block_trace_path.parent.parent.name 
            assert workload_type in self.workload_type_arr, \
                "Unidentified workload type {}, allowed {}".format(workload_type, self.workload_type_arr)
            sample_type = block_trace_path.parent.parent.parent.name
            output_dir_path = output_dir_path.joinpath("sample", sample_type, workload_type, workload_name, sample_params_str)
        else:
            workload_name = block_trace_path.stem 
            workload_type = block_trace_path.parent.name 
            assert workload_type in self.workload_type_arr, \
                "Unidentified workload type {}, allowed {}".format(workload_type, self.workload_type_arr)
            output_dir_path = output_dir_path.joinpath(workload_type, workload_name)
        return output_dir_path.joinpath(get_experiment_name_str(params))