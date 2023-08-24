from pathlib import Path 
from Config import Config 


def get_experiment_name_str(params: dict) -> str:
    """Get the name of experiment based on replay parameters.
    
    Args:
        params: Dictionary of replay parameters. 
    
    Returns:
        experiment_name_str: String representing experiment name generated using replay parameters. 
    """
    return "q={}_bt={}_at={}_t1={}_t2={}_rr={}_it={}".format(
        params["max_pending_block_requests"],
        params["num_block_threads"],
        params["num_async_threads"],
        params["t1_size_mb"],
        params["t2_size_mb"],
        params["replay_rate"],
        params["iteration"])


class ReplayOutputDB:
    def __init__(self) -> None:
        self.config = Config()
        self.workload_type_arr = ["cp", "test"]
        self.source = Path("/research2/mtc/cp_traces/pranav/replay/")

        self.live_experiment_file = self.config.replay_metadata_dir_path.joinpath("live.csv")
        self.done_experiment_file = self.config.replay_metadata_dir_path.joinpath("done.csv")


    def get_all_output_files(self) -> list:
        """Return all the output files in the replay output directory.
        
        Returns:
            output_file_list: Array of Path objects of output file paths. 
        """
        return list(self.source.rglob("*"))


    def init_output_dir(
            self,
            machine_name: str, 
            params: dict,
            host_name: str 
    ) -> None:
        """Initiate the output directory given the node and replay parameters. 
        
        Args:
            machine_name: The type of machine where replay is run. 
            params: The dictionary containing replay parameters. 
            host_name: The host name of the machine where replay is run. 
        """
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
    

if __name__ == "__main__":
    pass 