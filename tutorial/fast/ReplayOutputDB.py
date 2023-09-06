from pathlib import Path 
from Config import Config 
from pandas import DataFrame, read_csv
from itertools import chain 


def get_replay_params_from_experiment_name(name: str) -> dict:
    """Get replay parameters from experiment name.
    
    Args:
        name: String used to represent replay parameters. 

    Returns:
        replay_params: Dictionary of replay parameters. 
    """
    replay_params = {}
    split_name = name.split('_')
    for param_str in split_name:
        split_param_str = param_str.split('=')
        metric_name, metric_value = split_param_str
        replay_params[metric_name] = metric_value
    return replay_params

    
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


class ReplayOutput:
    def __init__(self, output_dir_path: str) -> None:
        self.output_dir_path = output_dir_path
        self.replay_params = get_replay_params_from_experiment_name(self.output_dir_path.name)
        self._load_stat_file()
        self._load_workload_params()


    def is_sample_output(self) -> bool:
        output_super_dir_name = self.output_dir_path.parent.name
        split_output_super_dir_name = output_super_dir_name.split("_")
        return len(split_output_super_dir_name) == 3
    

    def _load_stat_file(self):
        stat_dict = {}
        stat_file_handle = self.output_dir_path.joinpath("stat_0.out").open("r")
        line = stat_file_handle.readline()
        while line:
            line = line.rstrip()
            if not line:
                line = stat_file_handle.readline()
                continue 
            split_line = line.split('=')
            metric_name, metric_val = split_line
            stat_dict[metric_name] = metric_val
            line = stat_file_handle.readline()
        self.stat = stat_dict
    

    def _load_workload_params(self):
        workload_params = {}
        if self.is_sample_output():
            sample_param_str = self.output_dir_path.parent.name 
            split_sample_param_str = sample_param_str.split('_')
            workload_params["rate"], workload_params["bits"], workload_params["seed"] = split_sample_param_str
            workload_params["name"] = self.output_dir_path.parent.parent.name
            workload_params["type"] = self.output_dir_path.parent.parent.parent.name 
            workload_params["sample"] = self.output_dir_path.parent.parent.parent.parent.name 
        else:
            workload_params["name"] = self.output_dir_path.parent.name 
            workload_params["type"] = self.output_dir_path.parent.parent.name 
        self.workload_params = workload_params

    
    def write_stat_to_csv_file(
            self, 
            csv_file_path: str
    ) -> None:
        csv_file_path = Path(csv_file_path)
        stat = dict(chain(self.stat.items(), self.replay_params.items(), self.workload_params.items()))
        replay_index = "{}_{}_{}_{}_{}_{}_{}_".format(
                                    stat["q"],
                                    stat["bt"],
                                    stat["at"],
                                    stat["t1"],
                                    stat["t2"],
                                    stat["rr"],
                                    stat["it"])
        
        if self.is_sample_output():
            replay_index += "{}_{}_{}_{}_{}_{}".format(
                                        stat["sample"],
                                        stat["type"],
                                        stat["name"],
                                        stat["rate"],
                                        stat["bits"],
                                        stat["seed"])
        else:
            replay_index += "{}_{}".format(stat["type"], stat["name"])
        stat["index"] = replay_index
        df = DataFrame([stat])
        if not csv_file_path.exists():
            df.to_csv(csv_file_path, index=False)
        else:
            cur_df = read_csv(csv_file_path)
            if not len(cur_df[cur_df["index"]==replay_index]):
                df.to_csv(csv_file_path, mode='a', index=False, header=False)


class ReplayOutputDB:
    def __init__(self) -> None:
        self.config = Config()
        self.workload_type_arr = ["cp", "test"]
        self.source = Path("/research2/mtc/cp_traces/pranav/replay/")

        self.complete_replay_meta_file_path = self.config.replay_metadata_dir_path.joinpath("done.csv")
        self.complete_sample_replay_meta_file_path = self.config.replay_metadata_dir_path.joinpath("sample_done.csv")
    

    def update_completed_replay(self) -> None:
        """Update data of completed replays."""
        output_files = self.get_all_output_files()
        for output_file_path in output_files:
            if output_file_path.name != "host":
                continue 

            if len(list(output_file_path.parent.iterdir())) == 1:
                continue 

            print("Complete experiment found.")
            print(output_file_path.parent)
            replay_output = ReplayOutput(output_file_path.parent)
            if replay_output.is_sample_output():
                replay_output.write_stat_to_csv_file(self.complete_sample_replay_meta_file_path)
            else:
                replay_output.write_stat_to_csv_file(self.complete_replay_meta_file_path)


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
    output_db = ReplayOutputDB()
    output_db.update_completed_replay()