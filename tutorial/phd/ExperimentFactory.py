from json import load, dumps
from pathlib import Path 

from GlobalConfig import GlobalConfig


class ExperimentFactory:
    def __init__(self) -> None:
        self.config = GlobalConfig()
        self.exp_config = self.config.fast24["experiment"]
    

    def get_block_trace_path(
            self,
            workload_type: str, 
            workload_name: str 
    ) -> Path:
        return self.config.trace_path.joinpath(workload_type, "{}.csv".format(workload_name))


    def get_sample_block_trace_path(
            self,
            workload_type: str, 
            workload_name: str, 
            sample_type: str, 
            sample_rate: int, 
            random_seed: int, 
            reset_n_lower_bits: int
    ) -> Path:
        sample_file_name = "{}_{}_{}.csv".format(int(100*sample_rate), reset_n_lower_bits, random_seed)
        return self.config.sample_trace_path.joinpath(sample_type, workload_type, workload_name, sample_file_name)
    

    def get_experiment_info_dict(
            self,
            block_trace_path: str, 
            t1_size_mb: int, 
            t2_size_mb: int, 
            replay_rate: int,
            iteration: int = 0 
    ) -> dict:
        experiment_info_dict = {
            "num_block_threads": self.exp_config["num_block_threads"],
            "num_async_threads": self.exp_config["num_async_threads"],
            "max_pending_block_requests": self.exp_config["max_pending_block_requests"]
        }
        experiment_info_dict["t1_size_mb"] = t1_size_mb
        experiment_info_dict["t2_size_mb"] = t2_size_mb
        experiment_info_dict["replay_rate"] = replay_rate
        experiment_info_dict["block_trace_path"] = block_trace_path 
        experiment_info_dict["sample"] = 0 
        experiment_info_dict["iteration"] = iteration
        return experiment_info_dict


    def get_wss_mb(
        self, 
        workload_type: str,
        workload_name: str
    ) -> None:
        block_feature_file_path = self.config.block_metadata_dir_path.joinpath(workload_type, "{}.csv".format(workload_name))
        with block_feature_file_path.open("r") as block_feature_file_handle:
            block_features = load(block_feature_file_handle)
        return block_features["wss"]//(1024*1024), block_features["read_wss"]//(1024*1024), block_features["write_wss"]//(1024*1024)


    def get_base_mt_experiments(
            self,
            block_trace_path: str,
            wss_mb: int,
            replay_rate: int 
    ) -> list:
        experiment_arr = []
        for wss_to_t1_cache_size_ratio in self.exp_config["cache_size_ratio_arr"]:
            t1_size_mb = int(wss_mb * wss_to_t1_cache_size_ratio)
            if t1_size_mb < self.exp_config["min_t1_size_mb"] or t1_size_mb > self.exp_config["max_t1_size_mb"]:
                continue 
            experiment_info_dict = self.get_experiment_info_dict(block_trace_path, t1_size_mb, 0, replay_rate)
            experiment_arr.append(experiment_info_dict)

            for wss_to_t2_cache_size_ratio in self.exp_config["cache_size_ratio_arr"]:
                t2_size_mb = int(wss_mb * wss_to_t2_cache_size_ratio)
                if t2_size_mb < self.exp_config["min_t2_size_mb"] or t2_size_mb > self.exp_config["max_t2_size_mb"]:
                    continue 
                experiment_info_dict = self.get_experiment_info_dict(block_trace_path, t1_size_mb, t2_size_mb, replay_rate)
                experiment_arr.append(experiment_info_dict)
            
        return experiment_arr
    

    def get_sample_experiment(
            self, 
            workload_type: str, 
            workload_name: str, 
            experiment_info: dict,
            sample_type: str 
    ) -> list:
        sample_experiment_list = []
        random_seed = self.exp_config["random_seed"]
        for sample_rate in self.exp_config["sample_rate_arr"]:
            for reset_n_lower_bits in self.exp_config["bits_arr"]:
                sample_block_trace_path = self.get_sample_block_trace_path(
                                            workload_type, 
                                            workload_name, 
                                            sample_type,
                                            sample_rate, 
                                            random_seed,
                                            reset_n_lower_bits)
                
                sample_t1_size_mb = int(sample_rate * experiment_info["t1_size_mb"])
                if sample_t1_size_mb < self.exp_config["min_t1_size_mb"] or sample_t1_size_mb > self.exp_config["max_t1_size_mb"]:
                    continue 

                sample_t2_size_mb = int(sample_rate * experiment_info["t2_size_mb"])
                if sample_t2_size_mb < self.exp_config["min_t2_size_mb"] or sample_t2_size_mb > self.exp_config["max_t2_size_mb"]:
                    continue 

                sample_experiment_info = self.get_experiment_info_dict(
                                                str(sample_block_trace_path.expanduser()),
                                                sample_t1_size_mb,
                                                sample_t2_size_mb,
                                                experiment_info["replay_rate"])
                sample_experiment_info["sample"] = 1
                sample_experiment_list.append(sample_experiment_info)

        return sample_experiment_list
            

    def generate_experiments(self) -> list:
        experiment_arr = []
        sample_type = self.exp_config["sample_type"]
        for replay_rate in self.exp_config["replay_rate_arr"]:
            for workload_type, workload_name in self.config.get_workloads():
                block_trace_path = self.get_block_trace_path(workload_type, workload_name)
                wss_mb, _, _ = self.get_wss_mb(workload_type, workload_name)
                mt_experiment_arr = self.get_base_mt_experiments(str(block_trace_path.expanduser()), wss_mb, replay_rate)
                
                for mt_experiment in mt_experiment_arr:
                    experiment_arr.append(mt_experiment)
                    sample_experiment_arr = self.get_sample_experiment(workload_type, workload_name, mt_experiment, sample_type)
                    experiment_arr += sample_experiment_arr

        return experiment_arr
    

    def get_missing_samples(self) -> list:
        sample_arr = []
        experiment_arr = self.generate_experiments()
        for experiment_info in experiment_arr:
            if not experiment_info["sample"]:
                continue 

            sample_file_path = Path(experiment_info["block_trace_path"])
            if not sample_file_path.exists():
                sample_file_name_arr = sample_file_path.stem.split("_")
                sample_params = {
                    "workload_name": sample_file_path.parent.name,
                    "workload_type": sample_file_path.parent.parent.name, 
                    "rate": float(sample_file_name_arr[0])/100,
                    "bits": int(sample_file_name_arr[1]),
                    "seed": int(sample_file_name_arr[2]),
                    "type": sample_file_path.parent.parent.parent.name,
                    "path": str(sample_file_path.expanduser())
                }
                sample_arr.append(sample_params)
            else:
                print("Sample already exists for experiment {} in path {} of size: {}".format(experiment_info, sample_file_path, sample_file_path.stat().st_size))

        return sample_arr 


if __name__ == "__main__":
    factory = ExperimentFactory()
    experiment_list = factory.generate_experiments()
    print(dumps(experiment_list, indent=2))