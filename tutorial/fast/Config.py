from pathlib import Path 


class Config:
    def __init__(self):
        self.source_path = Path("/research2/mtc/cp_traces/pranav")
        self.trace_path = self.source_path.joinpath("block_traces/")
        self.sample_trace_path = self.source_path.joinpath("sample_block_traces/")

        self.metadata_dir_path = self.source_path.joinpath("meta")
        self.block_metadata_dir_path = self.metadata_dir_path.joinpath("block_features")
        self.sample_metadata_dir_path = self.metadata_dir_path.joinpath("sample_split")
        self.replay_metadata_dir_path = self.metadata_dir_path.joinpath("replay")

        self.replay_python_script_substring = "Replay.py"
        self.replay_cachebench_binary_substring = "bin/cachebench"
        self.workload_type_arr = ["test", "cp"]

        self.remote_output_dir = "/run/replay"
        self.replay_output_file_list = [
            "{}/config.json".format(self.remote_output_dir),
            "{}/usage.csv".format(self.remote_output_dir),
            "{}/power.csv".format(self.remote_output_dir),
            "{}/tsstat_0.out".format(self.remote_output_dir),
            "{}/stat_0.out".format(self.remote_output_dir),
            "{}/stdout.dump".format(self.remote_output_dir),
            "{}/stderr.dump".format(self.remote_output_dir)
        ]

        self.fast24 = {
            "workloads": {
                "test": ["w66"],
                "cp": ["w66", "w09", "w18", "w64", "w92"]
            },
            "experiment": {
                "replay_rate_arr": [3, 2, 1],
                "sample_rate_arr": [0.01, 0.05, 0.1, 0.2, 0.4, 0.8],
                "bits_arr": [12, 8, 4, 0],
                "seed_arr": [42],
                "cache_size_ratio_arr": [0.1, 0.2, 0.4, 0.6],
                "sample_type": "iat",
                "random_seed": 42, 
                "max_t1_size_mb": 110000,
                "max_t2_size_mb": 400000,
                "min_t1_size_mb": 100,
                "min_t2_size_mb": 150,
                "num_block_threads": 16,
                "num_async_threads": 16,
                "max_pending_block_requests": 128 
            }
        }
    

    def get_fast24_block_trace_list(self) -> list:
        """Get the list of all block trace relevant for FAST 24 submission."""
        block_trace_path_arr = []
        for workload_type in self.workload_type_arr:
            workload_name_arr = self.fast24["workloads"][workload_type]
            for workload_name in workload_name_arr: 
                block_trace_path = self.trace_path.joinpath(workload_type, "{}.csv".format(workload_name))
                block_trace_path_arr.append(block_trace_path)
                assert block_trace_path.exists(), "Block trace path {} does not exist!".format(block_trace_path)
        return block_trace_path_arr


    def get_workloads(self) -> list:
        workload_arr = []
        for workload_type in self.workload_type_arr:
            workload_name_arr = self.fast24["workloads"][workload_type]
            for workload_name in workload_name_arr: 
                workload_arr.append((workload_type, workload_name))
        return workload_arr 
