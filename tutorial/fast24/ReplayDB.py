"""This class manages files output from block trace replay."""

from json import dumps 
from re import search 
from pathlib import Path 

from pandas import read_csv, DataFrame, concat 


CONST_DICT = {
    "default_max_pending_block_requests": 128,
    "default_num_block_threads": 16,
    "default_num_async_threads": 16,
    "default_iteration": 0
}


class ReplayDB:
    def __init__(self, source_dir):
        self.source_dir = Path(source_dir)
        
    
    def check_replay_output_exists(
        self,
        config_dict: dict
    ) -> bool:
        """Check if output for replay with a given configuration already exists."""
        t1_size_mb = config_dict["cache_config"]["cacheSizeMB"]
        t2_size_mb = config_dict["cache_config"]["nvmCacheSizeMB"]

        block_replay_config = config_dict["test_config"]["blockReplayConfig"]
        block_trace_path = block_replay_config["traces"][0]
        replay_rate = block_replay_config["replayRate"]
        num_block_threads = block_replay_config["blockRequestProcesserThreads"]
        num_async_threads = block_replay_config["asyncIOReturnTrackerThreads"]
        max_pending_block_requests = block_replay_config["maxPendingBlockRequestCount"]

        return self.get_output_dir(
                        machine_name, 
                        block_trace_path, 
                        replay_rate, 
                        t1_size_mb,
                        t2_size_mb, 
                        max_pending_block_requests=max_pending_block_requests, 
                        num_block_threads=num_block_threads, 
                        num_async_threads=num_async_threads).exists()
        
        
    def get_full_block_trace_path_from_relative_path(
        self,
        relative_path: str 
    ) -> str:
        """Get the full block trace from a relative path.

        Args:
            relative_path: Relative path of block trace
        """
        return self.source_dir.parent.joinpath(relative_path)


    def mark_replay_started(
        self, 
        machine_name: str,
        host_name: str, 
        block_trace_path: str,
        replay_rate: int, 
        t1_size_mb: int, 
        t2_size_mb: int, 
        max_pending_block_requests: int = CONST_DICT["default_max_pending_block_requests"],
        num_block_threads: int = CONST_DICT["default_num_block_threads"],
        num_async_threads: int = CONST_DICT["default_num_async_threads"],
        iteration: int = CONST_DICT["default_iteration"]
    ) -> None:
        output_dir = self.get_output_dir(
            machine_name, 
            block_trace_path,
            replay_rate,
            t1_size_mb,
            t2_size_mb,
            max_pending_block_requests=max_pending_block_requests,
            num_block_threads=num_block_threads,
            num_async_threads=num_async_threads,
            iteration=iteration)
        output_dir.mkdir(parents=True)
        with output_dir.joinpath("host").open("w+") as host_file_handle:
            host_file_handle.write(host_name)

    
    def has_replay_started(
        self,
        machine_name: str, 
        replay_info: dict,
        max_pending_block_requests: int = CONST_DICT["default_max_pending_block_requests"],
        num_block_threads: int = CONST_DICT["default_num_block_threads"],
        num_async_threads: int = CONST_DICT["default_num_async_threads"],
        iteration: int = CONST_DICT["default_iteration"]
    ) -> bool:
        """Check if replay has started.
        
        Args:
            machine_name: Name of machine where replay is checked.
            replay_info: Dictionary of replay parameters. 
        
        Returns:
            has_started: Boolean indicating if block replay has started.
        """
        return self.get_output_dir(
            machine_name,
            self.source_dir.joinpath(replay_info["block_trace_path"]),
            replay_info["kwargs"]["replayRate"],
            replay_info["t1_size_mb"],
            0 if "nvmCacheSizeMB" not in replay_info["kwargs"] else replay_info["kwargs"]["nvmCacheSizeMB"],
            max_pending_block_requests=max_pending_block_requests,
            num_block_threads=num_block_threads,
            num_async_threads=num_async_threads,
            iteration=iteration).exists()

    
    def is_sample(
        self, 
        block_trace_path: str
    ) -> bool:
        """Checks if a given block trace path is a sample. Samples do not contain any alphabet characters in filename
        as they have the format: "RATE"_"BITS"_"SEED"."""
        return not search('[a-zA-Z]', Path(block_trace_path).stem)
    

    def get_remote_file_name(
        self, 
        block_trace_path: str 
    ) -> str:
        """Get the name of remote file based on local path of file. 

        Args:
            block_trace_path: Path of block trace.
        
        Return:
            file_name: Name of file in remote node. 
        """
        if self.is_sample(block_trace_path):
            return "{}-{}-{}-{}".format(
                block_trace_path.parent.parent.parent.name,
                block_trace_path.parent.parent.name,
                block_trace_path.parent.name,
                block_trace_path.name
            )
        else:
            return block_trace_path.name 


    def get_output_dir_from_replay_info(
        self,
        machine_name: str, 
        replay_info: dict,
        max_pending_block_requests: int = CONST_DICT["default_max_pending_block_requests"],
        num_block_threads: int = CONST_DICT["default_num_block_threads"],
        num_async_threads: int = CONST_DICT["default_num_async_threads"],
        iteration: int = CONST_DICT["default_iteration"]
    ) -> Path:
        """Get output directory from replay information.
        
        Args:
            machine_name: Name of machine where replay is checked.
            replay_info: Dictionary of replay parameters. 
        
        Returns:
            output_dir: Path to output directory for the replay. 
        """
        return self.get_output_dir(
            machine_name,
            self.source_dir.joinpath(replay_info["block_trace_path"]),
            replay_info["kwargs"]["replayRate"],
            replay_info["t1_size_mb"],
            0 if "nvmCacheSizeMB" not in replay_info["kwargs"] else replay_info["kwargs"]["nvmCacheSizeMB"],
            max_pending_block_requests=max_pending_block_requests,
            num_block_threads=num_block_threads,
            num_async_threads=num_async_threads,
            iteration=iteration)
    

    def get_output_dir_from_config_dict(
        self,
        machine_name: str, 
        config_dict: dict 
    ) -> str:
        """Get output dict from configuration dict. 

        Args:
            config_dict: A dictionary of configuration. 
        
        Returns:
            output_dir: The output directory to contain files. 
        """
        block_trace_path = Path(config_dict["test_config"]["blockReplayConfig"]["traces"][0])
        replay_rate = config_dict["test_config"]["blockReplayConfig"]["replayRate"]
        t1_size_mb = config_dict["cache_config"]["cacheSizeMB"] 

        if "nvmCacheSizeMB" in config_dict["cache_config"]:
            t2_size_mb = config_dict["cache_config"]["nvmCacheSizeMB"]
        else:
            t2_size_mb = 0.0 
        
        max_pending_block_requests = config_dict["test_config"]["blockReplayConfig"]["maxPendingBlockRequestCount"]
        num_block_threads = config_dict["test_config"]["blockReplayConfig"]["blockRequestProcesserThreads"]
        num_async_threads = config_dict["test_config"]["blockReplayConfig"]["asyncIOReturnTrackerThreads"]

        return self.get_output_dir(
                    machine_name, 
                    block_trace_path, 
                    replay_rate, 
                    t1_size_mb, 
                    t2_size_mb, 
                    max_pending_block_requests=max_pending_block_requests, 
                    num_block_threads=num_block_threads,
                    num_async_threads=num_async_threads)
        


        
    def get_output_dir(
        self,
        machine_name: str, 
        block_trace_path: str,
        replay_rate: int,
        t1_size_mb: int, 
        t2_size_mb: int,
        max_pending_block_requests: int = CONST_DICT["default_max_pending_block_requests"],
        num_block_threads: int = CONST_DICT["default_num_block_threads"],
        num_async_threads: int = CONST_DICT[ "default_num_async_threads"],
        iteration: int = CONST_DICT["default_iteration"]
    ) -> Path:
        """Get the path of output directory for block trace replay with specified parameters. 
         
            The directory path has format: [machine_name]/[workload_type]/[workload_name]/[experiment_name]. Variables
            [workload_type] and [workload_name] are derived from [block_trace_path]. How they are derived differs based 
            on whether the block trace path is for a sample or a full block trace. 
            
            The variable [experiment_name] has format: q=[max_pending_block_requests]_bt=[num_block_threads]_at=[num_async_threads]
            _t1=[t1_size_mb]_t2[t2_size_mb]_rr[replay_rate]_it[iteration]. 

        Args:
            machine_name: Name of the machine where replay is to be run. 
            block_trace_path: The path of block trace to be replayed. 
            block_trace_path: Path of the block trace to replay. 
            replay_rate: Value used to divide interarrival times to accelerate trace replay. 
            t1_size_mb: Size of tier-1 cache in MB. 
            t2_size_mb: Size of tier-2 cache in MB.
        
        Return:
            output_dir: Path to the output directory for block trace replay with specified parameters. 
        """
        if self.is_sample(block_trace_path):
            workload_set_name = block_trace_path.parent.parent.parent.name
            workload_name = block_trace_path.parent.name
        else:
            workload_set_name = block_trace_path.parent.name
            workload_name = block_trace_path.stem
        
        experiment_name = "q={}_bt={}_at={}_t1={}_t2={}_rr={}_it={}".format(
            max_pending_block_requests, 
            num_block_threads, 
            num_async_threads, 
            t1_size_mb, 
            t2_size_mb, 
            replay_rate, 
            iteration)
        
        return self.source_dir.joinpath(
            machine_name, 
            workload_set_name,
            workload_name,
            experiment_name)
    

    def get_output_dir_path_from_config_dict(
        self,
        machine_name: str, 
        config_dict: dict 
    ) -> Path:
        """Get the path of output directory from the configuration 
            directory. 
        
        Args:
            machine_name: The name of the machine where replay is run. 
            config_dict: Dictionary of replay configuration. 
        
        Return:
            output_path: Path of the directory that stores replay output. 
        """
        block_trace_path = Path(config_dict["test_config"]["blockReplayConfig"]["traces"][0])
        replay_rate = config_dict["test_config"]["blockReplayConfig"]["replayRate"]
        t1_size_mb = config_dict["cache_config"]["cacheSizeMB"] 
        max_pending_block_requests = config_dict["test_config"]["blockReplayConfig"]["maxPendingBlockRequestCount"]
        num_block_threads = config_dict["test_config"]["blockReplayConfig"]["blockRequestProcesserThreads"]
        num_async_threads = config_dict["test_config"]["blockReplayConfig"]["asyncIOReturnTrackerThreads"]

        if "nvmCacheSizeMB" in config_dict["cache_config"]:
            t2_size_mb = config_dict["cache_config"]["nvmCacheSizeMB"]
        else:
            t2_size_mb = 0.0 

        trace_file_name = block_trace_path.stem 
        return self.get_replay_output_path(
                    machine_name,
                    trace_file_name,
                    t1_size_mb,
                    t2_size_mb,
                    replay_rate,
                    max_pending_block_requests,
                    num_block_threads,
                    num_async_threads,
                    0)


    def get_replay_output_path(
        self,
        machine_name: str, 
        workload_name: str, 
        t1_size_mb: int,
        t2_size_mb: int,
        replay_rate: int,
        max_pending_block_requests: int,
        num_block_threads: int,
        num_async_threads: int, 
        iteration: int
    ) -> Path:
        """Get the output path given the parameters of trace replay.

        Args:
            machine_name: Name of the machine. 
            workload_set_name: Name of the set of workloads. 
            workload_name: Name of the workload. 
            t1_size_mb: Size of T1 cache in MB. 
            t2_size_mb: Size of T2 cache in MB. 
            replay_rate: Value used to divide inter-arrival time to accelerate trace replay. 
            max_pending_block_requests: Maximum number of block requests that can be pending in the system. 
            num_block_threads: Number of threads to process block requests. 
            num_async_threads: Number of threads to process async backing store requests. 
            iteration: The iteration of the experiment. 
        
        Return:
            output_path: Path of the directory where replay output is stored. 
        """
        experiment_name = "q={}_bt={}_at={}_t1={}_t2={}_rr={}_it={}".format(
                            max_pending_block_requests, 
                            num_block_threads, 
                            num_async_threads, 
                            t1_size_mb, 
                            t2_size_mb, 
                            replay_rate, 
                            iteration)
        return self.source_dir.joinpath(
                machine_name,
                workload_name, 
                experiment_name)

    
    def load_experiment_stat(
        self,
        replay_stat_file_path: str,
        output_dir: str 
    ) -> None:
        replay_stat = {}

        with open(replay_stat_file_path, "r") as stat_file_handle:
            line = stat_file_handle.readline().rstrip()
            while line:
                split_line = line.split('=')
                metric_name, metric_value = split_line[0], float(split_line[1])
                replay_stat[metric_name] = metric_value
                line = stat_file_handle.readline().rstrip()

        experiment_name = replay_stat_file_path.parent.name 
        workload_name = replay_stat_file_path.parent.parent.name 
        machine_name = replay_stat_file_path.parent.parent.parent.name 

        replay_stat['experiment'] = experiment_name
        replay_stat['workload'] = workload_name
        replay_stat['machine'] = machine_name

        agg_data_dir = Path(output_dir).joinpath(machine_name, workload_name)
        agg_data_dir.mkdir(exist_ok=True, parents=True)

        agg_data_file_path = agg_data_dir.joinpath("agg.csv")
        if agg_data_file_path.exists():
            old_df = read_csv(agg_data_file_path)
            match_rows = old_df[(old_df['machine'] == machine_name) & \
                                (old_df['workload'] == workload_name) & \
                                (old_df['experiment'] == experiment_name)]

            if len(match_rows) > 0:
                return 
            
            cur_df = DataFrame([replay_stat])
            df = concat([old_df, cur_df])
            df.to_csv(agg_data_file_path, index=False)
        else:
            df = DataFrame([replay_stat])
            df.to_csv(agg_data_file_path, index=False)


    def find_complete_experiment(self) -> list:
        complete_experiment = {}
        for machine_dir in self.source_dir.iterdir():
            machine_name = machine_dir.name 
            for workload_dir in machine_dir.iterdir():
                for experiment_dir in workload_dir.iterdir():
                    complete = False 

                    for experiment_file in experiment_dir.iterdir():
                        if "stat_0.out" == experiment_file.name:
                            complete = True 
                            self.load_experiment_stat(experiment_file, "/research2/mtc/cp_traces/pranav/meta/replay")
                    
                    if complete:
                        complete_experiment[experiment_dir.name] = 1
                    else:
                        complete_experiment[experiment_dir.name] = 0 

        print(dumps(complete_experiment, indent=2))
                            