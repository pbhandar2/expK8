"""BlockTraceReplay runs block trace replay in remote nodes. """
from time import sleep 
from json import load, loads, dumps 
from pathlib import Path 
from argparse import ArgumentParser
from logging import getLogger, Formatter, INFO, handlers, Logger

from expK8.remoteFS.RemoteFS import RemoteFS
from expK8.remoteFS.Node import Node, RemoteRuntimeError
from ReplayDB import ReplayDB

from NodeSetup import create_backing_file, create_nvm_file, install_cachelib, install_cydonia


CONST_DICT = {
    "default_config_path": "config.json",
    "default_experiment_file_path": "experiments/sample_cp-test_w66.json",
    "default_replay_output_dir": "/research2/mtc/cp_traces/pranav/replay",

    "remote_block_trace_dir": "/dev/shm",
    "remote_replay_output_dir": "/dev/shm/tracereplay/",
    "remote_setup_status_file": "/dev/shm/setup.status",
    "remote_install_status_file": "/dev/shm/install.status",
    "experiment_completion_file_name": "stat_0.out",

    "replay_python_script_substring": "Replay.py",
    "replay_cachebench_binary_substring": "bin/cachebench",
    "replay_io_file_name": "disk.file",
    "replay_create_file_substring": "dd"
}


class RemoteTraceReplay:
    def __init__(
        self, 
        remote_fs: RemoteFS
    ) -> None:
        """BlockTraceReplay runs block trace replay in remote nodes. 

        Attributes:
            remote_fs: RemoteFS is used to communciate with remote nodes. 
            replay_db: ReplayDB is used to manage output from block trace replay.
            logger: Logger to log important events to a log file. 
        """
        self.remote_fs = remote_fs 
        self.replay_db = ReplayDB("/research2/mtc/cp_traces/pranav/replay/")

        # Setup logger 
        self.logger = getLogger("remote_trace_replay")
        self.logger.setLevel(INFO)
        logHandler = handlers.RotatingFileHandler("/dev/shm/remotetrace.log", maxBytes=25*1e6)
        logHandler.setLevel(INFO)
        formatter = Formatter("%(asctime)s %(name)s %(levelname)s: %(message)s")
        logHandler.setFormatter(formatter)
        self.logger.addHandler(logHandler)
        self.logger.info("INIT")

    
    def reset(self) -> None:
        """Kill all processes related to block trace replay in all nodes."""
        self.logger.info("RESET")
        host_name_arr = self.remote_fs.get_all_host_names()
        for host_name in host_name_arr:
            self.kill_trace_replay(self.remote_fs.get_node(host_name))


    def mountpoint_setup(
        self,
        node: Node, 
        mountpoint: str, 
        chown_mountpoint: bool = True
    ) -> None:
        """Setup mountpoints in remote node.

        Args:
            node: Node to setup. 
            chown_mountpoint: Flag indicating whether to change ownership of mountpoints to the user. 
        """
        is_setup = False 
        if node.dir_exists(mountpoint):
            mountpoint_setup = True 
        else:
            node.mkdir(mountpoint)
        
        if chown_mountpoint:
            self.node.chown(mountpoint)


    def storage_files_setup(
        self,
        node: Node 
    ) -> bool:
        """Setup storage files for experiments. 

        Args:
            node: Node to setup. 
        
        Returns:
            is_ready: Indicating if storage files are ready to run experiments. 
        """
        is_ready = False 
        disk_mountpoint, nvm_mountpoint = "~/disk", "~/nvm"
        self.mountpoint_setup(disk_mountpoint)
        self.mountpoint_setup(nvm_mountpoint)

        disk_file_path, nvm_file_path = "~/disk/disk.file", "~/nvm/disk.file"
        desired_disk_file_size_byte = int(node.get_mountpoint_info(disk_mountpoint)["size"]*0.9)
        desired_nvm_file_size_byte = int(node.get_mountpoint_info(disk_mountpoint)["size"]*0.99)
        
        if not self.create_file(node, disk_file_path, desired_disk_file_size_byte) and not self.create_file(node, nvm_file_path, desired_nvm_file_size_byte):
            is_ready = True 
        
        return is_ready 


    def check_if_replay_process(
        self,
        ps_row: str
    ) -> bool:
        """Check if a process is related to trace replay.

        Args:
            ps_row: A row from output of 'ps au' command. 
        
        Returns:
            is_replay: Boolean indicating if this process belong to trace replay. 
        """
        is_replay_python_script = CONST_DICT["replay_python_script_substring"] in ps_row
        is_replay_cachebench_binary = CONST_DICT["replay_cachebench_binary_substring"] in ps_row
        return is_replay_python_script or is_replay_cachebench_binary
    

    def check_if_create_file_process(
        self,
        ps_row: str 
    ) -> bool:
        """Check if the process is related to creating a IO file. 

        Args:
            ps_row: A row from output of 'ps au' command. 
        
        Returns:
            is_create_file: Boolean indicating if this process is creating a IO file. 
        """
        is_disk_file_in_ps_command = CONST_DICT["replay_io_file_name"] in ps_row 
        is_dd_in_ps_command = CONT_DICT["replay_create_file_substring"] in ps_row 
        return is_disk_file_in_ps_command and is_dd_in_ps_command


    def kill_trace_replay(
        self,
        node: Node 
    ) -> None:
        """Kill the block trace replay running in remote node.

        Args:
            node: Node where the process is to be killed. 
        """
        ps_output = node.ps()
        """There are 3 processes running per block trace replay so we need to check and kill them all. 
            1. nohup - The nohup processing running the TraceReplay.py python script. 
            2. python script - The python script that runs trace replay and tracks
                memory, cpu and power usage. 
            3. c++ binary - The CacheBench binary running block trace replay. 
        """
        for ps_row in ps_output.split("\n"):
            if self.check_if_replay_process(ps_row):
                pid = int(ps_row.strip().split(' ')[0])
                node.kill(pid)
    

    def kill_create_file_process(
        self,
        ndoe: Node 
    ) -> bool:
        """Kill any process related to creating a file.

        Args:
            node: Node where the process is to be killed. 
        
        Returns:
            killed: Boolean indicating if any processes were killed. 
        """
        killed = False  
        ps_output = node.ps()
        """There are 2 processes that could be running to create file: a nohup process to prevent termination
            and the file creation process. Both contains substring "disk.file" and "dd" in it so we can 
            terminate process containing this substring in kill both processes."""
        for ps_row in ps_output.split("\n"):
            if self.check_if_create_file_process(ps_row):
                pid = int(ps_row.strip().split(' ')[0])
                node.kill(pid)
                killed = True 
        return killed 


    def check_for_replay_process(
        self,
        node: Node
    ) -> bool:
        """Check if a node has a replay process running. 

        Args:
            node: The node to check if replay is running. 

        Returns:
            running: Boolean indicating if any replay processes is found running. 
        """
        running = False 
        ps_output = node.ps()
        """There are 3 processes running per block trace replay so we need to check and kill them all. 
            1. nohup - The nohup processing running the TraceReplay.py python script. 
            2. python script - The python script that runs trace replay and tracks
                memory, cpu and power usage. 
            3. c++ binary - The CacheBench binary running block trace replay. 
        """
        for ps_row in ps_output.split("\n"):
            if self.check_if_replay_process(ps_row):
                running = True 
                break
        return running 
    

    def create_file(
        self, 
        node: Node, 
        path: str,
        desired_size_byte: int,
        file_size_diff_tolerance_byte: int = 10240
    ) -> bool:
        """Create a file of desired size. 

        Args:
            node: Node where file is to created.
            path: Path of file to be evaluated. 
            desired_size_byte: The desired size of the file in bytes. 
        
        Return:
            created: Boolean indicating if file creation was started.
        """
        file_size_byte = node.get_file_size(path)
        file_size_diff_byte = abs(desired_size_byte - file_size_byte)
        print("Byte difference of {} in desired and current size of path {}".format(file_size_diff_byte, path))

        created = False 
        if file_size_diff_byte > file_size_diff_tolerance_byte:
            if node.file_exists(path):
                node.rm(path)
            
            self.logger.info("{}: Creating a file of size {}MB in path {} because size diff was {} bytes.".format(
                node.host,
                desired_size_byte//(1024*1024),
                path,
                file_size_diff_byte
            ))
            node.create_random_file_nonblock(path, desired_size_byte//(1024*1024))
            created = True 
        
        return True 


    def check_for_replay_output(
        self,
        node: Node 
    ) -> bool:
        """Check if node contains output generated after completion of trace replay. 

        Args:
            node: Node where we check output from trace replay exists. 
        
        Returns:
            exists: Boolean indicating if the output for trace replay exists. 
        """
        return node.file_exists("{}/{}".format(CONST_DICT["remote_replay_output_dir"], CONST_DICT["experiment_completion_file_name"]))
    


    def get_remote_block_trace_path(
        self,
        local_block_trace_path: str 
    ) -> str:
        """Get the remote block trace path given a local path of a block trace file.

        Args:
            local_block_trace_path: Path of local block trace file to be transfered to remote node. 
        
        Returns:
            remote_block_trace_path: The corresponsind remote block trace path for the local block trace path. 
        """
        return "{}/{}".format(CONST_DICT["remote_block_trace_dir"], self.replay_db.get_remote_file_name(local_block_trace_path))


    def run_trace_replay(
        self,
        node: Node,
        block_trace_path: str,
        replay_rate: int,
        t1_size_mb: int,
        t2_size_mb: int
    ) -> bool:
        """Run trace replay on remote node. 

        Args:
            node: The node object lets you run commands in remote node. 
            block_trace_path: Path of the block trace to replay. 
            replay_rate: Value used to divide interarrival times to accelerate trace replay. 
            t1_size_mb: Size of tier-1 cache in MB. 
            t2_size_mb: Size of tier-2 cache in MB.
        
        Returns:
            did_trace_run: Boolean indicating if this block trace was run.
        """
        host_name, machine_name = node.host, node.machine_name
        if self.check_for_replay_process(node):
            print("{}: already running block replay.".format(host_name))
            return False 
        
        if self.check_for_replay_output(node):
            print("{}: already has replay output.".format(host_name))
            return False 
        
        print("{}: host available for replay \n\ttrace: {}, \n\treplay: {}, \n\tt1: {}, \n\tt2: {}".format(host_name, block_trace_path, replay_rate, t1_size_mb, t2_size_mb))


        chmod_cmd = "sudo chmod -R a+r /sys/class/powercap/intel-rapl".split(' ')
        stdout, stderr, exit_code = node.exec_command(chmod_cmd)
        if exit_code:
            raise RemoteRuntimeError(chmod_cmd, node.host, exit_code, stdout, stderr)
        
        install_cydonia(node)

        # make sure the latest version of the package is running 
        remote_trace_path = self.get_remote_block_trace_path(block_trace_path)
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
        
        self.logger.info("{}:start:machine={},trace={},replay={},t1={},t2={}".format(
            host_name,
            machine_name,
            block_trace_path,
            replay_rate,
            t1_size_mb,
            t2_size_mb
        ))

        self.replay_db.mark_replay_started(machine_name, host_name, block_trace_path, replay_rate, t1_size_mb, t2_size_mb)
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
        sleep(2)
        return True 


    def status(self):
        host_name_arr = self.remote_fs.get_all_host_names()
        for host_name in host_name_arr:
            node = self.remote_fs.get_node(host_name)
            stdout = self.check_for_replay_process(node)
            if stdout:
                print("{}: Experiment running: {}".format(host_name, stdout))
            else:
                print("{}: Experiment not running.".format(host_name))


    def get_status(
        self,
        replay_list: list 
    ) -> None:
        host_name_arr = self.remote_fs.get_all_host_names()
        for host_name in host_name_arr:
            node = self.remote_fs.get_node(host_name)
            print("{}: replay: {}".format(node.host, self.check_for_replay_process(node)))
        
        for replay_info in replay_list:
            if self.replay_db.has_replay_started(node.machine_name, replay_info):
                running_host = ""
                output_dir = self.replay_db.get_output_dir_from_replay_info(node.machine_name, replay_info)
                if output_dir.joinpath("host").exists():
                    with output_dir.joinpath("host").open("r") as host_handle:
                        running_host = host_handle.read()
                
                print("{}:running:replay={}".format(
                    running_host,
                    replay_info
                ))




    def install_runner(
        self,
        node: Node 
    ) -> None:
        """ Install the runner package in node running experiment. 

        Args:
            node: Node where package is to be installed. 
        """
        multi_command_install = """cd ~/disk/CacheLib/phdthesis/; git pull origin main
        pip3 install ~/disk/CacheLib/phdthesis/cydonia --user"""
        for install_cmd in multi_command_install.split("\n"):
            stdout, stderr, exit_code = node.exec_command(
                                            install_cmd.strip().split(' '), 
                                            timeout=600)
            if exit_code:
                raise RemoteRuntimeError(install_cmd, node.host, exit_code, stdout, stderr)
   

    def run(
        self, 
        replay_list: list 
    ) -> None:
        """Run all block trace replay specified in the list across remote nodes.
        
        Args:
            replay_list: List of dictionaries containing details of replay to run.
        """
        host_name_arr = self.remote_fs.get_all_host_names()
        for host_name in host_name_arr:
            node = self.remote_fs.get_node(host_name)
            if self.check_for_replay_process(node):
                continue

            if not self.install_runner(node):
                self.logger.info("{}:failed install".format(
                    host_name,
                    node.machine_name,
                    replay_info
                ))
                continue
            
            for replay_info in replay_list:
                if self.replay_db.has_replay_started(node.machine_name, replay_info):
                    running_host = ""
                    output_dir = self.replay_db.get_output_dir_from_replay_info(node.machine_name, replay_info)
                    if output_dir.joinpath("host").exists():
                        with output_dir.joinpath("host").open("r") as host_handle:
                            running_host = host_handle.read()
                    
                    if running_host == host_name:
                        self.logger.info("{}:running:machine={},replay={}".format(
                            host_name,
                            node.machine_name,
                            replay_info
                        ))
                    continue 

                replay_rate = 1 if "replayRate" not in replay_info["kwargs"] else replay_info["kwargs"]["replayRate"]
                t1_size_mb = replay_info["t1_size_mb"]
                t2_size_mb = 0 if "nvmCacheSizeMB" not in replay_info["kwargs"] else replay_info["kwargs"]["nvmCacheSizeMB"]
                block_trace_path = self.replay_db.get_full_block_trace_path_from_relative_path(replay_info["block_trace_path"])

                self.run_trace_replay(
                    node, 
                    block_trace_path,
                    replay_rate,
                    t1_size_mb, 
                    t2_size_mb)
                break 

    
    def handle_replay_complete(
        node: Node 
    ) -> None:
        config_str = node.exec_command["cat", "{}/config.json".format(CONST_DICT["remote_replay_output_dir"])]
        config_dict = loads(config_str)
        machine_name = node.machine_name 

        t1_size_mb = config_dict["cache_config"]["cacheSizeMB"]
        t2_size_mb = config_dict["cache_config"]["nvmCacheSizeMB"]

        block_replay_config = config_dict["test_config"]["blockReplayConfig"]
        block_trace_path = block_replay_config["traces"][0]
        replay_rate = block_replay_config["replayRate"]
        num_block_threads = block_replay_config["blockRequestProcesserThreads"]
        num_async_threads = block_replay_config["asyncIOReturnTrackerThreads"]
        max_pending_block_requests = block_replay_config["maxPendingBlockRequestCount"]

        output_dir = self.replay_db.get_output_dir(
                        machine_name, 
                        block_trace_path, 
                        replay_rate, 
                        t1_size_mb,
                        t2_size_mb, 
                        max_pending_block_requests=max_pending_block_requests, 
                        num_block_threads=num_block_threads, 
                        num_async_threads=num_async_threads)
        
        print(output_dir)
        print(block_replay_config)

        output_file_list = node.get_file_list_in_dir(CONST_DICT["remote_replay_output_dir"])
        for output_file_path in output_file_list:
            file_name = output_file_path.split("/")[-1]
            print("Upload {} to {}/{}".format(output_file_path), output_dir, file_name)
    

    def get_node_setup_status(
        self,
        node: Node,
        force_io_setup: bool 
    ) -> dict:
        """Get the setup status of a node. 

        Args:
            node: Node whose setup status to return. 
            force_io_setup: Boolean indicating if we want to force IO setup to run again. 
        
        Returns:
            all_setup_status: A dictionary containing the status for different steps of setup.
        """
        setup_status = {}
        setup_status["backing_file_status"] = create_backing_file(node, force_io_setup)
        setup_status["nvm_file_status"] = create_nvm_file(node, force_io_setup)
        setup_status["cachelib_status"] = install_cachelib(node)
        setup_status["cydonia_status"] = install_cydonia(node)
        return setup_status 
    

    def get_all_setup_status(
        self,
        force_io_setup: bool = False 
    ) -> dict:
        """Get the setup status of all nodes.
    
        Args:
            force_io_setup: Boolean indicating if IO files are forced to be recreated. 
        
        Returns:
            setup_status: Dictionary containing the setup status of different setup steps. 
         """
        host_name_arr = self.remote_fs.get_all_live_host_names()
        all_setup_status = {}
        for host_name in host_name_arr:
            current_status = {}
            node = self.remote_fs.get_node(host_name)
            setup_status = self.get_node_setup_status(node, force_io_setup)
            all_setup_status[host_name] = setup_status
        return all_setup_status


def main(args):
    with args.c.open("r") as config_file_handle:
        fs_config = load(config_file_handle)
    fs = RemoteFS(fs_config)

    with args.e.open("r") as experiment_file_handle:
        experiment_list = load(experiment_file_handle)

    remote_trace_replay = RemoteTraceReplay(fs)
    if args.r:
        remote_trace_replay.reset()
    
    # check setup of each node 
    all_setup_status = remote_trace_replay.get_all_setup_status()
    for host_name in all_setup_status:
        host_status = all_setup_status[host_name]
        node = remote_trace_replay.remote_fs.get_node(host_name)
        machine_name = host_name.split("-")[0]

        if not all([not host_status[setup_step] for setup_step in host_status]):
            print("{}: Setting up node.".format(host_name))
            continue

        if remote_trace_replay.check_for_replay_process(node):
            print("{}: Replay already running!".format(host_name))
            continue
        
        if remote_trace_replay.check_for_replay_output(node):
            print("{}: Has completed replay. Handle output!".format(host_name))
            remote_trace_replay.handle_replay_complete(node)
            continue  
        
        for experiment_entry in experiment_list:
            if not remote_trace_replay.replay_db.has_replay_started(machine_name, experiment_entry):
                print("{}: Start experiment {}".format(host_name, experiment_entry))

                replay_rate = 1 if "replayRate" not in experiment_entry["kwargs"] else experiment_entry["kwargs"]["replayRate"]
                t1_size_mb = experiment_entry["t1_size_mb"]
                t2_size_mb = 0 if "nvmCacheSizeMB" not in experiment_entry["kwargs"] else experiment_entry["kwargs"]["nvmCacheSizeMB"]
                block_trace_path = remote_trace_replay.replay_db.get_full_block_trace_path_from_relative_path(experiment_entry["block_trace_path"])

                remote_trace_replay.run_trace_replay(
                    node, 
                    block_trace_path,
                    replay_rate,
                    t1_size_mb, 
                    t2_size_mb)
                
                break 
            else:
                print("Replay {} already started in machine {}".format(experiment_entry, machine_name))



    # remote_trace_replay.run(experiment_list)
    # remote_trace_replay.get_status(experiment_list)


if __name__ == "__main__":
    parser = ArgumentParser(description="Run block trace replay in remote nodes.") 

    parser.add_argument("--c", 
        default=Path(CONST_DICT["default_config_path"]), 
        type=Path, 
        help="Path to remote FS configuration. (Default: {})".format(CONST_DICT["default_config_path"]))
    
    parser.add_argument("--e",
        default=Path(CONST_DICT["default_experiment_file_path"]),
        type=Path,
        help="Path to file containing list of block trace replay to run. (Default: {})".format(CONST_DICT["default_experiment_file_path"]))
    
    parser.add_argument("--o",
        default=CONST_DICT["default_replay_output_dir"],
        type=Path,
        help="Path of directory containing output of block trace replay. (Default: {})".format(CONST_DICT["default_replay_output_dir"]))
    
    parser.add_argument("--r",
        default=False,
        type=bool,
        help="Boolean if set will lead to all experiments in the nodes listed to be killed.")

    args = parser.parse_args()

    main(args)