""" This script runs block trace replay on remote nodes specified in the configuration. 
"""

from json import load 
from pathlib import Path 
from argparse import ArgumentParser 
from logging import getLogger, Formatter, INFO, handlers, Logger

from expK8.remoteFS.RemoteFS import RemoteFS
from expK8.remoteFS.Node import Node


BACKING_FILE_DIR, NVM_FILE_DIR = "~/disk", "~/nvm"
IO_FILE_NAME = "disk.file"
OUTPUT_DIR = Path("/dev/shm/block_replay")
OUTPUT_DIR.mkdir(exist_ok=True, parents=True)
SETUP_LOG_PATH = OUTPUT_DIR.joinpath("setup_logger.csv")
BASE_LOG_PATH = OUTPUT_DIR.joinpath("base_logger.csv")
# if SETUP_LOG_PATH.exists(): SETUP_LOG_PATH.unlink()
# if BASE_LOG_PATH.exists(): BASE_LOG_PATH.unlink()


class BlockTraceReplay:
    def __init__(
        self,
        remote_fs: RemoteFS,
        force_setup: bool
    ) -> None:
        self.remote_fs = remote_fs 
        self.force_setup = force_setup

        self.base_logger = getLogger("self.base_logger")
        self.setup_status_logger = getLogger("setup_logger")
        self.setup_complete_file_path = "/dev/shm/replay.setup.done"
        self.package_install_complete_file_path = "/dev/shm/replay.package.done"

        self._setup_logger(self.base_logger, BASE_LOG_PATH)
        self._setup_logger(self.setup_status_logger, SETUP_LOG_PATH)


    def _setup_logger(
        self, 
        logger: Logger, 
        log_path: str 
    ) -> None:
        """Setup the loggers. 

        Args:
            logger: Logger to setup. 
            log_path: Path to log file. 
        """
        logger.setLevel(INFO)
        logHandler = handlers.RotatingFileHandler(log_path, maxBytes=25*1e6)
        logHandler.setLevel(INFO)
        formatter = Formatter("%(asctime)s %(name)s %(levelname)s: %(message)s")
        logHandler.setFormatter(formatter)
        logger.addHandler(logHandler)
    

    def get_file_size(
        self, 
        host_name: str, 
        path: str 
    ) -> int:
        """Get size of remote file. 

        Args:
            host_name: Host name of remote node. 
            path: Path in remote node. 
        
        Return:
            size: Size of file in bytes. 
        """
        size = 0 
        if self.remote_fs.file_exists(host_name, path):
            size = self.remote_fs.get_file_size(host_name, path)
        return size 
    

    def check_mount_setup(
        self,
        host_name: str, 
        chown_mountpoint: bool = False 
    ) -> bool:
        """Check if the mountpoints are setup in remote host. 

        Args:
            host_name: Host name of remote node. 
            chown_mountpoint: Flag indicating whether to change ownership of mountpoints to the user. 
        """
        mount_setup_flag = True 
        backing_dir_exists = self.remote_fs.dir_exists(host_name, BACKING_FILE_DIR)
        nvm_dir_exists = self.remote_fs.dir_exists(host_name, NVM_FILE_DIR)
        
        if backing_dir_exists and nvm_dir_exists:
            base_log_msg = "Mount setup correct."
        else:
            base_log_msg = "Backing {} and/or NVM {} mountpoint does not exist.".format(backing_dir_exists, nvm_dir_exists)
            mount_setup_flag = False 
        self.base_logger.info("{}: {}".format(host_name, base_log_msg))
        
        if chown_mountpoint:
            if backing_dir_exists:
                self.remote_fs.chown(host_name, "~/disk")
            if nvm_dir_exists:
                self.remote_fs.chown(host_name, "~/nvm")
        
        return mount_setup_flag
    

    def get_backing_file_size_mb(
        self, 
        node: Node
    ) -> int:
        """Get the desired size of remote file in backing store based on the size of mountpoint. 

        Args:
            node: Node object to communicate with remote node. 
        """
        return int(node.get_mountpoint_size_gb(BACKING_FILE_DIR) * 0.95) * 1024


    def get_nvm_file_size_mb(
        self, 
        node: Node
    ) -> int:
        """Get the desired size of remote file in NVM device based on the size of mountpoint. 

        Args:
            node: Node object to communicate with remote node. 
        """
        return int(node.get_mountpoint_size_gb(NVM_FILE_DIR) * 0.95) * 1024


    def get_backing_file_path(self) -> str:
        return "{}/{}".format(BACKING_FILE_DIR, IO_FILE_NAME)


    def get_nvm_file_path(self) -> str:
        return "{}/{}".format(NVM_FILE_DIR, IO_FILE_NAME)
    

    def create_io_files(
        self, 
        node: Node
    ) -> None:
        """Create file in storage devices needed for the experiments. 

        Args:
            node: The node object used to communicate with the remote node. 
        """
        host_name = node.host 
        backing_file_path, nvm_file_path = self.get_backing_file_path(), self.get_nvm_file_path()

        latest_backing_file_size_mb = node.get_file_size(backing_file_path)//(1024**2)
        self.base_logger.info("{}: Backing file of size {}MB".format(host_name, latest_backing_file_size_mb))
        if not node.get_file_size(backing_file_path):
            self.base_logger.info("{}: Creating a backing file.".format(host_name))
            node.create_random_file_nonblock(backing_file_path, self.get_backing_file_size_mb(node))
        else:
            if latest_backing_file_size_mb < self.get_backing_file_size_mb(node) and self.force_setup:
                self.base_logger.info("{}: Force setup recreating backing disk file of size {}MB".format(host_name, self.get_backing_file_size_mb(node)))
                node.exec_command(["sudo", "rm", "-rf", backing_file_path])
                node.create_random_file_nonblock(backing_file_path, self.get_backing_file_size_mb(node))
        
        latest_nvm_file_size_mb = node.get_file_size(backing_file_path)//(1024**2)
        self.base_logger.info("{}: NVM file of size {}MB".format(host_name, latest_nvm_file_size_mb))
        if not node.get_file_size(nvm_file_path):
            self.base_logger.info("{}: Creating a NVM file.".format(host_name))
            node.create_random_file_nonblock(nvm_file_path, self.get_nvm_file_size_mb(node))
        else:
            if latest_nvm_file_size_mb < self.get_nvm_file_size_mb(node) and self.force_setup:
                self.base_logger.info("{}: Force setup recreating NVM disk file of size {}MB".format(host_name, self.get_nvm_file_size_mb(node)))
                node.rm(nvm_file_path)
                node.create_random_file_nonblock(nvm_file_path, self.get_nvm_file_size_mb(node))


    def install_packages(
        self, 
        host_name: str 
    ) -> None:
        """Install packges required to run the experiment. 

        Args:
            host_name: Host name of the remote node where packages are installed. 
        """
        if self.remote_fs.get_node(host_name).file_exists(self.package_install_complete_file_path):
            self.base_logger.info("{}: Install previously completed.".format(host_name))
            return 

        multi_command_install = """sudo apt-get update 
        sudo apt install -y python3-pip libaio-dev 
        pip3 install psutil boto3 pandas numpy psutil
        sudo rm -rf ~/disk/CacheLib 
        git clone https://github.com/pbhandar2/CacheLib.git ~/disk/CacheLib
        git clone https://github.com/pbhandar2/phdthesis ~/disk/CacheLib/phdthesis
        git -C ~/disk/CacheLib/ checkout active 
        cd ~/disk/CacheLib; sudo ./contrib/build.sh -j -d 
        pip3 install ~/disk/CacheLib/phdthesis/cydonia --user
        touch {}""".format(self.package_install_complete_file_path)

        for install_cmd in multi_command_install.split("\n"):
            self.base_logger.info("{}: Install cmd {}".format(host_name, install_cmd.strip()))
            stdout, stderr, exit_code = self.remote_fs.get_node(host_name).exec_command(install_cmd.strip().split(' '), timeout=600)
            if exit_code:
                self.base_logger.info("{}: Install failed".format(host_name))
                self.base_logger.info("{}: Install stdout {}".format(host_name, stdout))
                self.base_logger.info("{}: Install stderr {}".format(host_name, stderr))
                self.base_logger.info("{}: Install exit code {}".format(host_name, exit_code))
                self.setup_status_logger.error("{}:cmd={},exit_code={}".format(host_name, install_cmd.strip(), exit_code))

        
    def setup(self) -> None:
        """Setup nodes to be ready for experiments."""
        host_name_arr = self.remote_fs.get_all_host_names()
        for host_name in host_name_arr:
            self.setup_status_logger.error("{}: Init".format(host_name))
            remote_node = self.remote_fs.get_node(host_name)

            if remote_node.file_exists(self.setup_complete_file_path):
                complete_file_found_msg = "{}: Setup completion file {} exists.".format(host_name, self.setup_complete_file_path)
                self.base_logger.info(complete_file_found_msg)
                self.setup_status_logger.info(complete_file_found_msg)
                continue 
                
            if not self.check_mount_setup(host_name, chown_mountpoint=True):
                self.base_logger.info("{}: Setup failed. Mountpoints not setup.".format(host_name))
                self.setup_status_logger.error("{}: Mountpoints not setup".format(host_name))
                continue 
            
            self.create_io_files(remote_node)
            self.install_packages(host_name)

            backing_file_path = "{}/{}".format(BACKING_FILE_DIR, IO_FILE_NAME)
            nvm_file_path = "{}/{}".format(NVM_FILE_DIR, IO_FILE_NAME)
            backing_file_size_mb = self.get_backing_file_size_mb(remote_node)
            nvm_file_size_mb = self.get_nvm_file_size_mb(remote_node)
            if remote_node.get_file_size(nvm_file_path)//(1024**2) >= nvm_file_size_mb and \
                remote_node.get_file_size(backing_file_path)//(1024*2) >= backing_file_size_mb:
                node.touch(self.setup_complete_file_path)
                self.base_logger.info("{}: Ready to run experiments.".format(host_name))
            else:
                self.base_logger.info("{}: Waiting for file creation to complete. Backing: {}/{}, NVM: {}/{}".format(
                    host_name,
                    backing_file_size_mb,
                    remote_node.get_file_size(backing_file_path)//(1024**2),
                    nvm_file_size_mb, 
                    remote_node.get_file_size(nvm_file_path)//(1024**2)))
            
            setup_log = "{},{}".format(backing_file_size_mb, nvm_file_size_mb)
            self.setup_status_logger.info("{}:{}".format(host_name, setup_log))
    
    
    def run(self) -> None:
        host_name_arr = self.remote_fs.get_all_host_names()
        for host_name in host_name_arr:
            remote_node = self.remote_fs.get_node(host_name)

            if not remote_node.file_exists(self.setup_complete_file_path):
                self.base_logger.info("{}: Setup has not completed.".format(host_name))
                continue 
            
            exit_code, stdout, stderr = remote_node.exec_command("ps aux | grep RunExperiment.py | grep -v grep".split(' '))
            if stdout: 
                self.base_logger.info("{}: Experiment already running. {}".format(stdout, stdout))
                continue 
            
            self.base_logger.info("{}: Running experiment.".format(host_name))
            run_cmd = ["nohup",
                        "python3", 
                        "~/disk/CacheLib/phdthesis/scripts/fast24/RunExperiment.py", 
                        "--experiment_file", 
                        "~/disk/CacheLib/phdthesis/scripts/fast24/experiments/files/sample/cp-test_iat_w66.json"]
            remote_node.exec_command(run_cmd)
            

def main(args):
    with args.c.open("r") as config_file_handle:
        fs_config = load(config_file_handle)
    fs = RemoteFS(fs_config)

    replayer = BlockTraceReplay(fs, args.f)
    replayer.setup()


if __name__ == "__main__":
    parser = ArgumentParser(description="Run block trace replay in remote nodes.")
    parser.add_argument("experiment_file", type=Path, help="Path to file containing list of block trace replays to run.")
    parser.add_argument("--c", default=Path("config.json"), type=Path, help="Path to remote FS configuration.")
    parser.add_argument('--f', default=False, type=bool, help="Force setup to recreate IO file.")

    args = parser.parse_args()
    main(args)