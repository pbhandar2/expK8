"""This class prepares remote nodes to run block trace replay by downloading necessary files 
and installing the necessary packages."""

from expK8.remoteFS.Node import Node, RemoteRuntimeError


DISK_FILE_SIZE_RATIO = 0.9
NVM_FILE_SIZE_RATIO = 0.95
MIN_NVM_FILE_SIZE_MB = 390 * 1024
MIN_DISK_FILE_SIZE_MB = 1000 * 1024
IO_FILE_NAME = "disk.file"
DISK_MOUNTPOINT, NVM_MOUNTPOINT = "~/disk", "~/nvm"


def is_replay_running(
    node: Node
) -> bool:
    running = False 
    ps_output = node.ps()
    for ps_row in ps_output:
        if "bin/cachebench" in ps_row:
            running = True 
            break 
    return running


def create_backing_file(
    node: Node, 
    force: bool
) -> int:
    """Create a file on backing storage device."""
    return create_file(
                node, 
                DISK_MOUNTPOINT, 
                "{}/{}".format(DISK_MOUNTPOINT, IO_FILE_NAME),
                DISK_FILE_SIZE_RATIO,
                MIN_DISK_FILE_SIZE_MB,
                force)


def create_nvm_file(
    node: Node, 
    force: bool
) -> int:
    """Create a file on NVM device."""
    return create_file(
                node, 
                NVM_MOUNTPOINT, 
                "{}/{}".format(NVM_MOUNTPOINT, IO_FILE_NAME),
                NVM_FILE_SIZE_RATIO,
                MIN_NVM_FILE_SIZE_MB,
                force)


def create_file(
    node: Node, 
    mountpoint: str,
    path: str,
    size_ratio: float,
    min_file_size_mb: int,
    force
) -> int:
    """Create a random file whose size is determined by the size of the mountpoint.

    Args:
        node: Node where file is to be created.
        mounpoint: The mountpoint where the file is to be created. 
        path: Path of the file on the mountpoint. 
        size_ratio: Ratio to derive size of file from size of mountpoint. 
        size_diff_tolerance_byte: If the size difference is lower than this value, 
            we consider the file ready. 
        force: Boolean indicating whether we should kill all creation process and start over. 

    Returns:
        exit_code: Exit code indicating the action taken when trying to create a new file. 
            - 0: File is ready and no new file was created. 
            - 1: File creation process running but the file is not ready yet. 
            - 2: New file creation process started. Either because we were forced or there 
                    were no processes creating a new file and the file requirements were 
                    also not satisfied. 
    """
    create_processes = get_create_file_processes(node)
    if len(create_processes) > 0 and not force:
        return 1 
    
    current_file_size_byte = node.get_file_size(path)
    if current_file_size_byte//(1024*1024) >= min_file_size_mb:
        return 0

    kill_create_file_process(node)
    if node.file_exists(path):
        node.rm(path)

    mount_info = node.get_mountpoint_info(mountpoint)
    if not mount_info:
        raise ValueError("Mountpoint {} not found".format(mountpoint))
    
    mount_size_byte = mount_info["size"]
    desired_file_size_byte = int(mount_size_byte * size_ratio)

    node.create_random_file_nonblock(path, desired_file_size_byte//(1024*1024))
    return 2 


def install_packages(node):
    multi_command_install = """sudo apt-get update 
    sudo apt install -y python3-pip libaio-dev 
    pip3 install psutil boto3 pandas numpy psutil
    sudo rm -rf ~/disk/CacheLib 
    git clone https://github.com/pbhandar2/CacheLib.git ~/disk/CacheLib
    git clone https://github.com/pbhandar2/phdthesis ~/disk/CacheLib/phdthesis
    git -C ~/disk/CacheLib/ checkout active 
    cd ~/disk/CacheLib; sudo ./contrib/build.sh -j -d 
    pip3 install ~/disk/CacheLib/phdthesis/cydonia --user
    touch /dev/shm/package.install.done"""

    for install_cmd in multi_command_install.split("\n"):
        stdout, stderr, exit_code = node.exec_command(install_cmd.strip().split(' '), timeout=600)
        if exit_code:
            return exit_code 
    return 0


def clone_cydonia(node: Node) -> int:
    """Clone the cydonia repo. 

    Args:
        node: Node where cydonia is cloned
    """
    clone_cmd = "git clone https://github.com/pbhandar2/phdthesis ~/disk/CacheLib/phdthesis"
    stdout, stderr, exit_code = node.exec_command(clone_cmd.split(' '))
    return exit_code


def install_cydonia(node: Node) -> int:
    """Install the cydonia repo. 

    Args:
        node: Node where cydonia is cloned
    """
    install_cmd = "cd ~/disk/CacheLib/phdthesis/cydonia; git pull origin main; pip3 install ~/disk/CacheLib/phdthesis/cydonia --user"
    stdout, stderr, exit_code = node.exec_command(install_cmd.split(' '))
    return exit_code


def install_cachelib(node: Node) -> int:
    """Install CacheLib package in the node. 

    Args:
        node: Node where CacheLib is to be installed. 
    
    Returns:
        exit_code: Exit code (0 for success, >0 for failure)
    """
    cachelib_dir = "{}/CacheLib".format(DISK_MOUNTPOINT)

    if node.dir_exists(cachelib_dir):
        change_cachelib_dir = "cd {};".format(cachelib_dir)
        cachebench_binary_path = "./opt/cachelib/bin/cachebench"
        config_file_path = "~/disk/CacheLib/cachelib/cachebench/test_configs/block_replay/sample_config.json"
        cachelib_cmd = "{} {} --json_test_config {}".format(
                        change_cachelib_dir,
                        cachebench_binary_path,
                        config_file_path)
        
        stdout, stderr, exit_code = node.exec_command(cachelib_cmd.split(' '))
        if exit_code:
            print("{}: reinstall CacheLib".format(node.host))
            print("{}: stdout=\n {}".format(node.host, stdout))
            print("{}: stderr=\n {}".format(node.host, stderr))
            node.rm(cachelib_dir)
        else:
            print("{}: Test passed sucessfully.".format(node.host))
            return 0 
            
    return install_packages(node)

            
def get_create_file_processes(node: Node) -> list:
    process_list = []
    ps_output = node.ps()
    """There are 2 processes that could be running to create file: a nohup process to prevent termination
        and the file creation process. Both contains substring "disk.file" and "dd" in it so we can 
        terminate process containing this substring in kill both processes."""
    for ps_row in ps_output.split("\n"):
        is_disk_file_in_ps_command = IO_FILE_NAME in ps_row 
        is_dd_in_ps_command = "dd" in ps_row 
        if is_disk_file_in_ps_command and is_dd_in_ps_command:
            process_list.append(ps_row)
    return process_list


def kill_create_file_process(node: Node) -> bool:
    """Kill any process related to creating a file.

    Args:
        node: Node where the process is to be killed. 
    
    Returns:
        killed: Boolean indicating if any processes were killed. 
    """
    killed = False  
    create_file_process_list = get_create_file_processes(node)
    for ps_row in create_file_process_list:
        pid = int(ps_row.strip().split(' ')[0])
        node.kill(pid)
        killed = True 
    return killed 