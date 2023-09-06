from expK8.remoteFS.Node import Node, RemoteRuntimeError


CACHELIB_TEST_CONFIG_FILE_PATH = "~/disk/CacheLib/cachelib/cachebench/test_configs/block_replay/sample_config.json"


def is_replay_running(node: Node) -> bool:
    """Check if replay is running in a node.
    
    Args:
        node: Node where experiment might be running. 
    
    Returns:
        running: Boolean indicating if trace replay is already running in the node. 
    """
    running = False 
    ps_output = node.ps()
    for ps_row in ps_output.split('\n'):
        if "bin/cachebench" in ps_row:
            running = True 
            break 
    return running


def is_replay_test_running(node: Node) -> bool:
    """Check if replay test is running in a node.
    
    Args:
        node: Node where experiment might be running. 
    
    Returns:
        running: Boolean indicating if trace replay test is already running in the node. 
    """
    running = False 
    ps_output = node.ps()
    for ps_row in ps_output.split('\n'):
        if "bin/cachebench" in ps_row and CACHELIB_TEST_CONFIG_FILE_PATH in ps_row:
            running = True 
            break 
    return running


def check_storage_file(
    node: Node,
    mountpoint: str,
    path_relative_to_mountpoint: str,
    storage_file_size_mb: int 
) -> int:
    """Check if a required storage file is correctly setup and create one otherwise.

    Args:
        node: Node where file is checked. 
        mountpoint: Mountpoint where file is created. 
        path_relative_to_mountpoint: Path relative to mountpoint of the storage file. 
        storage_file_size_mb: Minimum size of file in MB

    Returns:
        status: Status of storage files represented by values: -1 (creation is running), 0 (no valid mount found),
                    1 (done), 2(started creation)
    """
    mount_info = node.get_mountpoint_info(mountpoint)
    if not mount_info:
        print(node.get_block_devices())
        print("No valid mount found!")
        return 0 
    
    create_file_ps_row = None
    ps_output = node.ps()
    for ps_row in ps_output.split('\n'):
        if "dd" not in ps_row:
            continue 
        
        if node.format_path(mountpoint) not in ps_row:
            continue 
        
        if path_relative_to_mountpoint not in ps_row:
            continue 
        
        create_file_ps_row = ps_row 
        break 
    
    create_file_live = create_file_ps_row is not None 
    if create_file_live:
        return -1 
    else:
        file_path = "{}/{}".format(mountpoint, path_relative_to_mountpoint)
        current_file_size_byte = node.get_file_size("{}/{}".format(mountpoint, path_relative_to_mountpoint))
        if current_file_size_byte//(1024*1024) < storage_file_size_mb:
            print("current file size {} and min file size {}".format(current_file_size_byte//(1024*1024), storage_file_size_mb))
            node.create_random_file_nonblock(file_path, storage_file_size_mb)
            return 2
        else:
            return 1


def clone_cydonia(
    node: Node,
    dir_path: str = "~/disk/CacheLib/phdthesis"
) -> None:
    """Clone the cydonia repo. 

    Args:
        node: Node where cydonia repository should be cloned. 
        dir_path: Path of directory in remote node where cydonia is cloned. 
    """
    clone_cmd = "git clone https://github.com/pbhandar2/phdthesis {}".format(dir_path)
    stdout, stderr, exit_code = node.exec_command(clone_cmd.split(' '))
    if exit_code:
        raise RemoteRuntimeError(clone_cmd, node.host, stdout, stderr, exit_code)


def setup_cydonia(node: Node) -> int:
    repo_dir = "~/disk/CacheLib/phdthesis"
    cydonia_dir = "{}/cydonia".format(repo_dir)
    if not node.dir_exists(cydonia_dir):
        node.rm(repo_dir)
        clone_cydonia(node)

    change_cydonia_dir = "cd {}; ".format(cydonia_dir)
    pull_cmd = "git pull origin main; "
    install_cmd = "pip3 install . --user"
    final_cmd = change_cydonia_dir + pull_cmd + install_cmd

    _, _, exit_code = node.exec_command(final_cmd.split(' '))
    return 0 if exit_code else 1 


def install_cachebench(node: Node):
    install_linux_packages_cmd = "sudo apt-get update; sudo apt install -y libaio-dev python3-pip"
    stdout, stderr, exit_code = node.exec_command(install_linux_packages_cmd.split(' '))
    if exit_code:
        raise RemoteRuntimeError(install_linux_packages_cmd, node.host, stdout, stderr, exit_code)
    
    cachelib_dir = "~/disk/CacheLib"
    if not node.dir_exists(cachelib_dir):
        clone_cachebench = "git clone https://github.com/pbhandar2/CacheLib.git ~/disk/CacheLib"
        checkout_cachebench = "git -C ~/disk/CacheLib/ checkout active"
        install_cachebench_cmd = "{};{}".format(clone_cachebench, checkout_cachebench)
        stdout, stderr, exit_code = node.exec_command(install_cachebench_cmd.split(' '))
        if exit_code:
            raise RemoteRuntimeError(install_cachebench_cmd, node.host, stdout, stderr, exit_code)
    
    install_cachebench_cmd = "cd ~/disk/CacheLib; git -C ~/disk/CacheLib/ checkout active; sudo ./contrib/build.sh -j -d"
    stdout, stderr, exit_code = node.exec_command(install_cachebench_cmd.split(' '))
    if exit_code:
        raise RemoteRuntimeError(install_cachebench_cmd, node.host, stdout, stderr, exit_code)


def test_cachebench(node: Node):
    cachelib_dir = "~/disk/CacheLib"
    change_cachelib_dir = "cd {};".format(cachelib_dir)
    cachebench_binary_path = "./opt/cachelib/bin/cachebench"
    cachelib_cmd = "{} {} --json_test_config {}".format(
                    change_cachelib_dir,
                    cachebench_binary_path,
                    CACHELIB_TEST_CONFIG_FILE_PATH)

    stdout, stderr, exit_code = node.exec_command(cachelib_cmd.split(' '))
    if exit_code:
        # print("CB TEST FAILED: STDOUT: {} \n STDERR: {} \n".format(stdout, stderr))
        return 0 
    else:
        return 1 