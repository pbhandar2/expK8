from json import load 

from expK8.remoteFS.RemoteFS import RemoteFS
from expK8.remoteFS.Node import Node


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


def check_if_ps_row_is_cachebench(ps_row: str) -> bool:
    cachebench_substr = "bin/cachebench"
    return cachebench_substr in ps_row 


def check_if_ps_row_is_cydonia(ps_row: str) -> bool:
    cydonia_substr = "Replay.py"
    return cydonia_substr in ps_row 


def check_if_dd_backing_file(ps_row: str) -> bool:
    dd_file_substr = "disk/disk.file"
    return dd_file_substr in ps_row 


def check_if_dd_nvm_file(ps_row: str) -> bool:
    dd_file_substr = "nvm/disk.file"
    return dd_file_substr in ps_row 


def get_create_file_processes(node: Node, path_substr: str) -> list:
    process_list = []
    ps_output = node.ps()
    for ps_row in ps_output.split("\n"):
        if check_if_dd_backing_file(ps_row) or check_if_dd_nvm_file(ps_row):
            process_list.append(ps_row)
    return process_list


def get_replay_processes(node: Node) -> list:
    process_list = []
    ps_output = node.ps()
    for ps_row in ps_output.split("\n"):
        if check_if_ps_row_is_cachebench(ps_row) or check_if_ps_row_is_cydonia(ps_row):
            process_list.append(ps_row)
    return process_list


def kill_create_file_process(node: Node, path_substr: str) -> bool:
    killed = False  
    create_file_process_list = get_create_file_processes(node, path_substr)
    for ps_row in create_file_process_list:
        pid = int(ps_row.strip().split(' ')[0])
        node.kill(pid)
        killed = True 
    return killed 


def kill_replay_process(node: Node) -> bool:
    killed = False  
    process_list = get_replay_processes(node)
    for ps_row in process_list:
        pid = int(ps_row.strip().split(' ')[0])
        node.kill(pid)
        killed = True 
    return killed 


def kill_cachebench_test(node: Node) -> bool:
    killed = False 
    process_list = get_replay_processes(node)
    for ps_row in process_list:
        if "test_configs/block_replay" in ps_row:
            pd = int(ps_row.strip().split(' ')[0])
            node.kill(pid)
            killed = True 
    return killed 


def setup_backing_storage(node: Node, force: bool = False) -> bool:
    backing_store_mountpoint = "~/disk"
    mount_info = node.get_mountpoint_info(backing_store_mountpoint)
    create_file_process_list = get_create_file_processes(node, "disk/disk.file")

    if len(create_file_process_list) > 0:
        if force: 
            kill_create_file_process(node, "disk/disk.file")
        else:
            return 0

    assert mount_info, print("{}: No mountpoint info found for backing store.".format(node.host))
    backing_file_path = "{}/disk.file".format(backing_store_mountpoint)
    min_backing_file_size_mb = 1000 * 1024 
    current_backing_file_size_mb = node.get_file_size(backing_file_path)//(1024*1024)
    backing_file_size_mb = int((mount_info["size"]//(1024*1024)) * 0.95)

    if current_backing_file_size_mb >= min_backing_file_size_mb:
        return 1 
    else:
        kill_create_file_process(node, "disk/disk.file")
        node.create_random_file_nonblock(backing_file_path, backing_file_size_mb)
        return 2 


def setup_nvm_storage(node: Node, force: bool = False) -> bool:
    nvm_store_mountpoint = "~/nvm"
    mount_info = node.get_mountpoint_info(nvm_store_mountpoint)
    create_file_process_list = get_create_file_processes(node, "nvm/disk.file")

    if len(create_file_process_list) > 0:
        if force: 
            kill_create_file_process(node, "nvm/disk.file")
        else:
            return 0

    assert mount_info, print("{}: No mountpoint info found for backing store.".format(node.host))
    nvm_file_path = "{}/disk.file".format(nvm_store_mountpoint)
    min_nvm_file_size_mb = 390 * 1024 
    current_nvm_file_size_mb = node.get_file_size(nvm_file_path)//(1024*1024)
    nvm_file_size_mb = int((mount_info["size"]//(1024*1024)) * 0.975)

    if current_nvm_file_size_mb >= min_nvm_file_size_mb:
        return 1  
    else:
        kill_create_file_process(node, "nvm/disk.file")
        node.create_random_file_nonblock(nvm_file_path, nvm_file_size_mb)
        return 2 


def test_cachebench(node: Node):
    cachelib_dir = "~/disk/CacheLib"
    change_cachelib_dir = "cd {};".format(cachelib_dir)
    cachebench_binary_path = "./opt/cachelib/bin/cachebench"
    config_file_path = "~/disk/CacheLib/cachelib/cachebench/test_configs/block_replay/sample_config.json"
    cachelib_cmd = "{} {} --json_test_config {}".format(
                    change_cachelib_dir,
                    cachebench_binary_path,
                    config_file_path)

    stdout, stderr, exit_code = node.exec_command(cachelib_cmd.split(' '))
    if exit_code:
        return 0 
    else:
        return 1 



def clone_cydonia(node: Node) -> int:
    """Clone the cydonia repo. 

    Args:
        node: Node where cydonia is cloned
    """
    clone_cmd = "git clone https://github.com/pbhandar2/phdthesis ~/disk/CacheLib/phdthesis"
    stdout, stderr, exit_code = node.exec_command(clone_cmd.split(' '))
    return exit_code



def setup_cachelib(node: Node):
    install_base_package_cmd = "sudo apt-get update; sudo apt get install -y libaio-dev python3-pip"
    stdout, stderr, exit_code = node.exec_command(install_base_package_cmd.split(' '))
    if not exit_code: 
        return 0 
    
    cachelib_dir = "~/disk/CacheLib"
    if not node.dir_exists(cachelib_dir):
        clone_cachebench = "git clone https://github.com/pbhandar2/CacheLib.git ~/disk/CacheLib"
        checkout_cachebench = "git -C ~/disk/CacheLib/ checkout active"
        install_cachebench_cmd = "{};{}".format(clone_cachebench, checkout_cachebench)
        stdout, stderr, exit_code = node.exec_command(install_cachebench_cmd.split(' '))
        if not exit_code: 
            return 0 
    
    install_cachebench_cmd = "cd ~/disk/CacheLib; sudo ./contrib/build.sh -j -d"
    stdout, stderr, exit_code = node.exec_command(install_cachebench_cmd.split(' '))
    if not exit_code: 
        return 0 
    else:
        return 1 
        

def setup_cydonia(node: Node):
    cydonia_dir = "~/disk/CacheLib/phdthesis/cydonia"
    if not node.dir_exists(cydonia_dir):
        clone_cydonia(node)

    change_cydonia_dir = "cd {}; ".format(cydonia_dir)
    pull_cmd = "git pull origin main; "
    install_cmd = "pip3 install . --user"
    final_cmd = change_cydonia_dir + pull_cmd + install_cmd

    stdout, stderr, exit_code = node.exec_command(final_cmd.split(' '))
    return 0 if exit_code else 1 


def setup_permission(node: Node):
    disk_file_cmd = "sudo chmod -R g+rwx ~/disk/; "
    nvm_file_cmd = "sudo chmod -R g+rwx ~/nvm/; "
    power_chmod_cmd = "sudo chmod -R a+r /sys/class/powercap/intel-rapl"
    final_cmd = "{}{}{}".format(disk_file_cmd, nvm_file_cmd, power_chmod_cmd)
    stdout, stderr, exit_code = node.exec_command(final_cmd.split(' '))
    if exit_code:
        return 0 
    else:
        return 1 

def setup_node(node: Node):
    kill_cachebench_test(node)
    setup_status = {}
    setup_status['backing_storage_setup'] = setup_backing_storage(node)
    setup_status['nvm_storage_setup'] = setup_nvm_storage(node)
    setup_status['cachelib_install'] = test_cachebench(node)
    if setup_status['cachelib_install'] == 0:
        setup_cachelib(node)
        setup_status['cachelib_install'] = test_cachebench(node)
    setup_status['cydonia_install'] = setup_cydonia(node)
    setup_status['file_permissions'] = setup_permission(node)
    setup_status['cur_replay'] = 0 if is_replay_running(node) else 1
    return setup_status 


def setup_all_nodes():
    with open("config.json", "r") as config_file_handle:
        fs_config = load(config_file_handle)
    fs = RemoteFS(fs_config)

    for host_name in fs.get_all_live_host_names():
        node = fs.get_node(host_name)
        setup_status = setup_node(node)
        print(setup_status)
        kill_replay_process(node)


if __name__ == "__main__":
    setup_all_nodes()

