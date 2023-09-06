from json import load, dumps 

from SetupNode import setup_node
from ReplayDB import ReplayDB
from expK8.remoteFS.Node import Node, RemoteRuntimeError


TEST_CONFIG_PATH = "cachelib/cachebench/test_configs/block_replay/sample_config.json"

def is_replay_running(
    node: Node
) -> bool:
    running = False 
    ps_output = node.ps()
    for ps_row in ps_output.split('\n'):
        if "bin/cachebench" in ps_row:
            running = True 
            break 
    return running


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
    
    install_cachebench_cmd = "cd ~/disk/CacheLib; sudo ./contrib/build.sh -j -d"
    stdout, stderr, exit_code = node.exec_command(install_cachebench_cmd.split(' '))
    if exit_code:
        raise RemoteRuntimeError(install_cachebench_cmd, node.host, stdout, stderr, exit_code)


def kill_test_cachebench(node: Node):
    node.match_kill(TEST_CONFIG_PATH)


def test_cachebench(node: Node):
    kill_test_cachebench(node)
    cachelib_dir = "~/disk/CacheLib"
    change_cachelib_dir = "cd {};".format(cachelib_dir)
    cachebench_binary_path = "./opt/cachelib/bin/cachebench"
    config_file_path = "~/disk/CacheLib/{}".format(TEST_CONFIG_PATH)
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

    stdout, stderr, exit_code = node.exec_command(final_cmd.split(' '))
    return 0 if exit_code else 1 


def setup_packages(config_file_path: str) -> dict:

    with open(config_file_path, "r") as config_handle:
        config_dict = load(config_handle)
        creds = config_dict["creds"]
        mounts = config_dict["mounts"]
        nodes = config_dict["nodes"]
    
    setup_status = {}
    for node_name in nodes:
        node_info = nodes[node_name]
        node = Node(node_info["host"], 
                        node_info["host"], 
                        creds[node_info["cred"]], 
                        mounts[node_info["mount"]])


        if is_replay_running(node):
            setup_status[node.host] = {
                "cachebench": 1,
                "pyCydonia": 1
            }
            continue 


        cachebench_test_status = test_cachebench(node)
        if not cachebench_test_status:
            print("reinstalling cachelib")
            install_cachebench(node)
            cachebench_test_status = test_cachebench(node)
        
        setup_status[node.host] = {
            "cachebench": cachebench_test_status,
            "pyCydonia": setup_cydonia(node)
        }
        print(dumps(setup_status, indent=2))
    
    print(dumps(setup_status, indent=2))


if __name__ == "__main__":
    setup_packages("config.json")