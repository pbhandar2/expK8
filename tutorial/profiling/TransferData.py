from argparse import ArgumentParser
from expK8.remoteFS.NodeFactory import NodeFactory

if __name__ == "__main__":
    parser = ArgumentParser("Transfer data from remote nodes.")
    parser.add_argument("host_name", help="Host name of node.")
    parser.add_argument("--config_file_path", default="config.json", help="Configuration file path.")
    args = parser.parse_args()

    node_factory = NodeFactory(args.config_file_path)
    remote_dir_path = "/users/vishwa/nvm/mtc/cp_traces/pranav/"
    local_dir_path = "/research2/mtc/cp_traces/pranav/"
    for node in node_factory.get_node_list():
        if node.host == args.host_name:
            node.sync_dir(remote_dir_path, local_dir_path)
            break 
    else:
        print("Did not find host with name {}".format(args.host_name))



    



