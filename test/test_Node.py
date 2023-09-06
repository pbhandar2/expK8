import unittest
import json 
from pathlib import Path 

from expK8.remoteFS.NodeFactory import NodeFactory

node_factory = NodeFactory("../data/test_RemoteConnect.json")


class TestNode(unittest.TestCase):
    def test_connect(self):
        local_data_dir_str = "/research2/"
        remote_data_dir_str = "/remote/data/dir"
        remote_data_path = Path("{}/dir/data.file".format(remote_data_dir_str))
        remote_data_dir_path = Path(remote_data_dir_str)
        local_data_dir_path = Path(local_data_dir_str)
        node_list = node_factory.get_node_list()
        temp_node = node_list[0]
        local_path = temp_node.local_path_map(remote_data_path, remote_data_dir_path, local_data_dir_path=Path(local_data_dir_str))
        print(local_path)
        assert local_path == Path("{}/dir/data.file".format(local_data_dir_str))

        file_list = temp_node.find_all_files_in_dir("~/nvm/mtc/cp_traces/pranav")
        for remote_file_path in file_list:
            local_path = temp_node.local_path_map(
                            Path(remote_file_path), 
                            Path(temp_node.format_path("~/nvm/mtc/cp_traces/pranav")))
            print("{} -> {}".format(remote_file_path, local_path))


if __name__ == '__main__':
    unittest.main()