"""This test of RemoteConnect class uses the file "test_RemoteConnect.json" as the configuration 
file. Update the file to include a remote node that you have access to along with the credentials 
required to connect to it. 
"""

import unittest
import json 
from pathlib import Path
from expK8.controller.RemoteConnect import RemoteConnect


test_config_file_path = Path("../data/test_RemoteConnect.json")
with test_config_file_path.open("r") as config_file_handle:
    test_config = json.load(config_file_handle)
remote_connect = RemoteConnect(test_config)


class TestRemoteConnect(unittest.TestCase):

    def test_connect(self):
        """ Test that the nodes listed in the test config are all connected. """
        node_status = remote_connect.get_node_status()
        assert node_status and not remote_connect._unresponsive
        for hostname in node_status:
            assert node_status[hostname]
    

    def test_exec_command(self):
        """ Test if we are able to run commands in remote nodes. """
        stdout, stderr, exit_code = remote_connect.exec_command(test_config["nodes"]["compute0"]["host"], ["ls", "-lh"])
        assert not len(stderr) and len(stdout) and not exit_code


    def test_transfer_data(self):
        """ Test if we can transfer data between connected nodes. """
        print("Testing data transfer.")
        source_node, target_node = "data0", "compute0"
        source_host_name = test_config["nodes"][source_node]["host"]
        target_host_name = test_config["nodes"][target_node]["host"]
        source_data_file_path = "/tmp/expK8.testfile"
        source_data_file_content = "sourcecontent"
        
        if remote_connect.remote_path_exists(source_host_name, source_data_file_path):
            remote_connect.rm(source_host_name, source_data_file_path)

        source_data_generation_command = ["echo", source_data_file_content, ">>", source_data_file_path]
        stdout, stderr, exit_code = remote_connect.exec_command(source_host_name, source_data_generation_command)
        assert not exit_code
        print("Created file {} in {}".format(source_data_file_path, source_host_name))

        # read the data file in the source node to validate file creation 
        read_source_data_return = remote_connect.cat(source_host_name, source_data_file_path)
        assert str(read_source_data_return) == source_data_file_content, \
            "Source data: {} did not match data returned {}".format(source_data_file_content, read_source_data_return)
        print("Transfering {}:{} to {}:{}.".format(source_host_name,
                                                source_data_file_path,
                                                target_host_name,
                                                source_data_file_path))
        
        remote_connect.scp(source_host_name, source_data_file_path, target_host_name, source_data_file_path, timeout=30)
        print("Data transfer completed.")
        remote_connect.rm(target_host_name, source_data_file_path)
        remote_connect.rm(source_host_name, source_data_file_path)
        print("Deleted {}:{} and {}:{}.".format(source_host_name, 
                                                source_data_file_path,
                                                target_host_name,
                                                source_data_file_path))


if __name__ == '__main__':
    unittest.main()