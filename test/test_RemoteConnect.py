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


if __name__ == '__main__':
    unittest.main()