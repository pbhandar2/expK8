import unittest
import json 
from pathlib import Path 

from expK8.remoteFS.RemoteFS import RemoteFS


test_config_file_path = Path("../data/test_RemoteConnect.json")
with test_config_file_path.open("r") as config_file_handle:
    test_config = json.load(config_file_handle)
fs = RemoteFS(test_config)


class TestRemoteFS(unittest.TestCase):
    def test_connect(self):
        assert fs.all_up()


if __name__ == '__main__':
    unittest.main()