"""These tests check the ability to run block trace replay in remote nodes. 
"""


import json 
import unittest
from pathlib import Path
from expK8.controller.RemoteConnect import RemoteConnect
from expK8.experiment.BlockTraceReplay import BlockTraceReplay


test_config_file_path = Path("../data/test_RemoteConnect.json")
with test_config_file_path.open("r") as config_file_handle:
    test_config = json.load(config_file_handle)
remote_connect = RemoteConnect(test_config)


class TestBlockTraceReplay(unittest.TestCase):

    def test_basic(self):
        block_trace_replay = BlockTraceReplay("test_basic", remote_connect)


if __name__ == '__main__':
    unittest.main()