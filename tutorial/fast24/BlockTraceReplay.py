from json import load 
from pathlib import Path 
from argparse import ArgumentParser 
from logging import getLogger, Formatter, INFO
from logging.handlers import handlers

from expK8.remoteFS.RemoteFS import RemoteFS


# Setup logging 
logger = getLogger("block_replay")
logger.setLevel(INFO)
logHandler = handlers.RotatingFileHandler("/dev/shm/block_replay.csv", maxBytes=25*1e6)
logHandler.setLevel(INFO)
formatter = Formatter("%(asctime)s %(name)s %(levelname)s: %(message)s")
logHandler.setFormatter(formatter)
logger.addHandle(logHandler)


class BlockTraceReplay:
    def __init__(
        self,
        remote_fs: RemoteFS 
    ) -> None:
        self.remote_fs = remote_fs 
        

def main(args):
    with args.c.open("r") as config_file_handle:
        fs_config = load(config_file_handle)
    fs = RemoteFS(fs_config)

    replayer = BlockTraceReplay(fs)


if __name__ == "__main__":
    parser = ArgumentParser(description="Run block trace replay in remote nodes.")
    parser.add_argument("experiment_file", type=Path, help="Path to file containing list of block trace replays to run.")
    parser.add_argument("--c", default=Path("config.json"), type=Path, help="Path to remote FS configuration.")

    args = parser.parse_args()
    main(args)