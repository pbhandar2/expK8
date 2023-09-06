from pathlib import Path 
from ReplayConfig import ReplayConfig


class BlockTraceReplay:
    """The class represents an instance of block trace replay. 
    """
    def __init__(self) -> None:
        self.replay_config = ReplayConfig()
        self.started = False 
        self.completed = False 
