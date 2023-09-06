from pathlib import Path 


class ReplayConfig:
    def __init__(
            self, 
            block_trace_path: str,
            t1_size_mb: int,
            t2_size_mb: int,
            num_block_threads: int = 16,
            num_async_threads: int = 16,
            max_pending_block_requests: int = 128,
            replay_output_dir_path: str = "/research2/mtc/cp_traces/pranav/replay"
    ) -> None:
        self.block_trace_path = Path(block_trace_path)
        self.t1_size_mb = t1_size_mb
        self.t2_size_mb = t2_size_mb 
        self.num_block_threads = num_block_threads
        self.num_block_threads = num_async_threads
        self.max_pending_block_requests = max_pending_block_requests
        self.replay_output_dir_path = Path(replay_output_dir_path)ß