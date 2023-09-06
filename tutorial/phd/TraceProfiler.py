"""This script generates block features files with JSON format from block storage traces."""

from json import dump 

from cydonia.profiler.CPReader import CPReader
from cydonia.profiler.BlockTraceProfiler import BlockTraceProfiler

from GlobalConfig import GlobalConfig


def profile_block_trace(
        block_trace_path: str, 
        output_path: str
) -> None:
    block_trace_profiler = BlockTraceProfiler(CPReader(block_trace_path))
    block_trace_profiler.run()
    
    kwargs = {'workload_name': block_trace_path.stem}
    stat = block_trace_profiler.get_stat(**kwargs)
    with output_path.open("w+") as write_handle:
        dump(stat, write_handle, indent=2)


def profile_fast24():
    config = GlobalConfig()
    for block_trace_path in config.get_fast24_block_trace_list():
        output_path = config.block_metadata_dir_path.joinpath(block_trace_path.parent.name, block_trace_path.name)
        output_path.parent.mkdir(exist_ok=True, parents=True)
        if not output_path.exists():
            print("{} does not exist! Profiling ....".format(output_path))
            profile_block_trace(block_trace_path, output_path)
            print("Completed.")
        else:
            print("{} already exists!".format(output_path))


if __name__ == "__main__":
    profile_fast24()