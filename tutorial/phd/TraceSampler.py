"""This script generates samples from block traces."""

from pathlib import Path 
from pandas import read_csv, DataFrame
from numpy import mean, zeros, percentile, ceil 

from cydonia.sample.Sampler import Sampler

from GlobalConfig import GlobalConfig
from ExperimentFactory import ExperimentFactory


def write_sample_features(
    features: dict, 
    file_path: str
) -> None:
    """Write features of a sample to a file. 
    
    Args:
        features: The dictionary containing features of the sample. 
        file_path: Path of file where features are written. 
    """
    df = DataFrame([features])
    if file_path.exists():
        df.to_csv(file_path, mode='a', index=False, header=False)
    else:
        df.to_csv(file_path, index=False)


def get_stats_from_split_counter(
    split_counter: dict, 
    sample_rate: float, 
    seed: int, 
    bits: int, 
    sample_file_path: str, 
    percentiles_arr: list = [0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 99, 99.9, 100]
) -> None:
    """Get statistics (mean, min, max, percentiles) from the counter of number of samples 
    generated from a sampled block request. 

    Args:
        split_counter : Counter of the number of samples generated per sampled block request. 

    Returns:
        stats: Split statistics such as (mean, min, max, percentiles) from the split_counter. 
    """
    total_request_sampled = sum(split_counter.values())
    stats = {}
    if total_request_sampled > 0:
        total_items = sum(split_counter.values())
        split_array = zeros(total_items, dtype=int)

        cur_index = 0 
        for key in split_counter:
            split_array[cur_index:cur_index+split_counter[key]] = key
            cur_index += split_counter[key]

        stats['mean'] = mean(split_array) 
        stats['total'] = len(split_array)

        for _, percentile_val in enumerate(percentiles_arr):
            stats['p_{}'.format(percentile_val)] = percentile(split_array, percentile_val, keepdims=False)
        
        no_split_count = len(split_array[split_array == 1])
        stats['freq%'] = int(ceil(100*(stats['total'] - no_split_count)/stats['total']))
        stats['rate'] = sample_rate 
        stats['seed'] = seed 
        stats['bits'] = bits 
        sample_df = read_csv(sample_file_path, names=["ts", "lba", "op", "size"])
        stats['unique_lba_count'] = int(sample_df['lba'].nunique())
    else:
        stats = {
            'mean': 0,
            'total': 0,
            'freq%': 0,
            'rate': sample_rate,
            'seed': seed,
            'bits': bits,
            'unique_lba_count': 0
        }
        for _, percentile_val in enumerate(percentiles_arr):
            stats['p_{}'.format(percentile_val)] = 0
    return stats 


def sample_fast24():
    """Generate samples for FAST24."""
    factory = ExperimentFactory()
    for sample_params in factory.get_missing_samples():
        original_trace_path = factory.config.trace_path.joinpath(sample_params["workload_type"], "{}.csv".format(sample_params["workload_name"]))
        sample_trace_path = Path(sample_params["path"])
        sample_trace_path.parent.mkdir(exist_ok=True, parents=True)
        
        print("Sampling {} with params {}".format(original_trace_path, sample_params))
        sampler = Sampler(original_trace_path)
        split_counter = sampler.sample(
                            sample_params["rate"], 
                            sample_params["seed"], 
                            sample_params["bits"], 
                            sample_params["type"],
                            sample_trace_path)
        
        print("Sample generated: {}".format(sample_trace_path))
        split_stats = get_stats_from_split_counter(split_counter, 
                        sample_params["rate"], 
                        sample_params["seed"], 
                        sample_params["bits"], 
                        Path(sample_params["path"]))

        split_stat_file_path = factory.config.sample_metadata_dir_path.joinpath(
                                sample_params["type"], 
                                sample_params["workload_type"], 
                                "{}.csv".format(sample_params["workload_name"]))
        split_stat_file_path.parent.mkdir(exist_ok=True, parents=True)
        write_sample_features(split_stats, split_stat_file_path)
        print("Stat {} updated: {}".format(split_stats, split_stat_file_path))


if __name__ == "__main__":
    sample_fast24()