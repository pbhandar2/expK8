from pathlib import Path 
from ReplayDB import ReplayDB
from pandas import read_csv 


def handle_complete_experiments():
    machine_name = "c220g5"
    replay_meta_dir = Path("/research2/mtc/cp_traces/pranav/meta/replay/{}".format(machine_name))

    for workload_dir in replay_meta_dir.iterdir():
        for file_path in workload_dir.iterdir():
            df = read_csv(file_path)
            df = df.drop_duplicates()
            print(list(df.columns))
            print(df[["workload", "experiment", 'blockWriteLatency_avg_ns', 'blockReadLatency_avg_ns']])


if __name__ == "__main__":
    handle_complete_experiments()