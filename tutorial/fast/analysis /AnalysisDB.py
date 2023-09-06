from sys import path
path.append("..")

from pathlib import Path 
from collections import defaultdict
from pandas import read_csv, DataFrame, Series
import matplotlib.pyplot as plt

from Config import Config 


class AnalysisDB:
    def __init__(self) -> None:
        self.config = Config()
        self.complete_replay_file_path = self.config.replay_metadata_dir_path.joinpath("done.csv")
        self.complete_sample_replay_file_path = self.config.replay_metadata_dir_path.joinpath("sample_done.csv")

        if self.complete_replay_file_path.exists():
            self.complete_df = read_csv(self.complete_replay_file_path)
        else:
            self.complete_df = DataFrame([])

        if self.complete_sample_replay_file_path.exists():
            self.complete_sample_df = read_csv(self.complete_sample_replay_file_path)
        else:
            self.complete_sample_df = DataFrame([])
    

    def get_sample_row_arr(self, full_row: Series) -> list:
        sample_rows = self.complete_sample_df[
                            (self.complete_sample_df['q']==full_row['q']) & \
                            (self.complete_sample_df['bt']==full_row['bt']) & \
                            (self.complete_sample_df['at']==full_row['at']) & \
                            (self.complete_sample_df['rr']==full_row['rr']) & \
                            (self.complete_sample_df['it']==full_row['it']) & \
                            (self.complete_sample_df['name']==full_row['name']) & \
                            (self.complete_sample_df['type']==full_row['type'])]
        
        t1_size_mb = full_row['t1']
        t2_size_mb = full_row['t2']
        sample_row_arr = []
        for _, sample_row in sample_rows.iterrows():
            rate  = sample_row['rate']
            scaled_t1_size_mb = int(t1_size_mb * rate/100.0)
            scaled_t2_size_mb = int(t2_size_mb * rate/100.0)

            if scaled_t1_size_mb == sample_row['t1'] and scaled_t2_size_mb == sample_row['t2']:
                sample_row_arr.append(sample_row)
        return sample_row_arr
    

    def get_t2_hr(self, row) -> float:
        t1_get_miss = row["numCacheGetMiss"]
        t1_get = row["numCacheGets"]
        read_hit_count = row["readCacheHitCount"] 
        read_req_count = row["readCacheReqCount"]
        t2_get = row["numNvmGets"]
        total_get_miss = (read_hit_count/read_req_count) * (t1_get + t2_get)
        t2_get_miss = total_get_miss - t1_get_miss 
        return 100*(t2_get - t2_get_miss)/t2_get if t2_get > 0 else 0 
    

    def get_lat_err(self, full_row, sample_row):
        read_err_map = {}
        write_err_map = {}
        for column_name in full_row.index.to_list():
            print(column_name)
            if "blockReadLatency" in column_name:
                split_column_name = column_name.split("_")
                percentile_val = split_column_name[1]
                read_err_map[percentile_val] = 100*(full_row[column_name] - sample_row[column_name])/full_row[column_name]

            if "blockWriteLatency" in column_name:
                split_column_name = column_name.split("_")
                percentile_val = split_column_name[1]
                write_err_map[percentile_val] = 100*(full_row[column_name] - sample_row[column_name])/full_row[column_name]

        return read_err_map, write_err_map


    def get_sample_replay_sets(self) -> list:
        output_arr = []
        for _, replay_row in self.complete_df.iterrows():
            sample_row_arr = self.get_sample_row_arr(replay_row)
            for sample_row in sample_row_arr:
                full_hr, sample_hr = replay_row["overallByteHitRate"], sample_row["overallByteHitRate"]
                hr_err = 100.0*(full_hr - sample_hr)/full_hr

                output_dict = {}
                output_dict['type'] = replay_row['type']
                output_dict['name'] = replay_row['name']
                output_dict['t1'] = replay_row['t1']
                output_dict['sample_t1'] = sample_row['t1']
                output_dict['t2'] = replay_row['t2']
                output_dict['sample_t2'] = sample_row['t2']
                output_dict['rate'] = sample_row['rate']
                output_dict['bits'] = sample_row['bits']
                output_dict['hr'] = replay_row["overallByteHitRate"]
                output_dict['sample_hr'] = sample_row["overallByteHitRate"]
                output_dict['hr_err'] = hr_err

                read_err_dict, write_err_dict = self.get_lat_err(replay_row, sample_row)

                for read_err_percentile in read_err_dict:
                    output_dict["r_{}".format(read_err_percentile)] = read_err_dict[read_err_percentile]

                for write_err_percentile in write_err_dict:
                    output_dict["w_{}".format(write_err_percentile)] = write_err_dict[write_err_percentile]

                output_arr.append(output_dict)
        
        return DataFrame(output_arr)
    

    def plot_err_bar(self, x, y, output_path):
        plt.rcParams.update({'font.size': 22})
        fig, ax = plt.subplots(figsize=[14,10])
        # print(x)
        # print(y)
        ax.bar([str(_) for _ in x], y, width=1.0)
        # for i, v in enumerate(y):
        #     ax.text(v + 3, i + .25, "{:.1f}".format(v),
        #             color = 'blue', fontweight = 'bold', fontsize=15)
        ax.set_xlabel("Percentiles")
        ax.set_ylabel("Percent Error")
        ax.tick_params(axis='x', labelrotation = 45)
        plt.savefig(output_path)
        plt.close()


    def plot_err_plots(self, df):
        plot_dir = Path("plots/err")
        plot_dir.mkdir(exist_ok=True, parents=True)

        read_col_name_arr, write_col_name_arr = [], []
        for column_name in df.columns:
            if "r_" in column_name[:2]:
                if "avg" in column_name:
                    continue 
                percentile_str = column_name.split("_")[1][1:]
                percentile = int(column_name.split("_")[1][1:])
                if percentile > 100:
                    percentile_str = str(percentile)[:2] + "." + str(percentile)[2:]
                    percentile = float(percentile_str)
                read_col_name_arr.append([percentile, column_name])
            
            if "w_" in column_name[:2]:
                if "avg" in column_name:
                    continue 
                percentile_str = column_name.split("_")[1][1:]
                percentile = int(column_name.split("_")[1][1:])
                if percentile > 100:
                    percentile_str = str(percentile)[:2] + "." + str(percentile)[2:]
                    percentile = float(percentile_str)
                write_col_name_arr.append([percentile, column_name])
        
        read_col_name_arr = sorted(read_col_name_arr, key=lambda k: k[0])
        write_col_name_arr = sorted(write_col_name_arr, key=lambda k: k[0])
        for row_index, row in df.iterrows():
            read_x = []
            read_err_arr = []
            for percentile_val, percentile_metric_name in read_col_name_arr:
                read_x.append(percentile_val)
                read_err_arr.append(row[percentile_metric_name])

            write_x = []
            write_err_arr = []
            for percentile_val, percentile_metric_name in write_col_name_arr:
                write_x.append(percentile_val)
                write_err_arr.append(row[percentile_metric_name])
            
            r_file_name = "r-{}-{}-{}-{}-{}-{}.png".format(row['type'], row['name'], row['t1'], row['t2'], row['rate'], row['bits'])
            w_file_name = "w-{}-{}-{}-{}-{}-{}.png".format(row['type'], row['name'], row['t1'], row['t2'], row['rate'], row['bits'])
            
            self.plot_err_bar(read_x, read_err_arr, plot_dir.joinpath(r_file_name))
            self.plot_err_bar(write_x, write_err_arr, plot_dir.joinpath(w_file_name))


if __name__ == "__main__":
    analysis_db = AnalysisDB()

    sample_data_db = analysis_db.get_sample_replay_sets()
    print(sample_data_db.to_string())
    sample_data_db.to_csv("current_sample_replay.csv", index=False)
    analysis_db.plot_err_plots(sample_data_db)
        
