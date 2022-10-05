import itertools
from multiprocessing.sharedctypes import Value
import pathlib 
from tabulate import tabulate
from scipy import stats
import numpy as np 
import pandas as pd 

import matplotlib.pyplot as plt
plt.rcParams.update({'font.size': 25})

DATA_DIR = pathlib.Path.home().joinpath("mtdata")
OUTPUT_DIR = pathlib.Path.home().joinpath("plots", "correlation")

from mtDB.db.MTDB import MTDB


""" This script plots scatterplots and computes the 
    pearson correlation coeeficients of metrics in 
    the DataFrame. 

    It is used to study the impact of metrics like 
    hit rate, latency increase in find and lat API calls 
    on performance metrics like bandwidth, block read 
    and write latency. 
"""
class Correlation:
    def __init__(self, df, eval=["best"]):
        self.df = df 

        # performance metrics 
        self.perf_metric_list = ["blockReadSlat_avg_ns", 
                                    "blockWriteSlat_avg_ns", 
                                    "blockReadSlat_p99_ns",
                                    "blockWriteSlat_p99_ns",
                                    "blockReadSlat_p999_ns",
                                    "blockWriteSlat_p999_ns",
                                    "blockReadSlat_p9999_ns",
                                    "blockWriteSlat_p9999_ns",
                                    "blockReadSlat_p99999_ns",
                                    "blockWriteSlat_p99999_ns",
                                    "bandwidth_byte/s"]
        
        # different ways metrics are computed from multiple iterations
        # currently the mean is taken so the bandwidth is the mean bandwidth across 3 iterations
        self.eval_list = eval

        # predictive metrics that we are evaluating to see if it has relationship with performance metrics 
        self.pred_metrics = ["t1_t2_size_ratio", "t2HitCount", "nvmCacheSizeMB", "og-gain"]

        # the X label of performance metrics 
        self.x_label_map = {
            "bandwidth_byte/s": "Percent change in bandwidth due to tier-2 cache",
            "blockWriteSlat_avg_ns": "Percent change in mean write latency due to tier-2 cache",
            "blockReadSlat_avg_ns": "Percent change in mean read latency due to tier-2 cache",
            "blockReadSlat_p99_ns": "Percent change in p99 read latency due to tier-2 cache",
            "blockWriteSlat_p99_ns": "Percent change in p99 write latency due to tier-2 cache",
            "blockReadSlat_p999_ns": "Percent change in p999 read latency due to tier-2 cache",
            "blockWriteSlat_p999_ns": "Percent change in p999 write latency due to tier-2 cache",
            "blockReadSlat_p9999_ns": "Percent change in p9999 read latency due to tier-2 cache",
            "blockWriteSlat_p9999_ns": "Percent change in p9999 write latency due to tier-2 cache",
            "blockReadSlat_p99999_ns": "Percent change in p99999 read latency due to tier-2 cache",
            "blockWriteSlat_p99999_ns": "Percent change in p99999 write latency due to tier-2 cache"
        }

        # mapping the performance metric to file name 
        self.metric_name_map = {
            "bandwidth_byte/s": "bandwidth",
            "blockWriteSlat_avg_ns": "writeAvg",
            "blockReadSlat_avg_ns": "readAvg",
            "blockReadSlat_p99_ns": "readP99",
            "blockWriteSlat_p99_ns": "writeP99",
            "blockReadSlat_p999_ns": "readP999",
            "blockWriteSlat_p999_ns": "writeP999",
            "blockReadSlat_p9999_ns": "readP9999",
            "blockWriteSlat_p9999_ns": "writeP9999",
            "blockReadSlat_p99999_ns": "readP99999",
            "blockWriteSlat_p99999_ns": "writeP99999"
        }

        # mapping the JSON key of features to y labels 
        self.y_label_map = {
            "t1_t2_size_ratio": "(Tier-1 size)/(Tier-2 size)",
            "t1_t2_hr_ratio": "(Tier-1 hit rate)/(Tier-2 hit rate)",
            "log_t1_t2_hr_ratio": "log((Tier-1 hit rate)/(Tier-2 hit rate))",
            "hmrc1": "Hit-Miss ratio",
            "t2HitRate": "Tier-2 hit rate",
            "t2HitCount": "Tier-2 hit count",
            "nvmCacheSizeMB": "Tier-2 cache size (MB)",
            "overhead": "Overhead",
            "t2Gain": "Latency gain from T2",
            "og-gain": "Overhead-Gain"
        }

        self.output_dir = OUTPUT_DIR
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # table that stores the pearson correlation coeeficient and p-values for each pair of features compared
        # under different groupings 
        self.table = []
        self.table_df = pd.DataFrame()

        # list of grouping features 
        # each grouping features entry comprises of the list of 
        # features to group by and the name of the output directory 
        # - inside each specified folder a subdir will be created for a grouped feature tuple separated by _
        # - each file inside the subdir will be named with format:
        # - - *pred metric*_*eval type*_*pref metric*.png 
        self.grouping_features = [
            {
                "feature_list": ["machine_id"],
                "output_dir_name": "instance"
            },
            {
                "feature_list": ["workload_id"],
                "output_dir_name": "workload"
            },
            {
                "feature_list": ["machine_id", "workload_id"],
                "output_dir_name": "machine_workload"
            },
            {
                "feature_list": ["inputQueueSize", "processorThreadCount", "scaleIAT"],
                "output_dir_name": "config"
            },
            {
                "feature_list": ["inputQueueSize", "processorThreadCount", "scaleIAT", "machine_id"],
                "output_dir_name": "instance_config"
            },
            {
                "feature_list": ["inputQueueSize", "processorThreadCount", "scaleIAT", "machine_id", "workload_id"],
                "output_dir_name": "instance_workload_config"
            }
        ]


    def plot_df(self, df, perf_metric, eval_type, pred_metric, output_dir, instance_id, workload_id, config_id):
        # generate a scatterplot from a pair of features specified by the user 
        xlabel = self.x_label_map[perf_metric]
        ylabel = self.y_label_map[pred_metric]

        # a pair of performance and predictive metrics are evaluated under various eval types 
        output_file_name = "{}_{}_{}.png".format(self.metric_name_map[perf_metric], eval_type, pred_metric)
        output_path = output_dir.joinpath(output_file_name)

        # track when to map log of a metric just add "log_" before it 
        if "log" in pred_metric:
            pred_metric = "_".join(pred_metric.split("_")[1:])

        # get the x-axis (performance metric) and y-axis (predictive metric)
        perf_metric_list = df[perf_metric].to_numpy()
        pred_metric_list = df[pred_metric].to_numpy()

        # filter infs and nans from the perf and pred metrics
        inf_index = np.where(np.isinf(pred_metric_list))
        perf_metric_list = np.delete(perf_metric_list, inf_index)
        pred_metric_list = np.delete(pred_metric_list, inf_index)

        # compute pearson correlation coeeficient and p-value and store in list 

        if len(perf_metric_list) > 10:
            res = stats.pearsonr(perf_metric_list, pred_metric_list)
            table_entry = [
                instance_id,
                workload_id,
                config_id,
                perf_metric,
                eval_type, 
                pred_metric,
                res.statistic,
                res.pvalue
            ]
        else:
            table_entry = [
                instance_id,
                workload_id,
                config_id,
                perf_metric,
                eval_type, 
                pred_metric,
                0.0,
                np.nan
            ]
        self.table.append(table_entry)

        if not output_path.exists():
            # scatter plot of the predictive and performance metrics 
            self.plot(perf_metric_list, 
                        pred_metric_list, 
                        output_dir.joinpath(output_file_name),
                        xlabel,
                        ylabel)


    def run(self):
        # only get rows with tier-2 cache (MT caches)
        df = self.df[self.df["nvmCacheSizeMB"]>0]

        # iterate through each grouping features 
        for grouping_feature_map in self.grouping_features:
            grouping_feature_list = grouping_feature_map["feature_list"]

            # iterate through each group
            for group_tuple, cur_df in df.groupby(grouping_feature_list):

                # create a directory for each grouping 
                output_dir = self.output_dir.joinpath(grouping_feature_map["output_dir_name"])
                output_dir.mkdir(exist_ok=True)

                if type(group_tuple) == str:
                    group_tuple = tuple([group_tuple])

                # create a dir for each grouping tuple 
                output_dir_name_list = []
                config_list = []
                for param in group_tuple:
                    try:
                        str_param = str(int(param))
                        config_list.append(str_param)
                    except ValueError:
                        str_param = str(param)
                    output_dir_name_list.append(str_param)
                output_dir_name = "_".join(output_dir_name_list)
                output_dir = output_dir.joinpath(output_dir_name)
                output_dir.mkdir(exist_ok=True)

                # iterate through each combination of metrics to evaluate, plot and save 
                for metric_combo in itertools.product(*[self.perf_metric_list, self.eval_list, self.pred_metrics]):
                    perf_metric = metric_combo[0]
                    eval_type = metric_combo[1]
                    pred_metric = metric_combo[2]

                    machine_id = "all"
                    if "machine_id" in grouping_feature_list:
                        index = grouping_feature_list.index("machine_id")
                        machine_id = group_tuple[index]

                    workload_id = "all"
                    if "workload_id" in grouping_feature_list:
                        index = grouping_feature_list.index("workload_id")
                        workload_id = group_tuple[index]

                    config_id = "all"
                    if len(config_list) > 0:
                        config_id = "_".join(config_list)

                    self.plot_df(cur_df, perf_metric, eval_type, pred_metric, output_dir, machine_id, workload_id, config_id)

        self.table_df = pd.DataFrame(self.table, columns=["machine_id", "workload_id", "config_id", "perf", "eval", "pred", "pearson", "p-value"])
        self.table_df["pearson_abs"] = abs(self.table_df["pearson"])
        self.table_df = self.table_df.dropna()
        print(self.table_df.sort_values(by=["pearson_abs"], ascending=False))


    def plot(self, x_list, y_list, output_path, x_label, y_label, ylog=False):
        _, ax = plt.subplots(figsize=[14, 10])
        ax.scatter(x_list, y_list, s=80, alpha=0.6)
        ax.set_xlabel(x_label)
        ax.set_ylabel(y_label)

        if 'log' in y_label:
            ax.set_yscale("log")

        plt.tight_layout()
        plt.savefig(output_path)
        plt.close()


if __name__ == "__main__":
    eval_type = "mean"
    database = MTDB(DATA_DIR, eval=eval_type)
    combined_df = database._get_combined_df()

    analysis = Correlation(combined_df, eval=[eval_type])
    analysis.run()