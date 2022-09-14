import itertools
import pathlib 
import matplotlib.pyplot as plt
plt.rcParams.update({'font.size': 25})

from mtDB.db.MTDB import MTDB


""" This scripts plots the percentage improvement from adding a cache tier 
    for various ratio of tier-1 and tier-2 cache sizes and ratio of tier-1 
    and tier-2 hit rate. 
"""
class Correlation:
    def __init__(self, df):
        self.df = df 
        self.perf_metric_list = ["blockReadSlat_avg_ns", 
                                    "blockWriteSlat_avg_ns", 
                                    "blockReadSlat_p99_ns",
                                    "blockWriteSlat_p99_ns",
                                    "bandwidth_byte/s"]
        self.eval_list = ["mean"]
        self.pred_metrics = ["t1_t2_size_ratio", "t1_t2_hr_ratio", "log_t1_t2_hr_ratio"]

        self.x_label_map = {
            "bandwidth_byte/s": "Percent change in bandwidth due to tier-2 cache",
            "blockWriteSlat_avg_ns": "Percent change in mean write latency due to tier-2 cache",
            "blockReadSlat_avg_ns": "Percent change in mean read latency due to tier-2 cache",
            "blockReadSlat_p99_ns": "Percent change in p99 read latency due to tier-2 cache",
            "blockWriteSlat_p99_ns": "Percent change in p99 write latency due to tier-2 cache"
        }

        self.metric_name_map = {
            "bandwidth_byte/s": "bandwidth",
            "blockWriteSlat_avg_ns": "writeAvg",
            "blockReadSlat_avg_ns": "readAvg",
            "blockReadSlat_p99_ns": "readP99",
            "blockWriteSlat_p99_ns": "writeP99"
        }

        self.y_label_map = {
            "t1_t2_size_ratio": "(Tier-1 size)/(Tier-2 size)",
            "t1_t2_hr_ratio": "(Tier-1 hit rate)/(Tier-2 hit rate)",
            "log_t1_t2_hr_ratio": "log((Tier-1 hit rate)/(Tier-2 hit rate))"
        }

        self.output_dir = pathlib.Path("./plots")


    def run(self):
        for metric_combo in itertools.product(*[self.perf_metric_list, self.eval_list, self.pred_metrics]):
            perf_metric = metric_combo[0]
            eval_type = metric_combo[1]
            pred_metric = metric_combo[2]

            xlabel = self.x_label_map[perf_metric]
            ylabel = self.y_label_map[pred_metric]

            output_file_name = "{}_{}_{}.png".format(self.metric_name_map[perf_metric], eval_type, pred_metric)

            print(output_file_name)

            if "log" in pred_metric:
                pred_metric = "_".join(pred_metric.split("_")[1:])

            perf_metric_list = self.df[perf_metric].to_list()
            pred_metric_list = self.df[pred_metric].to_list()

            self.plot(perf_metric_list, 
                        pred_metric_list, 
                        self.output_dir.joinpath(output_file_name),
                        xlabel,
                        ylabel)

    
    def plot(self, x_list, y_list, output_path, x_label, y_label, ylog=False):
        fig, ax = plt.subplots(figsize=[14, 10])
        
        ax.scatter(x_list, y_list, s=80, alpha=0.6)
        ax.set_xlabel(x_label)
        ax.set_ylabel(y_label)

        if 'log' in y_label:
            ax.set_yscale("log")

        plt.tight_layout()
        plt.savefig(output_path)
        plt.close()


if __name__ == "__main__":
    database = MTDB("/home/pranav/MTCacheData/output_dump")
    combined_df = database._get_combined_df()

    analysis = Correlation(combined_df)
    analysis.run()