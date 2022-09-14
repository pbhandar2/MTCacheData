import pathlib 
import pandas as pd
from collections import defaultdict 

from ExperimentOutput import ExperimentOutput

PERF_DIFF_FEATURES=[
    "bandwidth_byte/s"
]

PERF_DIFF_PERCENTILE_FEATURES=[
    ["blockReadSlat", "ns"],
    ["blockWriteSlat", "ns"]
]

""" This class analyzes a directory containing experiment outputs. 
"""
class MTDBUnit:

    def __init__(self, 
                    data_dir, 
                    experiment_id="random",
                    machine_id="unknown",
                    db_file_name="db.csv", 
                    sanity_check_flag=True):

        self._mt_stat = {} 
        self._config_key_list = []
        self._stat_diff_dict = {}

        self._data_dir = pathlib.Path(data_dir)

        self._best_mt_config_per_config = {}
        self._worst_mt_config_per_config = {}

        self._mt_perf_diff_list_dict = defaultdict(list)
        
        self._db_file_path = self._data_dir.joinpath(db_file_name)

        # check if the db file exists 
        if self._db_file_path.exists():
            self.df = pd.read_csv(self._db_file_path)
            if sanity_check_flag:
                self.sanity_check_df()
        else:
            self.df = None 
            self._load_df()
            self._load_mt_stat()
        

    def _load_df(self):
        json_list = []
        for data_file_path in self._data_dir.iterdir():
            output = ExperimentOutput(data_file_path)
            if output.is_output_complete():
                print("loading {}".format(output))
                json_list.append(output.get_row())
        self.df = pd.DataFrame(json_list)


    def sanity_check_df(self):
        # TODO: implement sanity check once we start to save DB files 
        pass 


    def get_best_mt(self):
        pass 


    def get_MT_improvement_count(self):
        pass 


    def _track_perf_diff(self, st_row, mt_row, config_key):
        t1_size_mb = int(st_row["cacheSizeMB"])
        t2_size_mb = int(mt_row["nvmCacheSizeMB"])
        compare_json_array = []

        if config_key not in self._stat_diff_dict:
            self._stat_diff_dict[config_key] = defaultdict(list)
        
        for percentile_metric_info_list in PERF_DIFF_PERCENTILE_FEATURES:
            metric_substr = percentile_metric_info_list[0]
            for metric_str, value in mt_row.items():
                if metric_substr in metric_str:
                    st_val = st_row[metric_str]
                    mt_val = mt_row[metric_str]
                    diff = mt_val - st_val 
                    diff_per = 100*diff/st_val

                    if t1_size_mb < t2_size_mb:
                        # pyramidal configuration 
                        if diff_per < 0.0:
                            self._mt_stat[config_key]["{}_pyMTOpt".format(metric_str)] += 1
                    else:
                        # non-pyramidal configuration 
                        if diff_per < 0.0:
                            self._mt_stat[config_key]["{}_npyMTOpt".format(metric_str)] += 1

                    self._stat_diff_dict[config_key][metric_str].append([t1_size_mb, t2_size_mb, diff_per])


    def _load_mt_stat(self):
        # using a set of configs as a primary key 
        # mostly compare holding these a set of configs constant 
        config_groups = self.df.groupby(["inputQueueSize", "processorThreadCount", "scaleIAT"])
        for config_tuple, config_df in config_groups:
            # for each config, exmample queusize=128, processorThreadCount=16, scaleIAT=100
            # key: 128-16-100
            config_key="-".join([str(_) for _ in config_tuple])
            self._config_key_list.append(config_key)
            self._mt_stat[config_key] = defaultdict(int)

            # group configs with the same T1 size together 
            # to compare ST and MT caches with the same T1 size 
            t1_size_mb_groups = config_df.groupby(["cacheSizeMB"])
            for t1_size_mb, t1_size_df in t1_size_mb_groups:
                self._mt_stat[config_key]["uniqueT1SizeMBValues"] += 1 

                # find the rows where there is no tier-2 cache 
                st_df = t1_size_df[t1_size_df["nvmCacheSizeMB"]==0]
                st_count = len(st_df)
                if st_count > 0:
                    self._mt_stat[config_key]["stCount"] += st_count
                    st_row = st_df.iloc[0]
                    if st_count == len(t1_size_df):
                        self._mt_stat[config_key]["soloST"] += 1
                    else: 
                        # ST and MT cache exists with current tier-1 size 
                        mt_df = t1_size_df[t1_size_df["nvmCacheSizeMB"]>0]
                        for mt_row_index, mt_row in mt_df.iterrows():
                            self._track_perf_diff(st_row, mt_row, config_key)
                            t2_size_mb = int(mt_row["nvmCacheSizeMB"])
                            if t2_size_mb > t1_size_mb:
                                self._mt_stat[config_key]["pyMTCount"] += 1
                            else:
                                self._mt_stat[config_key]["npyMTCount"] += 1
                else:
                    self._mt_stat[config_key]["soloMT"] += 1


    def print_mt_count(self):
        print("\nDBUnit={}".format(self._data_dir))
        print("----------------------------------\n")
        for config_key in self._config_key_list:
            print("Key={}".format(config_key))
            print("configCount(ST/MT-P/MT-NP)={}/{}/{}".format(
                    self._mt_stat[config_key]["stCount"],
                    self._mt_stat[config_key]["pyMTCount"],
                    self._mt_stat[config_key]["npyMTCount"]
            ))

            print("blockReadsLatAvgOPT(MT-P/MT-NP)={}/{}".format(
                    self._mt_stat[config_key]["{}_pyMTOpt".format("blockReadSlat_avg_ns")],
                    self._mt_stat[config_key]["{}_npyMTOpt".format("blockReadSlat_avg_ns")]
            ))

            print("blockWritesLatAvgOPT(MT-P/MT-NP)={}/{}".format(
                    self._mt_stat[config_key]["{}_pyMTOpt".format("blockWriteSlat_avg_ns")],
                    self._mt_stat[config_key]["{}_npyMTOpt".format("blockWriteSlat_avg_ns")]
            ))
            
            print()