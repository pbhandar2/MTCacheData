
import pathlib 
import pandas as pd 
from collections import defaultdict

from mtDB.db.ExperimentOutput import ExperimentOutput


""" This class stores data and statistics related to 
    all experiments output files in a user specified directory. 
    The user also specifies the machine and workload id 
    of the experiments in the directory. 
"""
class DBUnit:
    def __init__(self, 
                    data_dir, 
                    machine_id,
                    workload_id,
                    min_iteration = 3,
                    eval="mean",
                    grouping_features=["inputQueueSize", "processorThreadCount", "scaleIAT", "cacheSizeMB", "nvmCacheSizeMB"]):
        
        self._data_dir = pathlib.Path(data_dir)
        self._machine_id = machine_id 
        self._workload_id = workload_id
        self.min_iteration = min_iteration
        self._size = 0

        # main df contains the raw data on each experiment 
        self._main_df = pd.DataFrame()

        # set df contains the data on each experiment set (multiple iterations with same parameters)
        self.experiment_set_df = pd.DataFrame()

        # diff dif contains the percentage difference between statistics of an MT cache and an 
        # ST cache with the identically sized tier-1 cache 
        self.diff_df = pd.DataFrame()

        # feature used to group a set of experiments as one 
        # experiment having the same thread count, queue size, iat scale, t1 size and t2 size are considered the same 
        self.grouping_features = grouping_features

        # evaluation technique to compute the statistics from multiple iteration of the same experiment 
        self.eval = eval 

        self.mt_opt_count = 0 
        self.np_mt_opt_count = 0 
        self.st_opt_count = 0 

        self._load()
        self._run_mt_analysis()


    def get_size(self):
        return self.experiment_set_df.size


    def _get_key(self, output):
        # generate key by adding a "-" between the values of the grouping features
        key_array = []
        for feature in self.grouping_features:
            key_array.append(str(output.get_metric(feature)))
        return "-".join(key_array)


    def _load(self):
        json_list = []
        for data_file_path in self._data_dir.iterdir():
            output = ExperimentOutput(data_file_path)
            if output.is_output_complete():
                # collect a list of JSON features from each experiment output and create a dataframe 
                row = output.get_row()
                assert "inputQueueSize" in row
                json_list.append(row)

        self._main_df = pd.DataFrame(json_list)
        if self._main_df.size > 0:
            try:
                # group by the feature set that represent a unique experiment 
                # ["inputQueueSize", "processorThreadCount", "scaleIAT", "cacheSizeMB", "nvmCacheSizeMB"]
                # the numerous occurences of the same set of features are additional iteration of the experiment 
                experiment_set_feature_list = []
                for group_tuple, df in self._main_df.groupby(["inputQueueSize", "processorThreadCount", "scaleIAT", "cacheSizeMB", "nvmCacheSizeMB"]):
                    # TODO: can use some other method to pick or come up with metrics 
                    if self.eval == "mean" and df.size >= self.min_iteration:
                        # mean of all features in the set 
                        experiment_set_feature_list.append(df.mean())
                
                self.experiment_set_df = pd.DataFrame(experiment_set_feature_list)
            except:
                raise ValueError("Grouping error some feature not available")

        
    def _run_mt_analysis(self):
        """ For each MT cache with a corresponding ST cache with same tier 1 size collect, 
            percentage difference of all performance metrics. 
        """
        diff_row_list = []
        # group based on T1 size 
        if self.experiment_set_df.size > 0:
            for group_tuple, df in self.experiment_set_df.groupby(["inputQueueSize", "processorThreadCount", "scaleIAT", "cacheSizeMB"]):
                # check if an ST cache exists (T1>0, T2=0)
                st_df = df[(df["cacheSizeMB"]>0) & (df["nvmCacheSizeMB"]==0)]

                # TODO: fix what to do with ST and MT opt HMR list could plot it? 
                st_opt_hmr_list = []
                mt_opt_hmr_list = []
                # if an ST and an MT cache exists, we can compare! 
                if len(st_df)>0 and len(df)>1:
                    st_row = st_df.iloc[0]
                    # lets compare ST output with different MT outputs 
                    for _, row in df.iterrows():
                        if ((row["cacheSizeMB"]>0) & (row["nvmCacheSizeMB"] > 0)):

                            # collect percentage difference between every metric 
                            diff_row = row - st_row
                            diff_row = 100*diff_row/st_row
                            diff_row[["inputQueueSize", "processorThreadCount", "scaleIAT"]] = row[["inputQueueSize", "processorThreadCount", "scaleIAT"]]
                            diff_row[["cacheSizeMB", "nvmCacheSizeMB"]] = row[["cacheSizeMB", "nvmCacheSizeMB"]]
                            diff_row[["t1HitRate", "t2HitRate"]] = row[["t1HitRate", "t2HitRate"]]
                            diff_row_list.append(diff_row)

                            # is bandwidth higher? 
                            # is read and write latency lower? 
                            if ((diff_row['blockReadSlat_avg_ns'] < 0) and \
                                    (diff_row['blockWriteSlat_avg_ns'] < 0) and \
                                    (diff_row['bandwidth_byte/s'] > 0)):
                                self.mt_opt_count += 1
                                if row["cacheSizeMB"] >= row["nvmCacheSizeMB"]:
                                    self.np_mt_opt_count += 1 
                                mt_opt_hmr_list.append(float(row["hmrc1"]))
                            else:
                                self.st_opt_count += 1
                                st_opt_hmr_list.append(float(row["hmrc1"])) 
        
        self.diff_df = pd.DataFrame(diff_row_list)

        if len(self.diff_df) > 0:
            self.diff_df["machine_id"] = [self._machine_id]* len(self.diff_df)
            self.diff_df["workload_id"] = [self._workload_id]* len(self.diff_df)
            self.diff_df["t1_t2_size_ratio"] = self.diff_df["cacheSizeMB"]/self.diff_df["nvmCacheSizeMB"]

            # TODO: check to make sure that there was T2 eviction? 
            self.diff_df["t1_t2_hr_ratio"] = self.diff_df["t1HitRate"]/self.diff_df["t2HitRate"]

    
    def get_diff_df(self):
        return self.diff_df