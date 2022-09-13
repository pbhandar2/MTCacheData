
import pathlib 
import pandas as pd 
from collections import defaultdict
from scipy import stats

from mtDB.db.ExperimentOutput import ExperimentOutput
from mtDB.db.ExperimentSet import ExperimentSet


""" This class stores data and statistics about 
    all experiments related to the user specified 
    machine and workload.
"""
class DBUnit:
    def __init__(self, 
                    data_dir, 
                    machine_id,
                    workload_id,
                    grouping_features=["inputQueueSize", "processorThreadCount", "scaleIAT", "cacheSizeMB", "nvmCacheSizeMB"]):
        
        self._data_dir = pathlib.Path(data_dir)
        self._machine_id = machine_id 
        self._workload_id = workload_id

        # main df contains the raw data on each experiment 
        self._main_df = None 

        # map containing each experiment set 
        self.experiment_set_map = defaultdict(ExperimentSet)

        # set df contains the data on each experiment set (multiple iterations with same parameters)
        self.experiment_set_df = None

        # diff dif contains the percentage difference between statistics of an MT cache and an 
        # ST cache with the identically sized tier-1 cache 
        self.diff_df = None 

        # feature used to group a set of experiments as one 
        # experiment having the same thread count, queue size, iat scale, t1 size and t2 size are considered the same 
        self.grouping_features = grouping_features

        self.mt_opt_count = 0 
        self.np_mt_opt_count = 0 
        self.st_opt_count = 0 

        self._load()
        self._run_mt_analysis()


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
                # map the current output to a set of outputs to aggregate features of multiple iterations
                output_key = self._get_key(output)
                self.experiment_set_map[output_key].add(output)

                # collect a list of JSON features from each experiment output and create a dataframe 
                json_list.append(output.get_row())

        self._main_df = pd.DataFrame(json_list)

        # group by the feature set that represent a unique experiment 
        # ["inputQueueSize", "processorThreadCount", "scaleIAT", "cacheSizeMB", "nvmCacheSizeMB"]
        # the numerous occurences of the same set of features are additional iteration of the experiment 
        experiment_set_feature_list = []
        for group_tuple, df in self._main_df.groupby(["inputQueueSize", "processorThreadCount", "scaleIAT", "cacheSizeMB", "nvmCacheSizeMB"]):
            # mean of all features in the set 
            experiment_set_feature_list.append(df.mean())
            # TODO: can use some other method to pick or come up with metrics 
        self.experiment_set_df = pd.DataFrame(experiment_set_feature_list)

        
    def _run_mt_analysis(self):
        """ For each MT cache with a corresponding ST cache with same tier 1 size collect, 
            percentage difference of all performance metrics. 
        """
        diff_row_list = []
        # group based on T1 size 

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