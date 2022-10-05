
import pathlib 
import pandas as pd 
from collections import defaultdict

from mtDB.db.ExperimentOutput import ExperimentOutput


""" This class stores data and statistics related to  
    experiments output files in a user specified directory. 

    The user also specifies the machine, workload and the 
    evaluation methodology to represent multiple iterations 
    of the same experiment. 
"""
class DBUnit:
    def __init__(self, 
                    data_dir, 
                    machine_id,
                    workload_id,
                    eval,
                    min_iteration = 3,
                    grouping_features=["inputQueueSize", "processorThreadCount", "scaleIAT", "cacheSizeMB", "nvmCacheSizeMB"]):
        # directory containing experiment output files 
        self._data_dir = pathlib.Path(data_dir)

        # evaluation technique to compute the statistics from multiple iteration of the same experiment 
        self._eval = eval 

        self._machine_id = machine_id 
        self._workload_id = workload_id

        # minimum number of iteration per experiment to be considered a complete set 
        self._min_iteration = min_iteration

        # list of all ExperimentOutput classes 
        self.output_list = []

        # main df contains the raw data on each experiment 
        self._main_df = pd.DataFrame()

        # set df contains the data on each experiment set (multiple iterations with same parameters)
        self._experiment_set_df = pd.DataFrame()

        # diff dif contains the percentage difference between statistics of an MT cache and an 
        # ST cache with the identically sized tier-1 cache 
        self._diff_df = pd.DataFrame()

        # feature used to group a set of experiments as one 
        # experiment having the same thread count, queue size, iat scale, t1 size and t2 size are considered the same 
        self._grouping_features = grouping_features
        self._size = 0

        self.mt_opt_count = 0 
        self.np_mt_opt_count = 0 
        self.st_opt_count = 0 

        self._load()
        self._run_mt_analysis()


    def _load(self):
        # load self._main_df and self._experiment_set_df with data from output files 
        json_list = []
        for data_file_path in self._data_dir.iterdir():
            output = ExperimentOutput(data_file_path)
            if output.is_output_complete():
                # collect a list of JSON features from each experiment output and create a dataframe 
                row = output.get_row()
                row['index'] = len(self.output_list)
                json_list.append(row)
                self.output_list.append(output)
        self._main_df = pd.DataFrame(json_list)

        # group the data for the same experiment (multiple iterations) together to compute aggregate 
        # metrics based on the evaluation method (mean, best)
        if self._main_df.size > 0:
            try:
                # group by the feature set that represent a unique experiment 
                # ["inputQueueSize", "processorThreadCount", "scaleIAT", "cacheSizeMB", "nvmCacheSizeMB"]
                # the numerous occurences of the same set of features are additional iteration of the experiment 
                experiment_set_feature_list = []
                for _, df in self._main_df.groupby(["inputQueueSize", "processorThreadCount", "scaleIAT", "cacheSizeMB", "nvmCacheSizeMB"]):
                    if self._eval == "mean" and df.size >= self._min_iteration:
                        # mean of all features in the set 
                        experiment_set_feature_list.append(df.mean())
                    elif self._eval == "best" and df.size >= self._min_iteration:
                        # take the entry with the highest bandwidth 
                        experiment_set_feature_list.append(df[df["bandwidth_byte/s"]==df["bandwidth_byte/s"].max()].iloc[0])
                
                self._experiment_set_df = pd.DataFrame(experiment_set_feature_list)
            except:
                raise ValueError("Grouping error some feature not available")
    

    def get_st_mt_pairs(self):
        # get pair of every ST, MT cache with the same tier-1 size 
        st_mt_pairs = []
        for _, df in self._experiment_set_df.groupby(["inputQueueSize", "processorThreadCount", "scaleIAT", "cacheSizeMB"]):
            # group by experiment using the same T1 size 
            st_df = df[df["nvmCacheSizeMB"]==0]
            mt_df = df[df["nvmCacheSizeMB"]>0]

            if len(st_df) > 0 and len(mt_df) > 0:
                st_row = st_df.iloc[0]
                for _, mt_row in mt_df.iterrows():
                    st_mt_pairs.append([st_row, mt_row])
        
        return st_mt_pairs


    def get_output_files_per_row(self, row):
        output_file_name_prefix = "{}_{}_{}_{}_{}".format(int(row["inputQueueSize"]), 
                                                            int(row["processorThreadCount"]),
                                                            int(row["scaleIAT"]),
                                                            int(row["cacheSizeMB"]),
                                                            int(row["nvmCacheSizeMB"]))
        output_file_list = []
        for cur_output_file in self._data_dir.iterdir():
            cur_file_name = cur_output_file.name 
            if output_file_name_prefix in cur_file_name:
                output_file_list.append(cur_output_file)
        
        return output_file_list


    def _run_mt_analysis(self):
        """ For each MT cache with a corresponding ST cache with same tier 1 size collect, 
            percentage difference of all performance metrics. 
        """

        diff_row_list = []
        # group based on T1 size 
        if self._experiment_set_df.size > 0:
            for _, df in self._experiment_set_df.groupby(["inputQueueSize", "processorThreadCount", "scaleIAT", "cacheSizeMB"]):
                # check if an ST cache exists (T1>0, T2=0)
                st_df = df[(df["cacheSizeMB"]>0) & (df["nvmCacheSizeMB"]==0)]

                # TODO: fix what to do with ST and MT opt HMR list could plot it? 
                st_opt_hmr_list = []
                mt_opt_hmr_list = []

                # if an ST and an MT cache exists, we can compare! 
                if len(st_df)>0 and (len(df)-len(st_df))>0:
                    st_row = st_df.iloc[0]
                    # lets compare ST output with different MT outputs 
                    for _, row in df.iterrows():
                        if ((row["cacheSizeMB"]>0) & (row["nvmCacheSizeMB"] > 0)):

                            # collect percentage difference between every metric 
                            diff_row = row - st_row
                            diff_row = 100*diff_row/st_row

                            # reset some metrics where percentage difference doesn't apply 
                            diff_row[["inputQueueSize", "processorThreadCount", "scaleIAT"]] = row[["inputQueueSize", "processorThreadCount", "scaleIAT"]]
                            diff_row[["cacheSizeMB", "nvmCacheSizeMB"]] = row[["cacheSizeMB", "nvmCacheSizeMB"]]
                            diff_row[["t1HitRate", "t2HitRate"]] = row[["t1HitRate", "t2HitRate"]]
                            diff_row["hmrc1"] = row["hmrc1"]
                            diff_row["t2HitCount"] = row["t2HitCount"]
                            diff_row["writeIORatio"] = row["backingWriteIORequested_byte"]/row["backingIORequested_byte"]
                            diff_row["writeReqRatio"] = row["blockWriteReqCount"]/row["blockReqCount"]

                            # update alloc and find counts along with total and mean net latency change 

                            # the number of find is the total get request 
                            diff_row["findLatIncrease"] = row["t1GetCount"] * (row["findLat_avg_ns"]-st_row["findLat_avg_ns"])/1e9

                            # the number of alloc is the total write request + get miss count 
                            diff_row["allocLatIncrease"] = row["allocationCount"] * (row["allocLat_avg_ns"]-st_row["allocLat_avg_ns"])/1e9
                            diff_row["loadLatIncrease"] =  row["blockReqCount"] * (row["loadDuration_avg_us"] - st_row["loadDuration_avg_us"])*1000/1e9

                            diff_row["overhead"] = diff_row["findLatIncrease"] + diff_row["allocLatIncrease"] + diff_row["loadLatIncrease"]

                            diff_row["t2Gain"] = ((st_row["backingReadLat_avg_ns"] - (row["t2ReadLat_avg_us"]*1000)) * row["t2HitCount"])/1e9
                            diff_row["backingWriteGain"] = (row["backingWriteReqCount"]*(st_row["backingWriteLat_avg_ns"] - row["backingWriteLat_avg_ns"]))/1e9
                            diff_row["backingReadGain"] = ((row["backingReqCount"]-row["backingWriteReqCount"])*(st_row["backingReadLat_avg_ns"] - row["backingReadLat_avg_ns"]))/1e9

                            diff_row["og-gain"] = diff_row["t2Gain"] + diff_row["backingWriteGain"] + diff_row["backingReadGain"] - diff_row["overhead"]

                            diff_row["st_backingReadSize_avg_byte"] = st_row["backingReadSize_avg_byte"]
                            diff_row["st_backingWriteSize_avg_byte"] = st_row["backingWriteSize_avg_byte"]


                            # the total net latency change due to find and alloc 
                            # change in mean find and alloc latency 
                            self.compute_total_latency_from_percentile("allocLat", row)

                            # the total latency change in backing store read latency 
                            # the total latency change in bacing store write latency 

                            # the total and mean latency difference between T2 hits and ST read miss 
                            

                            # diff_row["t1_t2"] = row["hmrc1"]
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
        
        self._diff_df = pd.DataFrame(diff_row_list)

        if len(self._diff_df) > 0:
            self._diff_df["machine_id"] = [self._machine_id]* len(self._diff_df)
            self._diff_df["workload_id"] = [self._workload_id]* len(self._diff_df)
            self._diff_df["t1_t2_size_ratio"] = self._diff_df["cacheSizeMB"]/self._diff_df["nvmCacheSizeMB"]

            # TODO: check to make sure that there was T2 eviction? 
            self._diff_df["t1_t2_hr_ratio"] = self._diff_df["t1HitRate"]/self._diff_df["t2HitRate"]

            
    def get_size(self):
        return self._experiment_set_df.size


    def _get_key(self, output):
        # generate key by adding a "-" between the values of the grouping features
        key_array = []
        for feature in self._grouping_features:
            key_array.append(str(output.get_metric(feature)))
        return "-".join(key_array)
    

    def compute_total_latency_from_percentile(self, metric_name_substring, row):
        # from percentile latency and total requests compute total latency 
        # for the findAlloc call get total lat from ST and MT 
        percentile_data = []
        for column_name in row.index:
            if metric_name_substring in column_name:
                entry = row[column_name]
                split_column_name = column_name.split("_")
                percentile_str = split_column_name[1].replace("p","")
                if percentile_str == "avg":
                    mean_lat = entry 
                else:
                    percentile_data.append([int(percentile_str), entry])
        
        # sort data by percentile value 
        total = 0 
        percentile_data = sorted(percentile_data, key=lambda x: x[0])
        for index in range(len(percentile_data)-1):
            cur_percentile = percentile_data[index]
            next_percentile = percentile_data[index+1]

            weight = next_percentile[0] - cur_percentile[0]
            value = (next_percentile[1] + cur_percentile[1])/2

        # get the number of requests between each adjacent percentile values 
        # 95, 99, 4 percent of request have between this and this late 
        # 99 = 1, 999= 0.1, 9999=0.01, 99999=0.001
        # between 

    
    def get_diff_df(self):
        return self._diff_df

    
    def get_workload_and_machine_id(self):
        return self._workload_id, self._machine_id