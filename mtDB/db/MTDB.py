import pathlib 
import itertools
from platform import machine

import matplotlib.pyplot as plt

from mtDB.db.ExperimentOutput import ExperimentOutput
plt.rcParams.update({'font.size': 25})

from mtDB.db.DBUnit import DBUnit


class MTDB:
    def __init__(self, data_dir, eval="mean"):
        self.data_dir = pathlib.Path(data_dir)
        self.eval = eval

        # list of DBUnit objects
        # DBUnit represents a directory containing experiment outputs 
        self.unit_list = []
        self._load_data()


    def _load_data(self):
        # load all files inside the data directory 
        # create a single DBUnit per dir that contain data files 
        db_unit_path_list = []
        for data_file_path in self.data_dir.rglob("*"):
            if data_file_path.is_file():
                # each directory containing any of the output files is a DBUnit 
                db_unit_path = data_file_path.parent
                workload_id, machine_id = self._get_id(db_unit_path)
                # check if the DBUnit for the current directory being evaluated has 
                # already been created 
                if str(db_unit_path) not in db_unit_path_list:
                    db_unit_path_list.append(str(db_unit_path))
                    db_unit = DBUnit(db_unit_path, machine_id, workload_id, self.eval)
                    # store the DBUnit only if it has some valid points 
                    if db_unit.get_size() > 0:
                        self.unit_list.append(db_unit)


    def _get_id(self, db_unit_path):    
        # the subdir containing data files has path of format-> MACHINE/WORKLOAD/DATAFILE    
        return db_unit_path.name , db_unit_path.parent.name


    def plot_ts(self, metric_list, output_dir):
        # only support "best" and "mean" (mean needs to be checked)
        if self.eval != "best" and self.eval != "mean":
            raise ValueError("No support for other eval type {}".format(self.eval))

        y_label_map = {
            "overallBandwidth": "Bandwidth (MB/s)",
            "blockReadSLat_avg_ns": "Mean read latency (ns)",
            "blockWriteSLat_avg_ns": "Mean write latency (ns)",
            "blockReadSLat_p99_ns": "p99 read latency (ns)",
            "blockWriteSLat_p99_ns": "p99 write latency (ns)",
            "blockReadSLat_p999_ns": "p999 read latency (ns)",
            "blockWriteSLat_p999_ns": "p999 write latency (ns)",
            "t1HitRate": "T1 hit rate",
            "t2HitRate": "T2 hit rate",
            "t2HitCount": "T2 hit count",
            "backingReadLat_avg_ns": "Mean backing store read latency (ns)",
            "backingWriteLat_avg_ns": "Mean backing store write latency (ns)",
            "backingReadLat_p99_ns": "p99 backing store read latency (ns)",
            "backingWriteLat_p99_ns": "p99 backing store write latency (ns)"
        }

        # iterate through each metric
        for metric_name in metric_list:
            # iterate through each DB unit in the database 
            for db_unit in self.unit_list:
                workload_id, machine_id = db_unit.get_workload_and_machine_id()
                # iterate through each pair of ST, MT cache in the DBUnit 
                st_mt_pairs = db_unit.get_st_mt_pairs()
                for st_mt_pair in st_mt_pairs:
                    st_row = st_mt_pair[0]
                    mt_row = st_mt_pair[1]

                    st_output = db_unit.output_list[int(st_row["index"])]
                    mt_output = db_unit.output_list[int(mt_row["index"])]

                    st_time_list = []
                    mt_time_list = []

                    st_metric_list = []
                    mt_metric_list = []

                    x_len = min(len(st_output.ts_stat.keys()), len(mt_output.ts_stat.keys()))

                    sorted_st_time = sorted(st_output.ts_stat.keys())
                    sorted_mt_time = sorted(mt_output.ts_stat.keys())
                    for index in range(x_len):
                        st_cur_time = sorted_st_time[index]
                        mt_cur_time = sorted_mt_time[index]
                        st_time_list.append(st_cur_time)
                        mt_time_list.append(mt_cur_time)
                        if metric_name == "overallBandwidth":
                            st_metric_list.append(st_output.ts_stat[st_cur_time][metric_name]/(1024*1024))
                            mt_metric_list.append(mt_output.ts_stat[mt_cur_time][metric_name]/(1024*1024))
                        else:
                            st_metric_list.append(st_output.ts_stat[st_cur_time][metric_name])
                            mt_metric_list.append(mt_output.ts_stat[mt_cur_time][metric_name])

                    
                    # output dir will be output dir/*machine_id*/*workload_id*
                    plot_output_dir = output_dir.joinpath(machine_id, workload_id)
                    plot_output_dir.mkdir(parents=True, exist_ok=True)

                    mt_opt_flag = 0 
                    if mt_output.stat["bandwidth_byte/s"] > st_output.stat["bandwidth_byte/s"]:
                        mt_opt_flag = 1

                    # output file name will have format 
                    # *queue_size*_*thread_count*_*iat_scale*_*t1_size*_*t2_size*_*eval_type*_*metric_name*.png
                    output_file_name = "{}_{}_{}_{}_{}_{}_{}_{}.png".format(
                        int(mt_row["inputQueueSize"]),
                        int(mt_row["processorThreadCount"]),
                        int(mt_row["scaleIAT"]),
                        int(mt_row["cacheSizeMB"]),
                        int(mt_row["nvmCacheSizeMB"]),
                        self.eval,
                        metric_name,
                        mt_opt_flag
                    )
                    output_path = plot_output_dir.joinpath(output_file_name)
                    
                    if not output_path.exists():
                        _, ax = plt.subplots(figsize=[14, 10])
                        ax.plot(st_time_list, st_metric_list, "-*", markersize=10, label="ST")
                        ax.plot(mt_time_list, mt_metric_list, "-^", markersize=10, label="MT")
                        ax.set_xlabel("Time Elasped (sec)")
                        ax.set_ylabel("{}".format(y_label_map[metric_name]))
                        plt.legend()
                        plt.tight_layout()
                        plt.savefig(output_path)
                        plt.close()
                        print("Plot done: {}".format(output_path))
    

    def addToMap(self, list_map, machine_id, workload_id, config_id, value):
        if machine_id in list_map:
            if workload_id in list_map[machine_id]:
                if config_id in list_map[machine_id][workload_id]:
                    list_map[machine_id][workload_id][config_id].append(value)
                else:
                    list_map[machine_id][workload_id][config_id] = [value]
            else:
                list_map[machine_id][workload_id] = {
                    config_id: [value]
                }
        else:
            list_map[machine_id] = {
                workload_id: {
                    config_id: [value]
                }
            }


    def get_list_from_map_groups(self, list_map, grouping_features):
        # go through each item of map 
        # print(list_map)
        return_map = {}
        for machine_id in list_map:
            for workload_id in list_map[machine_id]:
                for config_id in list_map[machine_id][workload_id]:
                    item = list_map[machine_id][workload_id][config_id]

                    if len(grouping_features) == 1:
                        if 'machine_id' in grouping_features:
                            if machine_id in return_map:
                                print('MACHINE ID GROUPING {}'.format(machine_id))
                                return_map[machine_id] += item 
                            else:
                                return_map[machine_id] = item 
                        elif 'workload_id' in grouping_features:
                            if workload_id in return_map:
                                return_map[workload_id] += item 
                            else:
                                return_map[workload_id] = item 
                        else:
                            if config_id in return_map:
                                return_map[config_id] += item 
                            else:
                                return_map[config_id] = item 
                    elif len(grouping_features) == 2:
                        if 'machine_id' in grouping_features:
                            if 'workload_id' in grouping_features:
                                key = "{}_{}".format(machine_id, workload_id)
                                if key in return_map:
                                    return_map[key] += item 
                                else:
                                    return_map[key] = item 
                            else:
                                key = "{}_{}".format(machine_id, config_id)
                                if key in return_map:
                                    return_map[key] += item 
                                else:
                                    return_map[key] = item 
                        else:
                            key = "{}_{}".format(workload_id, config_id)
                            if key in return_map:
                                return_map[key] += item 
                            else:
                                return_map[key] = item 
                    else:
                        key = "{}_{}_{}".format(machine_id, workload_id, config_id)
                        if key in return_map:
                            return_map[key] += item 
                        else:
                            return_map[key] = item 
        return return_map 


    def plot_overhead_vs_bandwidth(self, output_dir):
        machine_id_set = set()
        workload_id_set = set()
        config_key_set = set()

        mt_bytes_map = {}
        t2_hit_count_map = {}
        percent_delta_bandwidth_map = {}
        byte_per_t2_hit_map = {}
        overhead_map = {}
        overhead_per_t2_map = {}

        for db_unit in self.unit_list:
            machine_id = db_unit._machine_id 
            workload_id = db_unit._workload_id 
            machine_id_set.add(machine_id)
            workload_id_set.add(workload_id)

            # get pair of ST and MT from each db unit 
            for st_mt_pair in db_unit.get_st_mt_pairs():
                st_row = st_mt_pair[0]
                mt_row = st_mt_pair[1]

                # get all the files relevant to the ST and MT rows 
                st_output_files = db_unit.get_output_files_per_row(st_row)
                mt_output_files = db_unit.get_output_files_per_row(mt_row)

                for st_mt_pair in list(itertools.product(st_output_files, mt_output_files)):
                    st_output_file = st_mt_pair[0]
                    mt_output_file = st_mt_pair[1]

                    # load the two experiment output files and get the difference 
                    st_output = ExperimentOutput(st_output_file)
                    mt_output = ExperimentOutput(mt_output_file)

                    if not st_output.is_output_complete() or not mt_output.is_output_complete():
                        continue 

                    config_key = mt_output.get_config_key()
                    config_key_set.add(config_key)

                    # get when t2 hits start 
                    t2_hr_at_T = mt_output.t2_hit_start
                    mt_bytes = mt_output.get_bytes_at_T(t2_hr_at_T)
                    st_bytes = st_output.get_bytes_at_T(t2_hr_at_T)
                    
                    if mt_bytes > 0:
                        percent_delta_bandwidth = 100*(mt_output.get_bandwidth()-st_output.get_bandwidth())/st_output.get_bandwidth()
                        t2_hit_count = mt_output.get_t2_hit_count()
                        cur_read_miss_byte = mt_output.get_read_io_processed_at_T(t2_hr_at_T)
                        cur_write_byte = mt_output.get_write_io_processed_at_T(t2_hr_at_T)

                        overhead_byte_per_read_miss_and_write_byte = (st_bytes - mt_bytes)/(cur_read_miss_byte+cur_write_byte)

                        if overhead_byte_per_read_miss_and_write_byte < 0:
                            overhead_byte_per_read_miss_and_write_byte = 0 

                        total_read_miss_byte = (100-mt_output.get_t1_hit_rate())*mt_output.get_read_io_processed()
                        total_write_miss_byte = mt_output.get_write_io_processed()

                        total_overhead = overhead_byte_per_read_miss_and_write_byte*(total_read_miss_byte+total_write_miss_byte)
                        total_overhead_per_t2_hit = total_overhead/t2_hit_count
                        
                        self.addToMap(mt_bytes_map, machine_id, workload_id, config_key, mt_bytes-st_bytes)
                        self.addToMap(t2_hit_count_map, machine_id, workload_id, config_key, t2_hit_count)
                        self.addToMap(percent_delta_bandwidth_map, machine_id, workload_id, config_key, percent_delta_bandwidth)
                        self.addToMap(byte_per_t2_hit_map, machine_id, workload_id, config_key, (mt_bytes-st_bytes)/t2_hit_count)
                        self.addToMap(overhead_map, machine_id, workload_id, config_key, total_overhead)
                        self.addToMap(overhead_per_t2_map, machine_id, workload_id, config_key, total_overhead_per_t2_hit)

        #print(mt_bytes_map)

        # group based on machine, workload and config 
        grouping_params = ["machine_id", "workload_id", "config_id"]
        perf_pred_pairs = [["byte_overhead", "bandwidth"], ["t2_hit_count", "bandwidth"], ["byte_per_t2_hit", "bandwidth"], ["overhead", "bandwidth"], ["overhead_per_t2", "bandwidth"]]
        for grouping_size in range(1, len(grouping_params)+1):
            for combo in itertools.combinations(grouping_params, grouping_size):
                cur_output_dir = output_dir.joinpath("_".join([_ for _ in combo]))
                cur_output_dir.mkdir(exist_ok=True, parents=True)

                perf_map = self.get_list_from_map_groups(percent_delta_bandwidth_map, combo)

                for perf_pred_pair in perf_pred_pairs:
                    output_file_name = "{}-{}.png".format(perf_pred_pair[0], perf_pred_pair[1])
                    
                    if perf_pred_pair[0] == "byte_overhead":
                        pred_map = self.get_list_from_map_groups(mt_bytes_map, combo)
                    elif perf_pred_pair[0] == "t2_hit_count":
                        pred_map = self.get_list_from_map_groups(t2_hit_count_map, combo)
                    elif perf_pred_pair[0] == "byte_per_t2_hit":
                        pred_map = self.get_list_from_map_groups(byte_per_t2_hit_map, combo)
                    elif perf_pred_pair[0] == "overhead":
                        pred_map = self.get_list_from_map_groups(overhead_map, combo)
                    elif perf_pred_pair[0] == "overhead_per_t2":
                        pred_map = self.get_list_from_map_groups(overhead_per_t2_map, combo)
                    
                    for key in pred_map:
                        temp_output_dir = cur_output_dir.joinpath(key)
                        temp_output_dir.mkdir(parents=True, exist_ok=True)
                        temp_output_path = temp_output_dir.joinpath(output_file_name)

                        if not temp_output_path.exists():
                            _, ax = plt.subplots(figsize=[14, 10])
                            ax.scatter(perf_map[key], pred_map[key], s=80, alpha=0.6)

                            ax.set_xlabel(perf_pred_pair[1])
                            ax.set_ylabel(perf_pred_pair[0])

                            plt.tight_layout()
                            plt.savefig(temp_output_path)
                            plt.close()
                            print("Plot done: {}".format(temp_output_path))


    def _get_combined_df(self):
        # the percentage difference in stats between MT and its corresponding 
        # ST cache from all eligible points 
        df = None 
        for db_unit in self.unit_list:
            if df is None:
                df = db_unit.get_diff_df()
            else:
                df = df.append(db_unit.get_diff_df(), ignore_index=True) 
        return df 


    def get_opt_count(self):
        st_opt_count, mt_opt_count, np_mt_opt_count = 0, 0, 0 
        for db_unit in self.unit_list:
            st_opt_count += db_unit.st_opt_count 
            mt_opt_count += db_unit.mt_opt_count 
            np_mt_opt_count += db_unit.np_mt_opt_count 
        
        return st_opt_count, mt_opt_count, np_mt_opt_count



