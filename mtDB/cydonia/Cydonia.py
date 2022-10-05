import pathlib 
from collections import Counter, defaultdict 
import pandas as pd 
from mtDB.cydonia.RDTraceProfiler import RDTraceProfiler
from mtDB.db.ExperimentOutput import ExperimentOutput

RD_TRACE_DIR = pathlib.Path("/research2/mtc/cp_traces/rd_traces_4k/")
RD_PROFILE_OUTPUT_DIR = pathlib.Path("/research2/mtc/cp_traces/pranav/rd_profiler/")

""" This class compares experiments that are identically configured 
    except for the size of the tier 1 and tier 2. It compares the performance 
    of each experiment at identifical intervals and extrapolates to predict
    future performance. 
"""
class Cydonia:
    def __init__(self, st_output_path, mt_output_path, workload_name):
        self._st = ExperimentOutput(st_output_path)
        self._mt = ExperimentOutput(mt_output_path)
        self._st_ts_keys = sorted(self._st.ts_stat.keys())
        self._mt_ts_keys = sorted(self._mt.ts_stat.keys())
        self._end_st_key = self._st_ts_keys[-1]
        self._end_mt_key = self._mt_ts_keys[-1]
        self._workload = workload_name 
        self._load()


    def _load(self):
        mt_row_list, st_row_list = [], []
        st_prev_window_stats, mt_prev_window_stats = defaultdict(int), defaultdict(int)
        st_future_stats, mt_future_stats = defaultdict(int), defaultdict(int)

        # iterate through each key in MT time series 
        for key_index, mt_key in enumerate(self._mt_ts_keys):
            # only compare the overlapping period 
            # if there are more keys in MT case then we break here 
            # if there are more keys in ST case the loop would automatically break 
            if key_index == len(self._st_ts_keys):
                break 

            # the timing is not perfect so we have to fetch the ST key separetely 
            # for instance, we track at 30 second intervals and the time at the 
            # second interval could be 60 in ST and 61 in MT 
            st_key = self._st_ts_keys[key_index]

            mt_row_json, st_row_json = {}, {}

            """ For each window collect, 
                - read IO processed in byte
                - write IO processed in byte
                - T1 hit byte 
                - T1 miss byte
                - T2 hit byte 
                - Delta between ST and MT bandwidth 
                - Delta between ST and MT block read/write Slat 
                - Delta between ST and MT backing read/write lat 
                - Delta IAT wait duration (due to workload)
                - Delta load duration (due to system)

                - Future read IO to be processed 
                - Future write IO to be processed
                - Future T1 hit byte 
                - Future T1 miss byte 
                - Future T2 hit byte 
                - Future IAT wait duration 
            """

            st_window_stats, mt_window_stats = defaultdict(int), defaultdict(int)
            st_window_stats["ts"], mt_window_stats["ts"] = st_key, mt_key
            st_window_stats["len"], mt_window_stats["len"] = st_key - st_prev_window_stats["ts"], mt_key - mt_prev_window_stats["ts"]

            """ Compute the amount of IO completed in the current, previous and future windows. IO processed in this window could 
                have been submitted in the previous window. """
            st_window_stats["readProcessedByte"] = self._st.ts_stat[st_key]["readIOProcessed"] - st_prev_window_stats["readProcessedByte"]
            st_future_stats["readProcessedByte"] = self._st.ts_stat[self._end_st_key]["readIOProcessed"] - self._st.ts_stat[st_key]["readIOProcessed"] 
            st_window_stats["writeProcessedByte"] = self._st.ts_stat[st_key]["writeIOProcessed"] - st_prev_window_stats["writeProcessedByte"]
            st_future_stats["writeProcessedByte"] = self._st.ts_stat[self._end_st_key]["writeIOProcessed"] - self._st.ts_stat[st_key]["writeIOProcessed"] 

            mt_window_stats["readProcessedByte"] = self._mt.ts_stat[mt_key]["readIOProcessed"] - mt_prev_window_stats["readProcessedByte"]
            mt_future_stats["readProcessedByte"] = self._mt.ts_stat[self._end_mt_key]["readIOProcessed"] - self._mt.ts_stat[mt_key]["readIOProcessed"]
            mt_window_stats["writeProcessedByte"] = self._mt.ts_stat[mt_key]["writeIOProcessed"] - mt_prev_window_stats["writeProcessedByte"]
            mt_future_stats["writeProcessedByte"] = self._mt.ts_stat[self._end_mt_key]["writeIOProcessed"] - self._mt.ts_stat[mt_key]["writeIOProcessed"]

            st_prev_window_stats["readProcessedByte"] = st_window_stats["readProcessedByte"]
            st_prev_window_stats["writeProcessedByte"] = st_window_stats["writeProcessedByte"]
            mt_prev_window_stats["readProcessedByte"] = mt_window_stats["readProcessedByte"]
            mt_prev_window_stats["writeProcessedByte"] = mt_window_stats["writeProcessedByte"]

            # compute the bandwidth of the current window and the change from previous window 
            st_window_stats["bandwidth"] = (st_window_stats["readProcessedByte"] + st_window_stats["writeProcessedByte"])/st_window_stats["len"]
            st_window_stats["deltaBandwidth"] = st_window_stats["bandwidth"] - st_prev_window_stats["bandwidth"]
            st_window_stats["deltaPercentBandwidth"] = 100*st_window_stats["deltaBandwidth"]/st_prev_window_stats["bandwidth"]

            st_prev_window_stats["bandwidth"] = st_window_stats["bandwidth"]
            st_prev_window_stats["deltaBandwidth"] = st_window_stats["deltaBandwidth"]
            st_prev_window_stats["deltaPercentBandwidth"] = st_window_stats["deltaPercentBandwidth"]

            mt_window_stats["bandwidth"] = (mt_window_stats["readProcessedByte"] + mt_window_stats["writeProcessedByte"])/st_window_stats["len"]
            mt_window_stats["deltaBandwidth"] = mt_window_stats["bandwidth"] - mt_prev_window_stats["bandwidth"]
            mt_window_stats["deltaPercentBandwidth"] = 100*mt_window_stats["deltaBandwidth"]/mt_prev_window_stats["bandwidth"]

            mt_prev_window_stats["bandwidth"] = mt_window_stats["bandwidth"]
            mt_prev_window_stats["deltaBandwidth"] = mt_window_stats["deltaBandwidth"]
            mt_prev_window_stats["deltaPercentBandwidth"] = mt_window_stats["deltaPercentBandwidth"]

            mt_window_stats["bandwidthSTvsMT"] = (st_window_stats["bandwidth"] - mt_window_stats["bandwidth"])/st_window_stats["bandwidth"]
            st_window_stats["bandwidthSTvsMT"] = mt_window_stats["bandwidthSTvsMT"]

            mt_window_stats["deltaBandwidthSTvsMT"] = mt_window_stats["bandwidthSTvsMT"] - mt_prev_window_stats["bandwidthSTvsMT"]
            mt_window_stats["deltaPercentBandwidthSTvsMT"] = 100*mt_window_stats["deltaBandwidthSTvsMT"]/mt_prev_window_stats["bandwidthSTvsMT"]

            st_prev_window_stats["ts"], mt_prev_window_stats["ts"] = st_key, mt_key
            st_prev_window_stats["bandwidthSTvsMT"], mt_prev_window_stats["bandwidthSTvsMT"] = mt_window_stats["bandwidthSTvsMT"], mt_window_stats["bandwidthSTvsMT"]


            # the first key being compared 
            if key_index == 0:
                # read bytes process in this time window 
                st_window_read_processed = self._st.ts_stat[st_key]["readIOProcessed"] 
                mt_window_read_processed = self._mt.ts_stat[mt_key]["readIOProcessed"]

                # read bytes to be processed in the future 
                st_future_read_byte = self._st.ts_stat[self._mt_ts_keys[-1]]["readIOProcessed"] - st_window_read_processed
                mt_future_read_byte = self._mt.ts_stat[self._mt_ts_keys[-1]]["readIOProcessed"] - mt_window_read_processed

                # write requests 
                st_window_write_byte_processed = self._st.ts_stat[st_key]["writeIOProcessed"] 
                mt_window_write_byte_processed = self._mt.ts_stat[mt_key]["writeIOProcessed"] 

                # read bytes to be processed in the future 
                st_future_write = self._st.ts_stat[self._mt_ts_keys[-1]]["writeIOProcessed"] - st_window_write_byte_processed
                mt_future_write = self._mt.ts_stat[self._mt_ts_keys[-1]]["writeIOProcessed"] - mt_window_write_byte_processed

                # t1 hit bytes 
                st_window_t1_hit_byte = self._st.ts_stat[st_key]["t1HitRate"] * st_window_read_processed
                mt_window_t1_hit_byte = self._mt.ts_stat[mt_key]["t1HitRate"] * mt_window_read_processed

                # t1 hit bytes in future 
                st_future_t1_hit_byte = (self._st.ts_stat[self._mt_ts_keys[-1]]["t1HitRate"] - self._st.ts_stat[st_key]["t1HitRate"]) * st_future_read
                mt_future_t1_hit_byte = (self._mt.ts_stat[self._mt_ts_keys[-1]]["t1HitRate"] - self._mt.ts_stat[mt_key]["t1HitRate"]) * mt_future_read

                # t1 miss bytes 
                st_window_t1_miss_byte = st_window_read_processed - st_window_t1_hit_byte
                mt_window_t1_miss_byte = mt_window_read_processed - mt_window_t1_hit_byte

                # t1 miss bytes in future 
                st_t1_miss_byte = st_future_read_byte - st_future_t1_hit_byte
                mt_t1_miss_byte = mt_future_read_byte - mt_future_t1_hit_byte

                # t2 hit bytes 
                st_window_t2_hit_byte = 0 
                mt_window_t2_hit_byte = self._mt.ts_stat[mt_key]["t2HitRate"] * mt_window_t1_miss_byte
            else:
                pass 
            
            st_row_json["block_req_count_at_window_end"] = self._st.ts_stat[st_key]["blockReqCount"]
            mt_row_json["block_req_count_at_window_end"] = self._mt.ts_stat[mt_key]["blockReqCount"]

            st_row_json["bandwidth"] = self._st.ts_stat[st_key]["overallBandwidth"]
            mt_row_json["bandwidth"] = self._mt.ts_stat[mt_key]["overallBandwidth"]

            st_row_json["t1HitRate"] = self._st.ts_stat[st_key]["t1HitRate"] 
            mt_row_json["t1HitRate"] = self._mt.ts_stat[mt_key]["t1HitRate"] 

            st_row_json["t2HitRate"] = 0.0
            mt_row_json["t2HitRate"] = self._mt.ts_stat[mt_key]["t2HitRate"] 

            st_row_json["writeIOProcessed"] = self._st.ts_stat[st_key]["writeIOProcessed"] 
            mt_row_json["writeIOProcessed"] = self._mt.ts_stat[mt_key]["writeIOProcessed"] 

            st_row_json["readIOProcessed"] = self._st.ts_stat[st_key]["readIOProcessed"] 
            mt_row_json["readIOProcessed"] = self._mt.ts_stat[mt_key]["readIOProcessed"] 

            st_row_json["blockReadSLat_avg_ns"] = self._st.ts_stat[st_key]["blockReadSLat_avg_ns"] 
            mt_row_json["blockReadSLat_avg_ns"] = self._mt.ts_stat[mt_key]["blockReadSLat_avg_ns"] 

            st_row_json["blockWriteSLat_avg_ns"] = self._st.ts_stat[st_key]["blockWriteSLat_avg_ns"] 
            mt_row_json["blockWriteSLat_avg_ns"] = self._mt.ts_stat[mt_key]["blockWriteSLat_avg_ns"] 

            mt_row_json["T"] = int(mt_key)
            st_row_json["T"] = int(st_key)



            mt_row_list.append(mt_row_json)
            st_row_list.append(st_row_json)

        if len(mt_row_list) > 0:
            mt_row_list[-1]["T"] = int(self._mt.get_runtime())
            mt_row_list[-1]["bandwidth"] = self._mt.get_bandwidth()
            mt_row_list[-1]["t1HitRate"] = self._mt.get_t1_hit_rate()
            mt_row_list[-1]["t2HitRate"] = self._mt.get_t2_hit_rate()

        if len(st_row_list) > 0:
            st_row_list[-1]["T"] = int(self._st.get_runtime())
            st_row_list[-1]["bandwidth"] = self._st.get_bandwidth()
            st_row_list[-1]["t1HitRate"] = self._st.get_t1_hit_rate()
            st_row_list[-1]["t2HitRate"] = self._st.get_t2_hit_rate()
        
        assert len(mt_row_list) > 0 and len(st_row_list) > 0 and len(mt_row_list) == len(st_row_list)

        self.mt_df = pd.DataFrame(mt_row_list)
        self.st_df = pd.DataFrame(st_row_list)


    def run(self):
        pass 
        # st_rd_profiler_df = self._st_rd_trace_profiler.df
        # mt_rd_profiler_df = self._mt_rd_trace_profiler.df
        # main_mt_df = pd.merge(left=mt_rd_profiler_df, right=self.mt_df, left_on="block_req_count_at_window_end", right_on="block_req_count_at_window_end")
        # main_st_df = pd.merge(left=st_rd_profiler_df, right=self.st_df, left_on="block_req_count_at_window_end", right_on="block_req_count_at_window_end")
        # # main_mt_df["t2HitCount"] = main_mt_df["t2HitRate"] * 
        # main_mt_df["readCount"] = main_mt_df["cum_t1_hit_count"] + main_mt_df["cum_t2_hit_count"] + main_mt_df["cum_miss_count"]
        # main_mt_df["delta_bandwidth"] = (main_mt_df["bandwidth"] - main_st_df["bandwidth"])/(1024*1024)
        # main_mt_df["delta_t2_hit_count"] = main_mt_df["t2_hit_count"].shift(1) - main_mt_df["t2_hit_count"]
        # main_mt_df["delta_delta_bandwidth"] = main_mt_df["delta_bandwidth"].shift(1) - main_mt_df["delta_bandwidth"]
        # main_mt_df["delta_t2HitRate"] = main_mt_df["t2HitRate"].shift(1) - main_mt_df["t2HitRate"]
        # main_mt_df["t2HitCount"] = main_mt_df["readCount"] * main_mt_df["t2HitRate"]
        # main_mt_df["delta_t2HitCount"] = main_mt_df["t2HitCount"].shift(1) - main_mt_df["t2HitCount"]


       
        # print("Theortical T2 hits: {}, got: {}".format(mt_rd_profiler_df.iloc[-1]["cum_t2_hit_count"], self._mt.get_t2_hit_count()))

        # print(self.st_df)
        # print(main_st_df)
        # print(main_mt_df)
        # print(main_mt_df.T)

        # print(main_mt_df[["delta_bandwidth", "delta_delta_bandwidth", "hmr", "hmr2", "hmr3", "cum_hmr", "cum_hmr2", "t2_hit_count", "write_count", "t2HitRate", "delta_t2HitRate", "t2HitCount", "delta_t2HitCount"]].corr(method ='pearson'))
        # print(main_mt_df[["delta_bandwidth", "delta_delta_bandwidth", "hmr", "hmr2", "hmr3", "cum_hmr", "cum_hmr2", "t2_hit_count", "write_count", "t2HitRate", "delta_t2HitRate", "t2HitCount", "delta_t2HitCount"]].corr(method ='spearman'))

        # # what is the difference between expected and total hits 

        # expected_t2_hits = mt_rd_profiler_df.iloc[-1]["t2_hit_count"]


        """ Each output will contain the following fields:
        - min, max, mean, median, var of % difference in bandwidth predictions 
        - pearson corrleation of 
        """
        

        # self._window_count = min(len(self._st_ts_keys), len(self._mt_ts_keys))
        # # per window stats 
        # self._window_t2_hit_count = defaultdict(int)
        # self._window_t2_miss_count = defaultdict(int)
        # self._window_write_count = defaultdict(int)
        # self._window_write_requested_byte = defaultdict(int)

        # # cumulative stats 
        # self._cum_t2_miss_count = defaultdict(int)
        # self._cum_t2_hit_count = defaultdict(int)
        # self._cum_write_count = defaultdict(int)
        # self._cum_write_requested_byte = defaultdict(int)
        # self._cum_block_req_completed = defaultdict(int)

        # # cumulative stats of warm windows 
        # self._cum_t2_miss_count = defaultdict(int)
        # self._cum_t2_hit_count = defaultdict(int)
        # self._cum_write_count = defaultdict(int)
        # self._cum_write_requested_byte = defaultdict(int)
        # self._cum_block_req_completed = defaultdict(int)

        # # mapping of request timestamps to time windows of the physical experiment 
        # self._ts_map = defaultdict(int)

        # # ground truth about whether MT or ST cache being compared performs better 
        # self._mt_opt_flag = self._mt.get_bandwidth() > self._st.get_bandwidth()

        # # percent difference in bandwidth between MT and ST cache being compared 
        # self._bandwidth_diff = self._mt.get_bandwidth()-self._st.get_bandwidth()
        # self._bandwidth_percent_diff = 100 * (self._bandwidth_diff)/self._st.get_bandwidth()

        # # end timestamp of the last window compared 
        # self._last_window_end_ts = 0 

    #     # read the RD trace and load all the necessary information 
    #     self._load_rd_trace()


    # def _get_cum_block_req_count_per_window(self):
    #     # block requests processed by each time window 
    #     mt_keys = sorted(self._mt.ts_stat.keys())
    #     return [self._mt.ts_stat[key]["blockReqCount"] for key in mt_keys]
        

    # def _load_rd_trace(self):
    #     # track the cumulative hit count, writes and read misses 
    #     cum_t2_hit_count, cum_write_count, cum_read_miss_count = 0, 0, 0
    #     mt_keys = sorted(self._mt.ts_stat.keys())

    #     # get the cumulative block request count at each time window 
    #     cum_block_req_per_window = self._get_cum_block_req_count_per_window()

    #     with self._rd_trace_path.open("r") as f:
    #         # track the index and block request count of the current time window 
    #         cur_window_index, block_req_count = 0, 0 

    #         # size of T1 and T2 
    #         # TODO: due to T2 usage issues, currently using 80% values 
    #         # TODO: track what is the actual usage as well and predict based on that 
    #         t1_size, t2_size = self._mt.get_ram_size()*256, int(self._mt.get_nvm_size()*256*0.8)

    #         # counter of read and write RD 
    #         r_counter, w_counter = Counter(), Counter() 

    #         # read and write counter 
    #         r_count, w_count = 0, 0 

    #         # maximum read and write RD 
    #         # the physical timestamp of the previous window 
    #         max_r_rd, max_w_rd, prev_physical_ts = -1, -1, -1 
    #         cur_window_block_req_count = cum_block_req_per_window[cur_window_index]

    #         line = f.readline()
    #         while line:
    #             split_line = line.rstrip().split(",")
    #             # get contents from the line 
    #             rd, op, cur_ts = int(split_line[0]), split_line[1], int(split_line[2])

    #             if op == "r":
    #                 r_counter[rd] += 1 
    #                 r_count += 1
    #                 if rd > max_r_rd:
    #                     max_r_rd = rd 
    #             else:
    #                 w_counter[rd] += 1 
    #                 w_count += 1
    #                 if rd > max_w_rd:
    #                     max_w_rd = rd 
                
    #             # the rd trace has timestamp for each page request 
    #             # set of page request with the same timestamp belong 
    #             # to the same block request 
    #             if cur_ts != prev_physical_ts:
    #                 block_req_count += 1
                
    #             if block_req_count == cur_window_block_req_count:
    #                 # found a new section 
    #                 cur_t = mt_keys[cur_window_index]
    #                 self._last_window_end_ts = cur_t 

    #                 # gather approximate tier-2 hit count 
    #                 t2_hit_count = 0 
    #                 for i in range(t1_size-1, t1_size+t2_size+1):
    #                     t2_hit_count += r_counter[i]
                    
    #                 # window stats 
    #                 self._window_t2_hit_count[cur_t] = t2_hit_count
    #                 self._window_t2_miss_count[cur_t] = r_count - t2_hit_count
    #                 self._window_write_count[cur_t] = w_count 
    #                 self._window_write_requested_byte[cur_t] = self._mt.ts_stat[cur_t]["writeIOProcessed"]

    #                 if cur_window_index > 0:
    #                     self._window_write_requested_byte[cur_t] -= self._mt.ts_stat[mt_keys[cur_window_index-1]]["writeIOProcessed"]

    #                 self._ts_map[cur_t] = cur_ts

    #                 cum_t2_hit_count += t2_hit_count
    #                 cum_read_miss_count += (r_count - t2_hit_count)
    #                 cum_write_count += w_count 

    #                 # cumulative stats 
    #                 self._cum_t2_hit_count[cur_t] = cum_t2_hit_count
    #                 self._cum_t2_miss_count[cur_t] = cum_read_miss_count
    #                 self._cum_write_count[cur_t] = cum_write_count
    #                 self._cum_block_req_completed[cur_t] = cum_block_req_per_window[cur_window_index]
                    
    #                 # reset 
    #                 r_counter = Counter()
    #                 w_counter = Counter() 
    #                 r_count = 0 
    #                 w_count = 0 
    #                 max_r_rd = -1 
    #                 max_w_rd = -1 
                    
    #                 # only compare overlapping windows 
    #                 cur_window_index += 1 
    #                 if cur_window_index >= self._window_count:
    #                     break
    #                 else:
    #                     # update the block req count for the window 
    #                     cur_window_block_req_count = cum_block_req_per_window[cur_window_index]
                        
    #             prev_physical_ts = cur_ts
    #             line = f.readline()


    # def run(self):
    #     print("Comparing {},{}".format(self._st._output_path, self._mt._output_path))

    #     # we only need to look at the overlaping window 
    #     st_keys = sorted(self._st.ts_stat.keys())
    #     mt_keys = sorted(self._mt.ts_stat.keys())
    #     num_window = min(len(st_keys), len(mt_keys))

    #     t2_warm_up_period = -1  

    #     for cur_window_index in range(num_window):
    #         # this part of the code runs after each time window to 
    #         mt_key = mt_keys[cur_window_index]
    #         st_key = st_keys[cur_window_index]

    #         # track the current difference in bandwidth 
    #         cur_bandwidth_diff = (self._st.get_bytes_processed_at_T(st_key) - \
    #                         self._mt.get_bytes_processed_at_T(mt_key))/(mt_key*1024*1024)
    #         cur_bandwidth_diff_percent = 100*cur_bandwidth_diff/(self._st.get_bytes_processed_at_T(st_key)/(1024*1024))
            
    #         # current hit miss byte ratio per second 
    #         cur_hit_miss_byte_ratio = (self._cum_t2_hit_count[mt_key]*4096)/((self._cum_t2_miss_count[mt_key]*4096)+self._cum_write_requested_byte[mt_key])
    #         cur_hit_miss_byte_ratio_per_ms = 1000*cur_hit_miss_byte_ratio/mt_key 

    #         if cur_hit_miss_byte_ratio > 0 and t2_warm_up_period == -1:
    #             t2_warm_up_period = cur_window_index-1
    #             # track the metrics since T2 hits > 0, to truly estimate 
    #             # the effect of T2 hits relative to misses 

    #             # including the warm up time will bias the result towards 
    #             # the ST cache and doesn't fairly evaluate the value of a 
    #             # T2 hit 

    #             # so first we need to track the T2 hits 
    #             #

    #         # future metrics 
    #         future_hit_count = self._cum_t2_hit_count[self._last_window_end_ts]-self._cum_t2_hit_count[mt_key]
    #         future_read_miss = self._cum_t2_miss_count[self._last_window_end_ts]-self._cum_t2_miss_count[mt_key]
    #         future_write = self._cum_write_count[self._last_window_end_ts]-self._cum_write_count[mt_key]
    #         future_write_processed = self._cum_write_count[self._last_window_end_ts]-self._cum_write_count[mt_key]

    #         # if there are future misses or writes then only we have future hit miss byte ratio 
    #         if future_read_miss == 0 and future_write == 0:
    #             future_hit_miss_byte_ratio = 0 
    #             future_hit_miss_byte_ratio_per_ms = 0 
    #             percent_diff_hit_miss_byte_ratio_per_ms = 0
    #         else:
    #             future_hit_miss_byte_ratio = (future_hit_count*4096)/((future_read_miss*4096)+future_write)
    #             future_hit_miss_byte_ratio_per_ms = 100*future_hit_miss_byte_ratio/(max(mt_keys) - mt_key)
            
    #         # if the current hit miss byte ratio is not 0, then we can try to compute the percent diff with future
    #         if cur_hit_miss_byte_ratio_per_ms > 0:
    #             percent_diff_hit_miss_byte_ratio_per_ms = 100*(future_hit_miss_byte_ratio_per_ms-cur_hit_miss_byte_ratio_per_ms)/cur_hit_miss_byte_ratio_per_ms
    #             bandwidth_predicted = percent_diff_hit_miss_byte_ratio_per_ms/100
                

    #         print("Bandwidth:{:.4f},{:.4f}%, T2 hit: {}, HMR: {:.6f},{:.6f}, FUTURE HMR: {:.4f},{:.6f}".format(cur_bandwidth_diff, 
    #                                                     cur_bandwidth_diff_percent,
    #                                                     self._cum_t2_hit_count[mt_key],
    #                                                     cur_hit_miss_byte_ratio, 
    #                                                     cur_hit_miss_byte_ratio_per_ms,
    #                                                     future_hit_miss_byte_ratio,
    #                                                     future_hit_miss_byte_ratio_per_ms))


    #         # future_bandwidth_diff = percent_diff_hit_miss_byte_ratio_per_ms
    #         delta_byte = self._st.get_bytes_processed_at_T(st_key) - \
    #                         self._mt.get_bytes_processed_at_T(mt_key)

    #         """ How to use HMR? 
    #         - When there is no T2 hit, HMR is 0. At this point, we only 
    #             have overhead and it should tell us what kind of overhead per 
    #             read miss or write to expect. 
    #         - When T2 hit > 0, HMR > 0. We have some value of HMR and how much byte 
    #             overhead we have seen. If HMR improves are we saying performance will 
    #             also improve? If so, by how much? 
    #             - Percent change in HMR and percent change in bandwidth per window can 
    #                 be plotted to see if there is correlation? 
            
    #         """
            
    #         if (self._cum_t2_miss_count[mt_key]+self._cum_write_count[mt_key]) == 0:
    #             hmr = 0 
    #             hmr2 = 0 
    #         else:
    #             hmr = self._cum_t2_hit_count[mt_key]/(self._cum_t2_miss_count[mt_key]+self._cum_write_count[mt_key])
    #             hmr2 = (self._cum_t2_hit_count[mt_key]*4096)/((self._cum_t2_miss_count[mt_key]*4096)+self._cum_write_requested_byte[mt_key])



    #         if future_read_miss + future_write == 0:
    #             future_hmr = hmr 
    #             future_hmr2 = hmr2
    #         else:
    #             future_hmr = future_hit_count/(future_read_miss+future_write)
    #             future_hmr2 = (future_hit_count*4096)/((future_read_miss*4096) + future_write_processed)

    #         if hmr > 0: 
    #             diff =(future_hmr - hmr)/hmr 
    #             diff2 = (future_hmr2 - hmr2)/hmr2
    #         else:
    #             diff = 0 
    #             diff2 = 0 

    #         future_delta_byte = diff*delta_byte
    #         future_delta_byte2 = diff2*delta_byte
    #         delta_byte_diff = future_delta_byte - delta_byte
    #         delta_byte_diff2 = future_delta_byte2 - delta_byte

    #         # if delta_byte_diff > 0 and self._cum_t2_hit_count[mt_key]>0:
    #         #     print("T={},found,ReqCount={},Diff={},Final={},Pred={},{}".format(mt_key,self._cum_block_req_completed[mt_key],delta_byte_diff/(1024*1024), bandwidth_diff_percent, delta_byte_diff, delta_byte_diff2))
    #         # else:
    #         #     print("T={},notfound,ReqCount={},Diff={},Final={},Pred={},{}".format(mt_key,self._cum_block_req_completed[mt_key],delta_byte_diff/(1024*1024), bandwidth_diff_percent, delta_byte_diff, delta_byte_diff2))
    #     print("Bandwidth:{:.4f},{:.4f}%".format(bandwidth_diff, bandwidth_diff_percent))
                

            


            





