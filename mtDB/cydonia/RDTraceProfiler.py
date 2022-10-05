import pathlib 
import pandas as pd 
from collections import defaultdict, Counter 


class RDTraceProfiler:
    def __init__(self, rd_trace_path, t1_size, t2_size, window_list):
        self._rd_trace_path = pathlib.Path(rd_trace_path)
        self._handle = self._rd_trace_path.open("r")
        self._t1_size = t1_size 
        self._t2_size = t2_size 
        self._window_list = window_list 
        self._window_count = len(window_list)

        self.df = pd.DataFrame()
        self._profile()


    def _get_hit_info_from_counter(self, counter, max_rd):
        t1_hit_count, t2_hit_count, miss_count = 0, 0, counter[-1]
        if max_rd > -1:
            for rd in range(max_rd+1):
                if rd < self._t1_size:
                    t1_hit_count += counter[rd]
                elif rd >= self._t1_size and rd < (self._t1_size+self._t2_size):
                    t2_hit_count += counter[rd]
                else:
                    miss_count += counter[rd]
        return t1_hit_count, t2_hit_count, miss_count


    def _profile(self):
        window_read_rd_counter, read_rd_counter = Counter(), Counter()
        block_req_count, window_index, window_write_count, write_count = 0, 0, 0, 0 
        window_max_rd, max_rd, prev_block_req_ts = -1, -1, -1 
        window_stat_list = [] 
        block_req_count_at_window_end = self._window_list[window_index]

        self._handle.seek(0)
        line = self._handle.readline()
        line_index = 0 

        while line:
            split_line = line.rstrip().split(",")
            rd, op, cur_block_req_ts = int(split_line[0]), split_line[1], int(split_line[2])

            if op == "r":
                if rd > window_max_rd:
                    window_max_rd = rd 
                if rd > max_rd:
                    max_rd = rd 
                window_read_rd_counter[rd] += 1
                read_rd_counter[rd] += 1
            else:
                window_write_count += 1
                write_count += 1

            if cur_block_req_ts != prev_block_req_ts:
                # new block request!
                # cache request / page request belonging to the same block request 
                # have the same timestamp 
                block_req_count += 1
                prev_block_req_ts = cur_block_req_ts

            if block_req_count == block_req_count_at_window_end:
                # window ends, start new window 
                window_index += 1
                window_t1_hit_count, window_t2_hit_count, window_miss_count = self._get_hit_info_from_counter(window_read_rd_counter, window_max_rd)
                t1_hit_count, t2_hit_count, miss_count = self._get_hit_info_from_counter(read_rd_counter, max_rd)

                window_stat = {
                    "t1_hit_count": window_t1_hit_count,
                    "t2_hit_count": window_t2_hit_count,
                    "miss_count": window_miss_count,
                    "write_count": window_write_count,
                    "cum_t1_hit_count": t1_hit_count,
                    "cum_t2_hit_count": t2_hit_count,
                    "cum_miss_count": miss_count,
                    "cum_write_count": write_count,
                    "end_block_req_ts": cur_block_req_ts,
                    "cum_t2_hit_rate": t2_hit_count, 
                    "block_req_count_at_window_end": block_req_count_at_window_end
                }
                window_stat_list.append(window_stat)
                
                window_max_rd = -1 
                window_write_count = 0 
                window_read_rd_counter = Counter()

                if window_index == len(self._window_list):
                    break 

                block_req_count_at_window_end = self._window_list[window_index]

            prev_ts = cur_block_req_ts 
            line = self._handle.readline()
            line_index += 1
        
        self.df = pd.DataFrame(window_stat_list)
        self.len = len(self.df)

        self.df["hmr"] = self.df["t2_hit_count"]/(self.df["write_count"] + self.df["miss_count"])
        self.df["hmr2"] = self.df["t2_hit_count"]/(self.df["write_count"] + self.df["miss_count"] + self.df["t1_hit_count"])
        self.df["hmr3"] = (self.df["t2_hit_count"]+self.df["write_count"])/(self.df["miss_count"] + self.df["t1_hit_count"])
        self.df["cum_hmr"] = self.df["cum_t2_hit_count"]/(self.df["cum_write_count"] + self.df["cum_miss_count"])
        self.df["cum_hmr2"] = self.df["cum_t2_hit_count"]/(self.df["cum_write_count"] + self.df["cum_miss_count"] + self.df["cum_t1_hit_count"])
        self.df["future_t1_hit_count"] = self.df.iloc[-1]["cum_t1_hit_count"] - self.df["cum_t1_hit_count"]
        self.df["future_t2_hit_count"] = self.df.iloc[-1]["cum_t2_hit_count"] - self.df["cum_t2_hit_count"]
        self.df["future_miss_count"] = self.df.iloc[-1]["cum_miss_count"] - self.df["cum_miss_count"]
        self.df["future_write_count"] = self.df["cum_write_count"].iloc[self.len-1] - self.df["cum_write_count"]
        self.df["future_hmr"] = self.df["future_t2_hit_count"]/(self.df["future_write_count"] + self.df["future_miss_count"])
        self.df["future_hmr2"] = self.df["future_t2_hit_count"]/(self.df["future_write_count"] + self.df["future_miss_count"] + self.df["future_t1_hit_count"])
        self.df["trace_ts_len"] = (self.df["end_block_req_ts"].shift(-1)-self.df["end_block_req_ts"])/1e6
