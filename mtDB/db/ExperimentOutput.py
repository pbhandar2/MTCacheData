from multiprocessing.sharedctypes import Value
import pathlib 
import numpy as np 

""" The class reads an experiment output file and loads the 
    metrics for analysis. """
class ExperimentOutput:
    def __init__(self, experiment_output_path):
        self._output_path = pathlib.Path(experiment_output_path)
        self._iteration_count = int(self._output_path.stem.split("_")[-1])

        # overall stats
        self.stat = {}

        # time series stats
        self.ts_stat = {}       

        # cache parameters 
        self.nvm_cache_size_mb = 0 
        self.ram_cache_size_mb = 0 
        self.ram_alloc_size_byte = 0
        self.page_size_byte = 0 
        self.input_queue_size = 0
        self.processor_thread_count = 0 
        self.iat_scale_factor = 0 
        self.t2_hit_start = -1
        self.tag = "unknown"

        # flag indicating whether the output is complete 
        self.full_output = True

        # read the file and load metrics 
        self._load()


    def _load(self):
        # load the experiment output to the class 
        with open(self._output_path) as f:
            line = f.readline()
            while line:
                line = line.rstrip()
                split_line = line.split("=")
                # a JSON string cannot have the '=' character without it being in a string with a quote '"'
                # in case there is a JSON property with an '=' in it 
                if len(split_line) == 2 and '"' not in line:
                    # these are performance metrics from CacheBench with format (*metric_name*=*metric_value*)
                    metric_name = split_line[0]
                    self.stat[metric_name] = float(split_line[1]) 
                elif 'stat:' in line:
                    # load the snapshot of stats at different points in time 
                    # line containing snapshot of stat at a specific time starts with stat: 
                    # then *metric_name*=*metric_value*, *metric_name*=*metric_value* ... 
                    temp_line = line.replace("stat:", "")
                    metric_str_list = temp_line.split(",")
                    stat_snapshot = {}
                    for metric_str in metric_str_list:
                        split_metric_str = metric_str.split("=") 
                        if len(split_metric_str) == 2:
                            metric_name = split_metric_str[0]
                            metric_val = int(float(split_metric_str[1]))
                            stat_snapshot[metric_name] = metric_val
                    
                    if "t2HitRate" not in stat_snapshot:
                        self.full_output = True
                    else: 
                        if self.t2_hit_start == -1 and stat_snapshot["t2HitRate"] > 0:
                            self.t2_hit_start = stat_snapshot["T"]
                            
                    self.ts_stat[int(stat_snapshot["T"])] = stat_snapshot
                else:
                    # these are configuration parameters stored as JSON string in the output file 
                    if "nvmCacheSizeMB" in line:
                        split_line = line.split(":")
                        self.nvm_cache_size_mb = int(split_line[1].replace(",", ""))

                    if "cacheSizeMB" in line:
                        split_line = line.split(":")
                        self.ram_cache_size_mb = int(split_line[1].replace(",", ""))
                        self.stat["cacheSizeMB"] = self.ram_cache_size_mb

                    if "allocSizes" in line:
                        self.ram_alloc_size_byte = int(f.readline().rstrip())
                        self.stat["t1AllocSize"] = self.ram_alloc_size_byte

                    if "pageSizeBytes" in line:
                        split_line = line.split(":")
                        self.page_size_byte = int(split_line[1].replace(",", ""))
                        self.stat["pageSizeBytes"] = self.page_size_byte

                    if "inputQueueSize" in line:
                        split_line = line.split(":")
                        self.input_queue_size = int(split_line[1].replace(",", ""))
                        self.stat["inputQueueSize"] = self.input_queue_size

                    if "processorThreadCount" in line:
                        split_line = line.split(":")
                        self.processor_thread_count = int(split_line[1].replace(",", ""))
                        self.stat["processorThreadCount"] = self.processor_thread_count

                    if "scaleIAT" in line:
                        split_line = line.split(":")
                        self.iat_scale_factor = int(split_line[1].replace(",", ""))
                        self.stat["scaleIAT"] = self.iat_scale_factor 
                    
                    if "tag" in line:
                        split_line = line.split(":")
                        self.tag = split_line[1].replace(",", "")

                line = f.readline()
            
            self.stat["nvmCacheSizeMB"] = self.nvm_cache_size_mb
            if self.input_queue_size == 0 or self.iat_scale_factor == 0 or self.processor_thread_count == 0:
                raise ValueError("Some cache parameter missing from file {}".format(self._output_path))

        # does the output have all the performance stats? Is it complete? 
        if self.is_output_complete():
            self.full_output = True
            self.stat["t2HitCount"] = self.get_t2_hit_count()


    def get_bytes_processed_at_T(self, T):
        return self.ts_stat[T]["readIOProcessed"] + self.ts_stat[T]["writeIOProcessed"]


    def get_read_io_processed_at_T(self, T):
        return self.ts_stat[T]["readIOProcessed"]


    def get_write_io_processed_at_T(self, T):
        return self.ts_stat[T]["writeIOProcessed"]


    

    def get_config_key(self):
        return "{}_{}_{}_{}".format(self.input_queue_size,
                                    self.processor_thread_count,
                                    self.iat_scale_factor,
                                    self.stat["cacheSizeMB"])

    
    def get_read_io_processed(self):
        return self.ts_stat[max(self.ts_stat.keys())]["readIOProcessed"]


    def get_write_io_processed(self):
        return self.ts_stat[max(self.ts_stat.keys())]["writeIOProcessed"]
    

    def get_ts_bandwidth(self):
        pass 


    def get_bandwidth(self):
        bandwidth_key = "bandwidth_byte/s"
        if bandwidth_key not in self.stat:
            print(self.stat)
            raise ValueError("No key {} in stat".format(bandwidth_key))
        return self.stat[bandwidth_key]


    def get_total_estimate_from_percentile(self):
        pass

            
    def get_runtime(self):
        return self.stat["experimentTime_s"]


    def metric_check(self, metric_name):
        return metric_name in self.stat


    def get_metric(self, metric_name):
        if not self.metric_check(metric_name):
            return np.inf 
        return self.stat[metric_name]


    def is_output_complete(self):
        return "t2WriteLat_p100_us" in self.stat and "t2GetCount" in self.stat and "inputQueueSize" in self.stat and "bandwidth_byte/s" in self.stat and self.full_output


    def get_block_req_per_second(self):
        return self.stat["blockReqCount"]/self.stat["experimentTime_s"]


    def get_nvm_size(self):
        return self.nvm_cache_size_mb

    
    def get_ram_size(self):
        return self.ram_cache_size_mb


    def get_ram_size_mb(self):
        return (self.stat["t1Size"]*self.ram_alloc_size_byte)/1e6


    def get_nvm_size_mb(self):
        return (self.stat["t2Size"]*self.page_size_byte)/1e6


    def get_ram_size(self):
        return self.ram_cache_size_mb


    def get_t1_hit_rate(self):
        return self.stat["t1HitRate"]


    def get_t2_hit_rate(self):
        return self.stat["t2HitRate"]


    def get_percentile_read_slat(self, percentile_str):
        return self.stat["blockReadSlat_{}_ns".format(percentile_str)]


    def get_percentile_write_slat(self, percentile_str):
        return self.stat["blockWriteSlat_{}_ns".format(percentile_str)]

    
    def get_mean_block_req_per_second(self):
        return self.stat["blockReqCount"]/self.stat["experimentTime_s"]


    def get_nvm_usage(self):
        # TODO: get the NVM usage 
        nvm_usage = 0.0 
        if self.nvm_cache_size_mb > 0:
            t2_count = self.stat["t2Size"]
            t2_size_mb = t2_count * 4136/1e6
            nvm_usage = 100.0*t2_size_mb/self.nvm_cache_size_mb
        return nvm_usage


    def get_t2_hit_count(self):
        return self.stat['t2GetCount'] * self.get_t2_hit_rate()/100.0


    def get_overhead(self):
        # pass 
        pass 


    def get_hmrc_1(self, page_size=4096):
        t2_hit_count = self.get_t2_hit_count()
        t2_miss_count = self.stat['t2GetCount'] - t2_hit_count
        t2_hit_bytes = t2_hit_count * page_size 
        t2_miss_bytes = t2_miss_count * page_size 
        write_bytes = self.stat["backingWriteIORequested_byte"]
        return t2_hit_bytes/(t2_miss_bytes+write_bytes)


    def get_hmrc_2(self, page_size=4096):
        t2_hit_count = self.get_t2_hit_count()


    def metric_1(self, st_backing_read_lat, st_backing_write_lat):
        """ 

            tier-1 read hit 
                - increase in find and alloc latency due to T2
                    - lat(find_mt) + lat(alloc_mt) - lat(find_st) - lat(alloc_st)

            tier-2 read hit
                - reduction in overall latency due to a tier-2 cache hit instead of a cache miss in ST
                    - lat(backing_read_st) - lat(tier2_read)
                - increase in find latency due to T2
                    - lat(find)

            read miss
                - reduction in backing store read latency due to reduced pressure
                    - lat(backing_read_st) - lat(backing_read_mt)
                - increase in find and alloc latency due to T2
                    - lat(find) + lat(alloc)

            write 
                - reduction in backing store write latency due to reduced pressure 
                    - lat(backing_write_st) - lat(backing_write_mt)
                - increase in alloc latency due to T2
                    - lat(alloc)

            If we have the count and latency differences of each scenario, will it tell us 
            whether adding a cache tier would help or not? 

            Even if it does, we would need to know how much the latency of the hard drive 
            would decrease if a second tier of cache is added. 
                - Can we derive it from a few simulations? 
                    - Run 25,50,75 hit rates and see how backing store performance changes 
                        and then interpolate. 
    
        """
        pass 


    def get_row(self):
        self.stat["hmrc1"] = self.get_hmrc_1()
        self.stat["t2HitCount"] = self.get_t2_hit_count()
        return self.stat 


    def __str__(self):
        repr_str = "ExperimentOutput:Size[T1/T2]={},{}, HR(%)[T1/T2]={:3.1f},{:3.1f}, MEANSLAT(ms)[R/W]={:3.2f},{:3.2f},{:3.2f},{:3.2f} BANDWIDTH={:3.2f}".format(
                                self.get_ram_size(), 
                                self.get_nvm_size(), 
                                self.get_t1_hit_rate(), 
                                self.get_t2_hit_rate(),
                                self.get_percentile_read_slat("avg")/1e6,
                                self.get_percentile_write_slat("avg")/1e6,
                                self.get_percentile_read_slat("p99")/1e6,
                                self.get_percentile_write_slat("p99")/1e6,
                                self.get_bandwidth()/1e6)
        return repr_str


    def plot_ts(self, metric_name):
        pass 