""" The class reads a experiment output file and loads the 
    metrics for analysis. 
"""

import pathlib 

class ExperimentOutput:
    def __init__(self, experiment_output_path):
        self.stat = {}
        self.nvm_cache_size_mb = 0 
        self.ram_cache_size_mb = 0 
        self.ram_alloc_size_byte = 0
        self.page_size_byte = 0 
        self.input_queue_size = 0
        self.processor_thread_count = 0 
        self.iat_scale_factor = 0 
        self.full_output = False 

        self._output_path = pathlib.Path(experiment_output_path)
        self._load()


    def _load(self):
        with open(self._output_path) as f:
            line = f.readline()
            while line:
                line = line.rstrip()
                split_line = line.split("=")
                if len(split_line) == 2:
                    metric_name = split_line[0]
                    self.stat[metric_name] = float(split_line[1]) 
                else:
                    if "nvmCacheSizeMB" in line:
                        split_line = line.split(":")
                        self.nvm_cache_size_mb = int(split_line[1].replace(",", ""))

                    if "cacheSizeMB" in line:
                        split_line = line.split(":")
                        self.ram_cache_size_mb = int(split_line[1].replace(",", ""))

                    if "allocSizes" in line:
                        self.ram_alloc_size_byte = int(f.readline().rstrip())

                    if "pageSizeBytes" in line:
                        split_line = line.split(":")
                        self.page_size_byte = int(split_line[1].replace(",", ""))

                    if "inputQueueSize" in line:
                        split_line = line.split(":")
                        self.input_queue_size = int(split_line[1].replace(",", ""))

                    if "processorThreadCount" in line:
                        split_line = line.split(":")
                        self.processor_thread_count = int(split_line[1].replace(",", ""))

                    if "scaleIAT" in line:
                        split_line = line.split(":")
                        self.iat_scale_factor = int(split_line[1].replace(",", ""))

                line = f.readline()
        
        if self.base_sanity_check():
            self.full_output = True


    def get_runtime(self):
        return self.stat["experimentTime_s"]


    def is_output_complete(self):
        return "t2WriteLat_p100_us" in self.stat


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


    def get_bandwidth(self):
        return self.stat["bandwidth_byte/s"]

    
    def get_mean_block_req_per_second(self):
        return self.stat["blockReqCount"]/self.stat["experimentTime_s"]


    def get_nvm_usage(self):
        nvm_usage = 0.0 
        if self.nvm_cache_size_mb > 0:
            t2_count = self.stat["t2Size"]
            t2_size_mb = t2_count * 4136/1e6
            nvm_usage = 100.0*t2_size_mb/self.nvm_cache_size_mb
        return nvm_usage


    def get_hmrc_1(self, page_size=4096):
        # divide t2 hits by t2 miss + writes 
        t1_miss = 100 - self.get_t1_hit_rate()
        get_count = self.stat["t1GetCount"]
        t2_hit_count = self.stat["t2GetCount"]*self.get_t2_hit_rate()/100
        t2_miss_count = self.stat["t2GetCount"] - t2_hit_count
        t2_hit_bytes = t2_hit_count * page_size 
        t2_miss_bytes = t2_miss_count * page_size 
        write_bytes = self.stat["backingWriteIORequested_byte"]
        return t2_hit_bytes/(t2_miss_bytes+write_bytes)


    def get_row(self):
        self.stat["cacheSizeMB"] = self.ram_cache_size_mb
        self.stat["nvmCacheSizeMB"] = self.nvm_cache_size_mb
        self.stat["t1AllocSize"] = self.ram_alloc_size_byte
        self.stat["pageSizeBytes"] = self.page_size_byte
        self.stat["inputQueueSize"] = self.input_queue_size
        self.stat["processorThreadCount"] = self.processor_thread_count
        self.stat["scaleIAT"] = self.iat_scale_factor 
        return self.stat 

    
    def base_sanity_check(self):
        return "blockReqCount" in self.stat and "blockWriteSlat_avg_ns" in self.stat and "blockReadSlat_avg_ns" in self.stat 


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
