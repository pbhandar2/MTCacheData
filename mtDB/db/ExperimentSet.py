from mtDB.db.ExperimentOutput import ExperimentOutput
import numpy as np 

class ExperimentSet:
    def __init__(self,
                    key_features=["inputQueueSize", "processorThreadCount", "scaleIAT", "cacheSizeMB", "nvmCacheSizeMB"]):
        self.size = 0 
        self.data = []
        self.key_features = key_features 


    def add(self, entry):
        self.data.append(entry)


    def get_metric(self, metric_name, method="avg"):
        metric_list = []
        for entry in self.data:
            if not entry.metric_check(metric_name):
                return np.inf 
            else:
                metric_list.append(entry.get_metric(metric_name))
        
        if method == "avg":
            return np.array(metric_list, dtype=float).mean()

    
    def get_row(self):
        # get the mean of the bandwidth 
        pass
        

        


    
