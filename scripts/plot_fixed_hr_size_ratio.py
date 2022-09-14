from mtDB.db.MTDB import MTDB

import matplotlib.pyplot as plt
plt.rcParams.update({'font.size': 25})

""" This scripts plots the percentage improvement from adding a cache tier 
    for various ratio of tier-1 and tier-2 cache sizes and ratio of tier-1 
    and tier-2 hit rate. 
"""

class Plotter:
    def __init__(self, df):
        self.df = df 


    def run(self):
        self.df["mt_opt"] = self.df["bandwidth_byte/s"] <= 0

        # bandwidth 
        bandwidth_list = self.df["bandwidth_byte/s"].to_list()
        t1_t2_size_ratio = self.df["t1_t2_size_ratio"]
        t1_t2_hr_ratio = self.df["t1_t2_hr_ratio"]

        self.plot(t1_t2_size_ratio, 
                    bandwidth_list, 
                    "size_band.png", 
                    "(Tier-1 size)/(Tier-2 size)")
        
        self.plot(t1_t2_hr_ratio, 
                    bandwidth_list, 
                    "hr_band.png", 
                    "(Tier-1 hit rate)/(Tier-2 hit rate)", 
                    ylog=True)

    
    def plot(self, t1_t2_size_ratio, bandwidth_list, file_path, y_label, ylog=False):
        fig, ax = plt.subplots(figsize=[14, 10])
        
        ax.scatter(bandwidth_list, t1_t2_size_ratio, s=80, alpha=0.6)
        ax.set_xlabel("Percent change in bandwidth due to tier-2 cache")
        ax.set_ylabel(y_label)

        if ylog:
            ax.set_yscale("log")
            ax.set_ylabel("log({})".format(y_label))

        plt.tight_layout()
        plt.savefig(file_path)
        plt.close()


if __name__ == "__main__":
    database = MTDB("/home/pranav/MTCacheData/output_dump")
    combined_df = database._get_combined_df()

    plotter = Plotter(combined_df)
    plotter.run()