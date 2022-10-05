import argparse 
import pathlib 

import pandas as pd 
pd.options.display.float_format = '{:,.2f}'.format

from mtDB.db.MTDB import MTDB

DATA_DIR = pathlib.Path.home().joinpath("mtdata")


""" This class prints the top-n best, worst 
    MT cache performance based on user parameters. 
"""
class TopN:
    def __init__(self, df):
        self.df = df 
        self.df["bandwidth_byte/s"] = self.df["bandwidth_byte/s"].round(2)
        self.df["cacheSizeMB"] = self.df["cacheSizeMB"].astype(int)


    def get_best_bandwidth(self, top_n, df):
        return df.nlargest(top_n, 'bandwidth_byte/s')


    def get_worst_bandwidth(self, top_n, df):
        return df.nsmallest(top_n, 'bandwidth_byte/s')

    
    def print_best_and_worst_bandwidth(self, top_n):
        print(self.df)
        # group by instance
        for group_tuple, df in self.df.groupby(["machine_id", 'workload_id', 'scaleIAT', "inputQueueSize", "processorThreadCount"]):
            best_df = self.get_best_bandwidth(top_n, df)
            worst_df = self.get_worst_bandwidth(top_n, df)

            # features_print = ['bandwidth_byte/s', 
            #                     'cacheSizeMB', 
            #                     'nvmCacheSizeMB',
            #                     'backingReadLat_avg_ns',
            #                     'backingWriteLat_avg_ns',
            #                     't2HitRate',
            #                     'machine_id',
            #                     'workload_id',
            #                     'allocLat_avg_ns',
            #                     'findLat_avg_ns',
            #                     'blockReadSlat_avg_ns',
            #                     'blockWriteSlat_avg_ns']

            features_print = ['bandwidth_byte/s', 
                                'cacheSizeMB',
                                'nvmCacheSizeMB',
                                'writeIORatio',
                                't1HitRate',
                                't2HitRate',
                                'blockReadSlat_avg_ns',
                                'blockWriteSlat_avg_ns',
                                'backingReadLat_avg_ns',
                                'backingWriteLat_avg_ns',
                                'og-gain',
                                'findLat_avg_ns',
                                'allocLat_avg_ns',
                                'backingReadSize_avg_byte',
                                'backingWriteSize_avg_byte',
                                "st_backingReadSize_avg_byte",
                                "st_backingWriteSize_avg_byte",
                                "iatWaitDuration_avg_us",
                                "writeReqRatio"]

            print("\n\n")
            print(df["cacheSizeMB"].unique())
            print(df["nvmCacheSizeMB"].unique())
            print(group_tuple)
            print(best_df[features_print].round(2).T)
            print("\n")
            print(worst_df[features_print].T)


if __name__ == "__main__":
    database = MTDB(DATA_DIR, eval="best")
    combined_df = database._get_combined_df()

    top_n = TopN(combined_df)
    top_n.print_best_and_worst_bandwidth(2)
    
