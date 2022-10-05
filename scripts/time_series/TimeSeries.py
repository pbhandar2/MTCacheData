import pathlib 
from mtDB.db.MTDB import MTDB

OUTPUT_DIR = pathlib.Path.home().joinpath("plots", "time_series")
DATA_DIR = pathlib.Path.home().joinpath("mtdata")

if __name__ == "__main__":
    database = MTDB(DATA_DIR, eval="best")
    database.plot_ts(
                ["overallBandwidth", 
                    "blockReadSLat_avg_ns", 
                    "blockWriteSLat_avg_ns", 
                    "blockReadSLat_p99_ns", 
                    "blockWriteSLat_p99_ns",
                    "blockReadSLat_p999_ns", 
                    "blockWriteSLat_p999_ns",
                    "backingReadLat_avg_ns",
                    "backingWriteLat_avg_ns",
                    "backingReadLat_p99_ns",
                    "backingWriteLat_p99_ns",
                    "t1HitRate",
                    "t2HitRate"], 
                OUTPUT_DIR)