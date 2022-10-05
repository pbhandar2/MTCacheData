import pathlib 
from mtDB.db.MTDB import MTDB

OUTPUT_DIR = pathlib.Path.home().joinpath("plots", "t2_eval")
OUTPUT_DIR.mkdir(exist_ok=True)
DATA_DIR = pathlib.Path.home().joinpath("mtdata")

if __name__ == "__main__":
    database = MTDB(DATA_DIR, eval="best")
    database.plot_overhead_vs_bandwidth(OUTPUT_DIR)

    print()