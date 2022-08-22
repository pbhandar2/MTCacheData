import argparse 
import pathlib 

from MTDBUnit import MTDBUnit

def print_db_unit_stats(data_dir):
    unit = MTDBUnit(data_dir)
    unit.print_mt_count()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Display metric related to a workload in a particular machine")
    parser.add_argument("--d", 
                            default=pathlib.Path("data/cloudlab_a/w105"),
                            type=pathlib.Path, 
                            help="Directory containing experiment outputs")
    args = parser.parse_args()

    print_db_unit_stats(args.d)