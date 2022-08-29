import argparse 
import pathlib 
from DataSync import DataSync

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Sync folders from S3 to local.")
    parser.add_argument("--d",
                        type=pathlib.Path,
                        default=pathlib.Path("./data"),
                        help="Path to the directory to sync the S3 bucket")
    args = parser.parse_args()

    sync_obj = DataSync(args.d, "./config.json")
    sync_obj.sync(args.d)