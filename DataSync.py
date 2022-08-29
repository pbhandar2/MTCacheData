import argparse 
import pathlib 
import json 
from S3Client import S3Client

class DataSync:
    def __init__(self, data_dir, config_file_path):
        self.data_dir = data_dir
        self.config_json = {}
        with open(config_file_path) as f:
            self.config_json = json.load(f)


    def get_file_path_from_key(self, key, output_dir):
        split_key = key.split("/")
        return output_dir.joinpath(*key.split("/")[1:])


    def sync(self, data_dir):
        s3 = S3Client(self.config_json["aws_access_key"], self.config_json["aws_secret"])
        key_list = s3.get_keys()
        download_count = 0 
        for key_obj in key_list:
            key_str = key_obj["Key"]
            if len(key_str.split("/")) > 2:
                local_file_path = self.get_file_path_from_key(key_str, data_dir)
                local_file_path.parents[0].mkdir(parents=True, exist_ok=True)
                if not local_file_path.exists():
                    s3.download(key_str, str(local_file_path.resolve()))
                    download_count += 1
            else:
                print("Key not valid. {}".format(key_str))
        
        print("log: downloaded {} files.".format(download_count))