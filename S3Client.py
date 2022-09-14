import boto3 
import pathlib 
import logging
from botocore.exceptions import ClientError


class S3Client:
    def __init__(self, ACCESS_KEY, SECRET_KEY):
        self.s3 = boto3.client("s3",
                    aws_access_key_id=ACCESS_KEY,
                    aws_secret_access_key=SECRET_KEY)
        self.bucket_name = "mtcachedata"
        self.key_prefix = "output_dump"
        self.data_dir = pathlib.Path("./output_dump")
        self.data = self.get_keys()
        assert self.data_dir.exists()


    def get_keys(self):
        paginator = self.s3.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=self.bucket_name, Prefix=self.key_prefix)
        key_list = []
        for page in pages:
            for obj in page['Contents']:
                key_list.append(obj)
        
        return key_list


    def get_key_count(self, key):
        return self.s3.list_objects_v2(Bucket=self.bucket_name, Prefix=key)['KeyCount']


    def download(self, key, file_path):
        print("Downloading key {} to {}".format(key, file_path))
        try:
            self.s3.download_file(self.bucket_name, key, file_path)
        except ClientError as e:
            logging.error("Error: {} in download".format(e))


    def sync(self):
        update_count = 0 
        # sync the local directory to data in S3 bucket 
        for entry in self.data:
            key = entry['Key']
            size = entry['Size']
            split_key = key.split("/")
            data_path = self.data_dir.joinpath("/".join(split_key[1:]))
            if data_path.parent.exists():
                if not data_path.exists():
                    print("Key {} in path {} does not exist!".format(key, data_path))
                    self.download(key, str(data_path.resolve()))
                    update_count += 1
                else:
                    print("This size of value at key {} changed".format(key))
                    file_size = data_path.stat().st_size
                    if file_size < size:
                        self.download(key, str(data_path.resolve()))
            else:
                data_path.parent.mkdir(parents=True, exist_ok=True)
                self.download(key, str(data_path.resolve()))

        print("Updated: {}".format(update_count))


    def delete(self, key):
        # delete a key from S3 bucket 
        pass 


    def sanity_check(self):
        # make sure that each experiment output is complete 
        pass

