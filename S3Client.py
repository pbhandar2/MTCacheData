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
        self.key_prefix = "output/"
        self.data_dir = pathlib.Path("./data")
        assert self.data_dir.exists()


    def get_keys(self):
        response = self.s3.list_objects_v2(Bucket=self.bucket_name, Prefix=self.key_prefix)
        return response["Contents"]


    def get_key_count(self, key):
        return self.s3.list_objects_v2(Bucket=self.bucket_name, Prefix=key)['KeyCount']


    def download(self, key, file_path):
        print("Downloading key {} to {}".format(key, file_path))
        try:
            self.s3.download_file(self.bucket_name, key, file_path)
        except ClientError as e:
            logging.error("Error: {} in download".format(e))


    def sync(self):
        # sync the local directory to data in S3 bucket 
        print("Sync")
        for entry in self.data['Contents']:
            key = entry['Key']
            split_key = key.split("/")
            if len(split_key) > 4:
                data_path = self.data_dir.joinpath("/".join(split_key[1:]))
                if data_path.parent.exists():
                    if not data_path.exists():
                        self.download(key, str(data_path.resolve()))
                    else:
                        # print("Key {} in path {} exists!".format(key, data_path))
                        pass 
                else:
                    data_path.parent.mkdir(parents=True, exist_ok=True)
                    self.download(key, str(data_path.resolve()))


    def delete(self, key):
        # delete a key from S3 bucket 
        pass 


    def sanity_check(self):
        # make sure that each experiment output is complete 
        pass

