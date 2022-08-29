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
        try:
            self.s3.download_file(self.bucket_name, key, file_path)
        except ClientError as e:
            logging.error("Error: {} in download".format(e))