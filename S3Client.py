import boto3 
import logging 
import pathlib 
from botocore.exceptions import ClientError

class S3Client:
    def __init__(self, access_key, secret_key):
        self.s3 = boto3.client("s3",
                                    aws_access_key_id=access_key,
                                    aws_secret_access_key=secret_key)
        self.bucket_name = "mtcachedata"
        self.key_prefix = "output/"
        self.data_dir = pathlib.Path("./data")
        self.data = self.get_keys()
        assert self.data_dir.exists()


    def get_keys(self):
        # get a list of relevant keys in S3 bucket 
        list_api_return = self.s3.list_objects_v2(Bucket=self.bucket_name, Prefix=self.key_prefix)
        return list_api_return 
        

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

