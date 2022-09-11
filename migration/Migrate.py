from UpdateOutput import UpdateOutput
import pathlib 
import os
import boto3 
import logging 
from botocore.exceptions import ClientError


OLD_DATA_DIR = pathlib.Path("../data")
NEW_DATA_DIR = pathlib.Path("../output_dump")
BUCKET_NAME = "mtcachedata"


""" Migrate old output dump files to a new 
    directory. """
class Migrate:
    def __init__(self,
                    old_data_dir=OLD_DATA_DIR,
                    new_data_dir=NEW_DATA_DIR,
                    bucket_name=BUCKET_NAME):
        self.old_data_dir = old_data_dir
        self.new_data_dir = new_data_dir
        self.bucket_name = bucket_name
        self.s3 = boto3.client("s3",
                                    aws_access_key_id=os.getenv('AWS_ACCESS_KEY'),
                                    aws_secret_access_key=os.getenv('AWS_SECRET_KEY'))


    def run(self):
        file_update_count = 0 
        for cur_path in self.old_data_dir.glob('**/*'):
            if cur_path.is_file():
                cur_file_name = cur_path.name 
                cur_workload_id = cur_path.parent.name 
                cur_machine_id = cur_path.parent.parent.name

                if cur_machine_id == "sgdp7":
                    new_machine_id = "sgdp7"
                elif "cloudlab" in cur_machine_id:
                    cur_tag = cur_machine_id.split("cloudlab")[1]
                    if cur_tag == '17' or cur_tag == '18' or cur_tag == '19':
                        new_machine_id = "r6525"
                    else:
                        new_machine_id = "c220g1"
                else:
                    raise ValueError("Machine id {} cannot be processed".format(cur_machine_id))

                new_data_path = self.new_data_dir.joinpath(new_machine_id, cur_workload_id, cur_file_name)
                if not new_data_path.exists():
                    new_key = "/".join(new_data_path.parts[1:])
                    new_data_path.parent.mkdir(exist_ok=True, parents=True)
                    new_data_path.touch()
                    output_update = UpdateOutput(cur_path, cur_machine_id)
                    output_update.write_to_file(new_data_path)
                    file_update_count += 1
                    try:
                        self.s3.upload_file(str(new_data_path), self.bucket_name, new_key)
                        logging.info("Key {} uploaded!".format(new_key))
                    except ClientError as e:
                        logging.error("Error: {} in upload".format(e))
                
                
        
        print("Update count: {}".format(file_update_count))


if __name__ == "__main__":
    migrate = Migrate()
    migrate.run()