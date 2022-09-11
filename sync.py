import argparse 
from S3Client import S3Client


def sync_s3(args):
    s3 = S3Client(args.aws_access, args.aws_secret)
    s3.sync()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Sync folders from S3 to local.")
    parser.add_argument("aws_access", help="AWS access key")
    parser.add_argument("aws_secret", help="AWS secret key")
    args = parser.parse_args()

    sync_s3(args)

