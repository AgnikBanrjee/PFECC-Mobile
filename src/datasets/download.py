import os
import boto3
from dotenv import load_dotenv


def make_dir(dir_name):
    if not os.path.exists(os.path.dirname(dir_name)):
        os.makedirs(os.path.dirname(dir_name))


def download_s3_folder(bucket_name, s3_folder):
    base_dir = f"data/raw/{s3_folder}"
    make_dir(base_dir)
    bucket = s3_resource.Bucket(bucket_name)
    for obj in bucket.objects.filter(Prefix=s3_folder):
        target = os.path.join(base_dir, os.path.relpath(obj.key, s3_folder))
        if not os.path.exists(os.path.dirname(target)):
            os.makedirs(os.path.dirname(target))
        if obj.key[-1] == "/":
            continue
        bucket.download_file(obj.key, target)


def download_s3_file(bucket_name, file_name, download_dir):
    make_dir(download_dir)
    s3_client.download_file(bucket_name, file_name, f"{download_dir}/{file_name}")


if __name__ == "__main__":
    load_dotenv()
    bucket_name = os.getenv("S3_BUCKET_NAME")
    access_key_id = os.getenv("S3_ACCESS_KEY")
    secret_access_key = os.getenv("S3_SECRET_ACCESS_KEY")
    s3_resource = boto3.resource(
        "s3", aws_access_key_id=access_key_id, aws_secret_access_key=secret_access_key
    )
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=access_key_id,
        aws_secret_access_key=secret_access_key,
        region_name="us-west-2",
    )

    s3_folders = [
        "CAFE",
        "cohn-kanade_dataset",
        "fer_2013",
        "jaffe",
        "TIF",
        "NIMH-ChEFS Picture Set",
        "feph",
        "kdef-akdef",
        "dartmouth",
        "ExpW",
    ]
    for i in s3_folders:
        download_s3_folder(bucket_name, i)

    s3_file_names = [
        "RafD_zipped.zip",
        "NimStim.zip",
        "Tsinghua FED images.zip",
        "oulu.zip",
    ]
    download_dirs = [
        "data/raw/RaFD/",
        "data/raw/NimStim/",
        "data/raw/Tsinghua/",
        "data/raw/Oulu-CASIA/",
    ]
    for i in range(len(s3_file_names)):
        download_s3_file(bucket_name, s3_file_names[i], download_dirs[i])
