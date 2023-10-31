import boto3
from tqdm import tqdm
from botocore.exceptions import ClientError
import os
import logging

def upload_file(file_name, bucket, object_name=None):
    """Upload a file to an S3 bucket

    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = os.path.basename(file_name)
    file_size = os.stat(file_name).st_size
    # Upload the file
    s3_client = boto3.client('s3')
    try:
        with tqdm(total=file_size, unit="B", unit_scale=True, desc=file_name) as pbar:
            s3_client.upload_file(
                file_name,
                bucket,
                object_name,
                Callback=lambda bytes_transferred: pbar.update(bytes_transferred),
            )
    except ClientError as e:
        logging.error(e)

# upload_file("financial_dataset.csv","financial-data-bucket")

def download_file(file_name,bucket,object_name):
    s3 = boto3.client('s3')
    kwargs = {"Bucket": bucket, "Key": object_name}
    object_size = s3.head_object(**kwargs)["ContentLength"]
    print(object_size)
    with tqdm(total=object_size, unit="B", unit_scale=True, desc=file_name) as pbar:
        s3.download_file(
            bucket,
            object_name,
            file_name,
            Callback=lambda bytes_transferred: pbar.update(bytes_transferred),
        )

# download_file("financial_dataset.csv","financial-data-bucket","financial_dataset.csv")