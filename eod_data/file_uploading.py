# Imports
import boto3

# Local Imports
from keys import aws_keys


def upload_to_s3(filename, bucket_name, folder):
    session = boto3.Session(
        aws_access_key_id=aws_keys["ACCESS_KEY"],
        aws_secret_access_key=aws_keys["SECRET_KEY"],
    )
    s3_client = session.client("s3")

    with open("/tmp/" + filename, "rb") as file:
        object = file.read()
        s3_client.put_object(
            Body=object,
            Bucket=bucket_name,
            Key=folder + "/" + filename,
        )