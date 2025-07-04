import boto3
import os

from dotenv import load_dotenv
load_dotenv()

from src.constants import AWS_ACCESS_KEY_ID_ENV_KEY, AWS_SECRET_ACCESS_KEY_ENV_KEY, REGION_NAME


# Clients provide a low-level interface to AWS whose methods map close to 1:1 with service APIs. 
# Resources represent an object-oriented interface to Amazon Web Services (AWS)
class S3Client:
    s3_client = None
    s3_resource = None

    def __init__(self, region_name=REGION_NAME):
        """ creates an connection with s3 bucket """
        if S3Client.s3_client==None or S3Client.s3_resource==None:
            access_key_id = os.getenv(AWS_ACCESS_KEY_ID_ENV_KEY)
            secret_access_key = os.getenv(AWS_SECRET_ACCESS_KEY_ENV_KEY)
            region_name = REGION_NAME

            if access_key_id is None:
                raise Exception("Environment variable: AWS_ACCESS_KEY_ID_ENV_KEY is not not set.")
            if secret_access_key is None:
                raise Exception("Environment variable: AWS_SECRET_ACCESS_KEY_ENV_KEY is not set.")
            
            S3Client.s3_client = boto3.resource("s3",
                                                access_key_id = access_key_id,
                                                secret_access_key = secret_access_key,
                                                region_name = region_name)
            
            S3Client.s3_resource = boto3.client("s3",
                                                access_key_id = access_key_id,
                                                secret_access_key = secret_access_key,
                                                region_name = region_name)
            
            self.s3_client = S3Client.s3_client
            self.s3_resource = S3Client.s3_resource