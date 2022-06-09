from dotenv import load_dotenv
from datetime import datetime
import boto3
import os

load_dotenv()

# GENERAL CONFIG
AWS_ACCESS_KEY_ID=os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY=os.getenv('AWS_SECRET_ACCESS_KEY')
BUCKET_NAME=os.getenv('BUCKET_NAME')
MUSEUMS_URL=os.getenv('MUSEUMS_URL')
CINEMAS_URL=os.getenv('CINEMAS_URL')
LIBRARIES_URL=os.getenv('LIBRARIES_URL')
REDSHIFT_URL=os.getenv('REDSHIFT_URL')
RUN_DATE = datetime.today()
TEMP_DATA_DIR=os.getenv('TEMP_DATA_DIR')


# S3 Connection
s3 = boto3.resource(
    service_name='s3',
    region_name='us-east-1',
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY
)
