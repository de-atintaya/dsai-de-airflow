import pandas as pd
import boto3
import logging
import os
from io import StringIO


def leer_datos_s3() -> pd.DataFrame:
  s3_client = None

  try:
    aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
    aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')
    aws_region = os.getenv('AWS_REGION', 'us-east-1')
    bucket_name = os.getenv('S3_BUCKET_NAME')
    file_key = os.getenv('S3_FILE_KEY')

    if not aws_access_key_id:
      raise ValueError("AWS_ACCESS_KEY_ID environment variable is required")
    if not aws_secret_access_key:
      raise ValueError(
          "AWS_SECRET_ACCESS_KEY environment variable is required")
    if not bucket_name:
      raise ValueError("S3_BUCKET_NAME environment variable is required")
    if not file_key:
      raise ValueError("S3_FILE_KEY environment variable is required")

    logging.info(f"Connecting to AWS S3 in region: {aws_region}")
    s3_client = boto3.client(
        's3',
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name=aws_region
    )

    s3_client.head_bucket(Bucket=bucket_name)
    logging.info(f"Successfully connected to S3 bucket: {bucket_name}")

    logging.info(f"Downloading file: s3://{bucket_name}/{file_key}")

    response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
    csv_content = response['Body'].read().decode('utf-8')

    df = pd.read_csv(StringIO(csv_content))

    logging.info(f"Successfully extracted {len(df)} records from S3")
    logging.info(f"Columns: {list(df.columns)}")
    logging.info(f"DataFrame shape: {df.shape}")

    return df

  except Exception as e:
    logging.error(f"Error extracting data from S3: {str(e)}")
    raise
