import json
import boto3
import random
import string
import logging
import os
from botocore.exceptions import ClientError
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Initialize Logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Environment Variables
S3_BUCKET = os.getenv('S3_BUCKET_NAME')
S3_PREFIX = os.getenv('S3_SOURCE_PREFIX')
KINESIS_STREAM_NAME = os.getenv('KINESIS_STREAM_NAME')

def list_json_objects(bucket_name: str, prefix: str = '') -> list:
    """
    List all JSON objects in an S3 bucket with the given prefix.

    Args:
        bucket_name (str): Name of the S3 bucket.
        prefix (str): Prefix to filter objects.

    Returns:
        list: List of JSON object keys.
    """
    try:
        s3_client = boto3.client('s3')
        json_files = []
        params = {'Bucket': bucket_name, 'Prefix': prefix}

        while True:
            response = s3_client.list_objects_v2(**params)
            logger.info(f'Fetched {len(response.get("Contents", []))} objects from S3.')

            for obj in response.get('Contents', []):
                if obj['Key'].lower().endswith('.json'):
                    json_files.append(obj['Key'])

            if not response.get('IsTruncated'):
                break

            params['ContinuationToken'] = response['NextContinuationToken']

        return json_files

    except ClientError as e:
        logger.error(f"Error accessing S3: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        raise

def read_json_data(bucket_name: str, json_file: str) -> dict:
    """
    Read JSON data from an S3 object.

    Args:
        bucket_name (str): Name of the S3 bucket.
        json_file (str): Key of the S3 object.

    Returns:
        dict: JSON data.
    """
    try:
        s3_client = boto3.client('s3')
        response = s3_client.get_object(Bucket=bucket_name, Key=json_file)
        content = response['Body'].read().decode('utf-8')
        logger.info(f'Read JSON data from {json_file}')
        return json.loads(content)

    except ClientError as e:
        logger.error(f"Error accessing S3: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        raise

def generate_random_string(length: int) -> str:
    """
    Generate a random string of the given length.

    Args:
        length (int): Length of the random string.

    Returns:
        str: Random string.
    """
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

def put_message_to_stream(message: dict, stream_name: str) -> None:
    """
    Put a message to a Kinesis stream.

    Args:
        message (dict): Message to put.
        stream_name (str): Name of the Kinesis stream.
    """
    try:
        kinesis_client = boto3.client('kinesis')
        response = kinesis_client.put_record(
            StreamName=stream_name,
            Data=json.dumps(message),
            PartitionKey=generate_random_string(4)
        )
        logger.info(f'Message sent to Kinesis stream: {response}')

    except ClientError as e:
        logger.error(f"Error accessing Kinesis: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        raise

def main():
    json_files = list_json_objects(S3_BUCKET, S3_PREFIX)
    for file in json_files:
        logger.info(f"Processing file: {file}")
        json_data = read_json_data(S3_BUCKET, file)
        for txn in json_data:
            logger.info(f"Sending transaction: {txn}")
            put_message_to_stream(txn, KINESIS_STREAM_NAME)

if __name__ == "__main__":
    main()