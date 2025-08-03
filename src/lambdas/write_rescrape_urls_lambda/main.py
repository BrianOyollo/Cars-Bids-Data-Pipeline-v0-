import json
import boto3
import os
from datetime import datetime


s3 = boto3.client('s3')
BUCKET_NAME = os.environ.get('AUCTION_URLS')
RAW_RESCRAPE_FOLDER = os.environ.get('RAW_RESCRAPE_FOLDER')


def lambda_handler(event, context):
    try:
        # Get URLs from event
        urls = event.get('rescrape_urls')
        if not urls:
            return {
                'statusCode': 400,
                'body': json.dumps('No URLs provided in the event.')
            }

        # Create file content
        file_content = "\n".join(urls)

        # Generate unique key for S3
        timestamp = int(datetime.now().timestamp())
        s3_key = f"{RAW_RESCRAPE_FOLDER}/{timestamp}.txt"

        # Upload to S3
        s3.put_object(
            Bucket=BUCKET_NAME,
            Key=s3_key,
            Body=file_content.encode('utf-8')
        )

        print(f"File uploaded to S3: {s3_key}")

        return {
            'statusCode': 200,
            'bucket':f"{BUCKET_NAME}",
            'key': f"{s3_key}"
        }
    except Exception as e:
        print(f"Error: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps(f"Error: {str(e)}")
        }
