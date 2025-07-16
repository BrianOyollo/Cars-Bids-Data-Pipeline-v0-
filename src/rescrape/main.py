import os
import json
import setup 
import boto3
import sys
import scrape_auction
import transform_load
from dotenv import load_dotenv

load_dotenv()


def read_txt_from_s3(s3_client, bucket_name: str, key: str) -> list[str]:
    """
    Reads a .txt file from S3 and returns a list of lines.

    Parameters:
    - bucket_name (str): The name of the S3 bucket.
    - key (str): The key (path) of the .txt file in the bucket.

    Returns:
    - list of str: Each line from the file as a string (with trailing newline removed).
    """
    
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=key)
        content = response['Body'].read().decode('utf-8')
        
        # Split into lines and strip extra whitespace
        lines = [line.strip() for line in content.splitlines() if line.strip()]
        return lines
    
    except s3_client.exceptions.NoSuchKey:
        print(f"File not found: s3://{bucket_name}/{key}")
        return []
    except Exception as e:
        print(f"Error reading file from S3: {e}")
        return []



def read_inputs(inputs_path:str)->str:
    """Reads the contents of a text file and returns it as a stripped string.

    Args:
        inputs_path (str): The file path to the input text file.

    Returns:
        str:  The content of the file with leading and trailing whitespace removed
    """

    with open(inputs_path, "r") as f:
        return f.read().strip()
    
    
def send_task_success(sfn_client, task_token:str, output:dict):
    # sends a success response to Step Functions

    try:
        response = sfn_client.send_task_success(
            taskToken=task_token,
            output=json.dumps(output)
        )
        print("success callback sent:", response)
    except Exception as e:
        print(f"Error sending task success: {e}")
        sys.exit(1)


def send_task_failure(sfn_client, task_token, error, cause):
    # sends a failure response to Step Functions

    try:
        response = sfn_client.send_task_failure(
            taskToken=task_token,
            error=error,
            cause=cause
        )
        print("Failure callback sent:", response)
    except Exception as e:
        print("Failed to send failure callback:", e)
        sys.exit(1)
        
def rescrape(s3_client, sfn_client, processed_auctions_bucket:str, urls:list, task_token:str):

    driver = None
    auctions_data = []

    try:
        # initialize the driver
        print("Setting up the driver...")
        driver = setup.driver_setup()

        # scrape auction data for each URL
        print("Scraping auction data...")
        for url in urls:
            auction_data = scrape_auction.scrape_auction_data(driver, url)
            auctions_data.append(auction_data)

        # transform 
        print("Cleaning & Transforming auction data...")
        def transform_auction_data(raw_data):
            # reshape raw data
            data = transform_load.convert_to_list_dicts(raw_data)

            # create auction df
            df = transform_load.create_auction_df(data)

            # extract valid auctions
            valid_df = transform_load.extract_invalid_auctions(df)[0]

           # clean and transform data
            cleaned_transformed_df = transform_load.clean_and_transform(valid_df)
            return cleaned_transformed_df
        
        transformed_df = transform_auction_data(auctions_data)
        transformed_df.to_json("transformed_auction_data.json", orient="records", indent=3)

        # load transformed data to s3 processed auctions bucket
        print("Loading transformed data to S3...")
        uploaded_objects_keys = transform_load.load_to_s3(s3_client, processed_auctions_bucket, transformed_df)

        print("Data loaded successfully. Uploaded objects keys:", uploaded_objects_keys)

        # send task success
        send_task_success(sfn_client, task_token, {
            "bucket": processed_auctions_bucket,
            "uploaded_objects_keys": uploaded_objects_keys,
        })
        
    
    except Exception as e:
        print(f"Rescrape Pipeline Error: {e}")

        # send task failure
        send_task_failure(sfn_client, task_token, "RescrapePipelineError", str(e))
    
    finally:
        if driver:
            setup.driver_teardown(driver)


# urls = [
#     "https://carsandbids.com/auctions/3vA7lPBv/2002-bmw-330i-sedan",
#     "https://carsandbids.com/auctions/9AbGRNBj/2000-ferrari-360-modena"
# ]



s3_client = boto3.client('s3')
sfn_client = boto3.client('stepfunctions')
processed_auctions_bucket = os.getenv('PROCESSED_AUCTIONS_BUCKET')
raw_auctions_bucket = os.getenv('RAW_AUCTIONS_BUCKET')
rescrape_bucket_dir = os.getenv('RESCRAPE_BUCKET_DIR')

rescrape_obj_path = "/tmp/rescrape/rescrape_object.txt"
task_token_path = "/tmp/rescrape/task_token.txt"

task_token = read_inputs(task_token_path)
obj_key = read_inputs(rescrape_obj_path)

urls = read_txt_from_s3(s3_client, raw_auctions_bucket, obj_key)
rescrape(s3_client, sfn_client, processed_auctions_bucket, urls, task_token)

