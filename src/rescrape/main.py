import os
import json
import setup 
import boto3
import sys
import scrape_auction
import transform_load
from dotenv import load_dotenv

load_dotenv()

def read_rescrape_urls(inputs_path:str):
    with open(inputs_path, "r") as f:
        data = json.load(f)
    return data

def read_task_token(task_token_path:str):
    with open(task_token_path, "r") as f:
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

inputs_path = "/tmp/rescrape/inputs.txt"
task_token_path = "/tmp/rescrape/task_token.txt"

task_token = read_task_token(task_token_path)
urls = read_rescrape_urls(inputs_path)
rescrape(s3_client, sfn_client, processed_auctions_bucket, urls, task_token)

