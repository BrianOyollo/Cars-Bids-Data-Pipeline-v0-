import os
import json
import pandas as pd
import time


def save_auction_data_locally(
    data:list,
    output_dir: str,
    file_prefix: str  = "auctions",
    tag: str  = None,
) -> str:
    """
    Saves auction data in json with optional file identifier.
    
    Args:
        data: list of nested dicts
        output_dir: Directory path where file will be saved
        file_prefix: Base name for the output file (default: "auctions")
        tag: Additional identifier  (optional)
    
    Returns:
        str: Full path to the saved file

    """
    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)
    
    # Create filename components
    filename_parts = [file_prefix]
    
    if tag:
        filename_parts.append(tag)
    
    filename = "_".join(filename_parts) + ".json"
    filepath = os.path.join(output_dir, filename)
    
    with open(filepath, 'w') as file:
        json.dump(data, file, indent=3)
    
    return filename


def save_auction_data_to_s3(s3_client, bucket:str, data:list, object_key:str) -> str:
    """
    Saves auction data (list of dicts) as a JSON file to an S3 bucket.

    Args:
        s3_client: A boto3 S3 client.
        bucket: Name of the S3 bucket.
        data: List of nested dicts (auction data).

    Returns:
        str: key of uploaded object.
    """

    # Upload to S3
    try:
        s3_client.put_object(
            Bucket=bucket,
            Key=f"{object_key}",
            Body=json.dumps(data, indent=3),
            ContentType="application/json"
        )
        return object_key
    except Exception as e:
        raise

