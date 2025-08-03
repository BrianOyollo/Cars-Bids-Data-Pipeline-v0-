import json,os,json
import boto3
import pandas as pd
import numpy as np
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values


def update_dim_tables(cursor):
    """
    Runs the SQL script to update dimension and fact tables.
    Prints which section is being executed.
    """

    section_names = [
        "LOAD auction_status_dim",
        "LOAD reserve_status_dim",
        "LOAD body_style_dim",
        "LOAD seller_type_dim",
        "LOAD drivetrain_dim",
        "LOAD transmission_dim",
        "LOAD city_dim",
        "LOAD vehicle_make_dim",
        "LOAD vehicle_model_dim",
        "LOAD vehicle_dim",
        "LOAD auction_fact",
    ]

    with open("update_dims.sql", "r") as f:
        script = f.read()

    sql_script = script.split(";")

    for i, (section_name, statement) in enumerate(zip(section_names, sql_script)):
        sql_block = statement.strip()
        if not sql_block:
            continue

        print(f"\n Running [{i+1}/{len(section_names)}]: {section_name}")

        try:
            cursor.execute(sql_block)
        except Exception as e:
            print(f"Error in section: {section_name}")
            raise 3
    


def read_json_from_s3(s3_client, bucket_name:str, key:str)->list:
    """
    Reads a JSON file from S3 and returns it as a Python object.

    Parameters:
        bucket_name (str): Name of the S3 bucket
        key (str): Path/key to the JSON file (e.g., 'data/file.json')
        aws_conn_id (str): Optional Airflow connection ID (if using Airflow context)

    Returns:
        dict or list: Parsed JSON content
    """


    response = s3_client.get_object(Bucket=bucket_name, Key=key)
    content = response['Body'].read().decode('utf-8')

    return [json.loads(line) for line in content.splitlines()]
    

def psycopg_connection(db_user:str,db_password:str,db_host:str,db_port:int,db_name:str):
    connection = psycopg2.connect(
        dbname=db_name,
        user=db_user,
        password=db_password,
        host=db_host,
        port=db_port
        
    )
    cursor = connection.cursor()
    return connection, cursor


def load_to_postgres(df, conn, cursor):
        insert_columns = [
            "auction_date","auction_id","vin","seller_type","reserve_status","reserve_met","auction_status",
            "auction_title","auction_subtitle","make","model","exterior_color","interior_color",
            "body_style","mileage","engine","drivetrain","transmission","transmission_type", "gears",
            "title_status_cleaned","title_state","city","state","bid_count", "view_count", "watcher_count",
            "highest_bid_value","max_bid","min_bid","mean_bid","median_bid","bid_range","bids",
            "highlight_count","equipment_count","mod_count","flaw_count","service_count","included_items_count",
            "video_count","manufacture_year","location","auction_url","seller"  
        ]
        insert_df = df[insert_columns]
        
        insert_df = insert_df.replace({np.nan: None})
        insert_df = insert_df.sort_values('auction_date', ascending=False).reset_index(drop=True)
        insert_df = insert_df.drop_duplicates('auction_id', keep='first')

        data = list(insert_df.itertuples(index=False, name=None))
        query = f"INSERT INTO staging ({', '.join(insert_columns)}) VALUES %s"

        # empty staging
        cursor.execute("TRUNCATE TABLE staging")
        conn.commit()

        # insert new data
        execute_values(cursor, query, data, page_size=150)
     
        # update dim & fact tables
        update_dim_tables(cursor)

        # commit changes   
        conn.commit()


def lambda_handler(event, context):
    
    # get keys of uploaded processed files
    processed_obj_keys = event['uploaded_objects'] # list of uploaded keys

    # env variables
    processed_auctions_bucket = os.getenv('PROCESSED_AUCTIONS_BUCKET')
    db_user = os.getenv('DB_USER')
    db_password = os.getenv('DB_PASSWORD')
    db_host = os.getenv('DB_HOST')
    db_port = os.getenv('DB_PORT')
    db_name = os.getenv('DB_NAME')


    conn = None,
    cursor = None

    try:
        # init s3 client, db connection
        s3_client = boto3.client('s3')
        conn, cursor = psycopg_connection(db_user, db_password, db_host, db_port, db_name)

        # read processed auctions into a list
        auctions_data = []
        for obj_key in processed_obj_keys:
            obj_data = read_json_from_s3(s3_client, processed_auctions_bucket, obj_key)
            auctions_data.extend(obj_data)

        # create df
        df = pd.DataFrame(auctions_data)

        # load to PostreSQL data warehouse
        load_to_postgres(df, conn, cursor)

        return {
            "status": 200
        }

    except Exception as e:
        print(f"LoadError: {e}")
        raise
    
    finally:
        if cursor:
            cursor.close()

        if conn:
            conn.close()
