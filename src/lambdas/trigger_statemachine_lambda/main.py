import json
import requests
import boto3
import os

sfn_client = boto3.client('stepfunctions')
statamachine_arn = os.getenv("STATEMACHINE_ARN")

def lambda_handler(event, context):
    state_machine_arn = statamachine_arn
    # TODO implement
    input_payload = {
        "bucket": event['Records'][0]['s3']['bucket']['name'],
        "key": event['Records'][0]['s3']['object']['key']
    }

    try:
        response = sfn_client.start_execution(
            stateMachineArn=state_machine_arn,
            input=json.dumps(input_payload)
        )
    except Exception as e:
        raise


