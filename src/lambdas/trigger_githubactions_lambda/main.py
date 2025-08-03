import json
import os
import requests 

GITHUB_TOKEN = os.environ['GITHUB_TOKEN']
GITHUB_REPO = os.getenv("GITHUB_REPO")
WORKFLOW_FILE = os.getenv("WORKFLOW_FILE")
GITHUB_API_URL = f"https://api.github.com/repos/{GITHUB_REPO}/actions/workflows/{WORKFLOW_FILE}/dispatches"


def lambda_handler(event, context):
    # rescrape_obj_key = event["rescrape_obj_key"]
    # task_token = event["TaskToken"]

    headers = {
        "Accept": "application/vnd.github+json",
        "Authorization": f"Bearer {GITHUB_TOKEN}",
        "X-GitHub-Api-Version": "2022-11-28"
    }

    payload = {
        "ref": "main",
        "inputs": {
            "rescrape_obj_key": event["rescrape_object_key"],
            "TaskToken": event["TaskToken"]
        }
    }

    response = requests.post(GITHUB_API_URL, headers=headers, json=payload)

    if response.status_code == 201 or response.status_code == 204:
        print("GitHub Workflow triggered successfully!")
        return {"status": "success"}
    else:
        print(f"Error triggering GitHub Workflow: {response.text}")
        raise Exception("GitHub workflow trigger failed")
    
