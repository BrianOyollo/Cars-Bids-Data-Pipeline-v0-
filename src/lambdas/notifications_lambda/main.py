import json
import requests

def send_notification(tags:str,attachment:str,priority:str, message:str, title:str=None):
    topic="CarsnBidsStateMachine"
    try:
        response = requests.post(
            f"https://ntfy.sh/{topic}",
            data=message.encode("utf-8"),
            headers={
                "Title": f"Cars&Bids Pipeline - {title}",
                "Priority": str(priority),
                "Tags": tags,
                "Attach": attachment
            },
            timeout=5 
        )
        if response.status_code >= 400:
            print(f"Failed to send notification: {response.status_code} - {response.text}")
        else:
            print("Notification sent successfully.")
    except requests.RequestException as e:
        print(f"Notification failed with exception: {e}")

def lambda_handler(event, context):
    status_code = event['statusCode']
    message = event['message']


    # priority levels
    success_priority = 2 # normal
    failure_priority = 5 # urgent
    warning_priority = 4 # high

    # tags
    success_tag = "hammer_and_wrench,white_check_mark"
    failure_tag = "hammer_and_wrench,skull"
    warning_tag = "hammer_and_wrench, warning"

    #images
    success_image = "https://static1.hotcarsimages.com/wordpress/wp-content/uploads/2023/02/doug-demuro-in-his-garage.jpg"
    failure_image = "https://www.yotatech.com/wp-content/uploads/2022/04/00.jpg"
    warning_image = "https://smart-motoring.com/wp-content/uploads/2018/02/Doug-DeMuro-Tesla-Model-X.jpg"


    if status_code == 200:
        title = "All good"
        send_notification(success_tag, success_image, success_priority, message, title)

    elif status_code ==206:
        title = "Warning"
        send_notification(warning_tag, warning_image, warning_priority, message, title)

    else:
        title = event['error']
        send_notification(failure_tag, failure_image, failure_priority, message, title)
