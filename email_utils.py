# email_utils.py

import json
import requests
from config import EMAIL_API_URL, EMAIL_API_KEY, SUBSCRIBER_EMAILS, DEVELOPER_EMAILS

def sending_email_api(mail_subject, mail_content, to_email_addresses):
    mail_content = mail_content.replace('"', '\"')
    data = {
        'userEmail': 'datasciencealert@sciera.com',
        'appName': 'strl_pipeline',
        'appPage': 'auto-mailer',
        'type': 'STRL-Pipeline-Notification',
        'toEmailAddress': to_email_addresses,
        'emailHTMLBody': mail_content,
        'emailSubject': mail_subject,
        'fromEmailAddress': 'datasciencealert@sciera.com'
    }
    json_data = json.dumps(data)
    headers = {
        'Content-Type': 'application/json',
        'x-api-key': EMAIL_API_KEY
    }
    response = requests.post(EMAIL_API_URL, headers=headers, data=json_data)
    return response.text

def notify_subscribers(subject, message):
    response = sending_email_api(subject, message, SUBSCRIBER_EMAILS)
    print("Notification sent to subscribers. Response:", response)

def notify_developers(subject, message):
    response = sending_email_api(subject, message, DEVELOPER_EMAILS)
    print("Notification sent to developers. Response:", response)
