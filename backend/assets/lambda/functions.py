import json
import os

import boto3


def user_update_saved_tracks(event: dict, context: dict) -> dict:
    body = json.loads(event["body"])

    user_id = body["user_id"]
    access_token = body["access_token"]
    offset = 0

    sqs = boto3.client("sqs")
    response = sqs.send_message(
        QueueUrl=os.getenv('QUEUE_URL'),
        MessageAttributes={
            'user_id': {
                'DataType': 'String',
                'StringValue': user_id
            },
            'access_token': {
                'DataType': 'String',
                'StringValue': access_token
            },
            'offset': {
                'DataType': 'Number',
                'StringValue': '0'
            }
        },
        MessageBody=f'Get page {offset} of saved tracks for user {user_id}'
    )

    return {
        "statusCode": 200,
        "body": json.dumps({'MessageId': response['MessageId']}),
    }
