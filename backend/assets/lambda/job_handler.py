from datetime import datetime
import re
from typing import Callable
import json
import logging
import os
import random
import string
import traceback

import boto3


dynamodb = boto3.resource('dynamodb')


logger = logging.getLogger()
logger.setLevel(logging.INFO)


def handler(event: dict, context: dict):
    job_type = event['rawPath']

    table = dynamodb.Table(os.getenv('JOBS_TABLE'))

    match event['requestContext']['http']['method']:
        case 'POST':
            job_id = ''.join(random.choices(
                string.ascii_uppercase + string.digits,
                k=8
            ))
            request_timestamp = event['requestContext']['timeEpoch']
            request_datetime \
                = datetime.fromtimestamp(request_timestamp / 1000).isoformat()

            table.put_item(
                Item={
                    'Type': job_type,
                    'Id': job_id,
                    'Parameters': event['body'],
                    'Status': 'PENDING',
                    'StatusMessage': 'Job started',
                    'RequestedAt': request_datetime,
                    'LastStatusUpdateAt': request_datetime,
                    'TTL': request_timestamp + + 60 * 60 * 24 * 7,
                }
            )

            return {
                "statusCode": 202,
                "body": json.dumps({'Id': job_id, 'Status': 'PENDING'}),
            }
        case 'GET':
            job_id = event['queryStringParameters'].get('job_id')

            if job_id is None:
                return {
                    "statusCode": 400,
                    "body": 'Missing job ID'
                }

            job = table.get_item(
                Key={'Type': job_type, 'Id': job_id}).get('Item')

            match job:
                case None:
                    return {
                        "statusCode": 404,
                        "body": json.dumps({
                            'Id': job_id,
                            'Status': 'NOT FOUND'
                        })
                    }
                case {
                    'Id': id,
                    'Status': status,
                    'RequestedAt': request_datetime,
                    'LastStatusUpdateAt': last_update_datetime,
                    **kw
                }:
                    return {
                        "statusCode": 500 if status == 'FAILED' else 200,
                        "body": json.dumps({
                            'Id': id,
                            'Status': status,
                            'StatusMessage': kw.get('StatusMessage'),
                            'RequestedAt': request_datetime,
                            'LastStatusUpdateAt': last_update_datetime,
                        })
                    }


def from_job_request(job_type: str) -> Callable[[dict, dict], dict]:
    '''Decorator for lambda function that executes from a job request.'''

    def decorator(func):
        def wrapper(event: dict, context: dict):
            job_id = event['Records'][0]['dynamodb']['NewImage']['Id']['S']
            table_arn = event['Records'][0]['eventSourceARN']
            table_name = re.findall(
                r'arn:aws:dynamodb:.*?:table/([^/]*)', table_arn)[0]

            table = dynamodb.Table(table_name)

            try:
                func(event, context)

                status = 'SUCCEEDED'
                status_message = 'Job completed successfully'
                logger.info('Job %s finished successfully', job_id)

            except Exception as e:
                status = 'FAILED'
                status_message = traceback.format_exc()
                raise e

            finally:
                table.update_item(
                    Key={'Type': job_type, 'Id': job_id},
                    UpdateExpression=(
                        'SET #Status = :status, '
                        'StatusMessage = :statusMessage, '
                        'LastStatusUpdateAt = :lastStatusUpdateAt'
                    ),
                    ExpressionAttributeNames={'#Status': 'Status'},
                    ExpressionAttributeValues={
                        ':status': status,
                        ':statusMessage': status_message,
                        ':lastStatusUpdateAt': datetime.now().isoformat(),
                    }
                )

        return wrapper
    return decorator
