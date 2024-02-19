from typing import Callable
import json
import logging
import os
import random
import string
import traceback

import awswrangler as wr
from boto3.dynamodb.conditions import Key


logger = logging.getLogger()
logger.setLevel(logging.INFO)


def handler(event: dict, context: dict):
    job_type = event['rawPath']

    if event['requestContext']['http']['method'] == 'GET':
        job_id = event['queryStringParameters']['job_id']

        items = wr.dynamodb.read_items(
            table_name=os.getenv('JOBS_TABLE'),
            key_condition_expression=(
                Key('type').eq(job_type) & Key('id').eq(job_id)
            ),
        )
        if len(items) == 0:
            return {
                "statusCode": 404,
                "body": "Job not found"
            }

        job_status = items.loc[0, 'status']

        return {
            "statusCode": 200,
            "body": json.dumps({'job_id': job_id, 'status': job_status}),
        }

    if event['requestContext']['http']['method'] == 'POST':
        job_id = ''.join(random.choices(
            string.ascii_uppercase + string.digits,
            k=8
        ))

        wr.dynamodb.put_items(
            table_name=os.getenv('JOBS_TABLE'),
            items=[{
                'type': job_type,
                'id': job_id,
                'parameters': event['body'],
                'status': 'PENDING',
                'requested_at': event['requestContext']['timeEpoch'],
                'ttl': event['requestContext']['timeEpoch'] + 60 * 60 * 24 * 7,
            }],
        )

        return {
            "statusCode": 202,
            "body": json.dumps({'job_id': job_id, 'status': 'PENDING'}),
        }


def from_job_request(job_type: str) -> Callable[[dict, dict], dict]:
    '''Decorator for lambda function that executes from a job request'''

    def decorator(func):
        def wrapper(event: dict, context: dict):
            job_id = event['Records'][0]['dynamodb']['Keys']['id']['S']
            parameters \
                = event['Records'][0]['dynamodb']['Keys']['parameters']['S']
            requested_at \
                = event['Records'][0]['dynamodb']['Keys']['requested_at']['N']
            ttl = event['Records'][0]['dynamodb']['Keys']['ttl']['N']

            try:
                func(event, context)
                status = 'SUCCEEDED'
                logger.info('Job %s finished successfully', job_id)

                wr.dynamodb.put_items(
                    table_name=os.getenv('JOBS_TABLE'),
                    items=[{
                        'type': job_type,
                        'id': job_id,
                        'parameters': parameters,
                        'status': status,
                        'requested_at': requested_at,
                        'ttl': ttl,
                    }]
                )
            except Exception as e:
                status = 'FAILED'
                logger.error('Job %s failed: %s', job_id, e)

                wr.dynamodb.put_items(
                    table_name=os.getenv('JOBS_TABLE'),
                    items=[{
                        'type': job_type,
                        'id': job_id,
                        'parameters': parameters,
                        'status': status,
                        'statusMessage': traceback.format_exc(),
                        'requested_at': requested_at,
                        'ttl': ttl,
                    }]
                )

        return wrapper
    return decorator
