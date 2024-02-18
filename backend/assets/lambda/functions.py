import json
import logging
import os

import awswrangler as wr
import boto3
import pandas as pd
import spotipy


logger = logging.getLogger()
logger.setLevel(logging.INFO)


def send_sqs_message(message: str, message_attributes: dict) -> dict:
    message_attributes_parsed = dict()
    for key, value in message_attributes.items():
        match value:
            case str():
                message_attributes_parsed[key] = {
                    'DataType': 'String',
                    'StringValue': value
                }
            case int():
                message_attributes_parsed[key] = {
                    'DataType': 'Number',
                    'StringValue': str(value)
                }
            case _:
                raise ValueError(
                    f'Invalid type {type(value)} for message attribute {key}'
                )

    sqs = boto3.client('sqs')
    return sqs.send_message(
        QueueUrl=os.getenv('QUEUE_URL'),
        MessageAttributes=message_attributes_parsed,
        MessageBody=message
    )


def user_update_saved_tracks(event: dict, context: dict) -> dict:
    body = json.loads(event["body"])

    user_id = body["user_id"]
    access_token = body["access_token"]
    offset = 0
    requested_at = event['requestContext']['timeEpoch']

    response = send_sqs_message(
        f'Requesting update to user {user_id} saved tracks',
        {
            'user_id': user_id,
            'access_token': access_token,
            'offset': offset,
            'requested_at': requested_at,
        }
    )

    return {
        "statusCode": 200,
        "body": json.dumps({'MessageId': response['MessageId']}),
    }


def user_update_saved_tracks_by_page(event: dict, context: dict) -> dict:
    for message in event['Records']:
        attributes = message['messageAttributes']

        user_id = attributes['user_id']['stringValue']
        access_token = attributes['access_token']['stringValue']
        offset = int(attributes['offset']['stringValue'])
        requested_at = pd.Timestamp.fromtimestamp(
            int(attributes['requested_at']['stringValue']) / 1000)

        logger.info(
            'Getting page %d of saved tracks for user %s',
            offset/50, user_id
        )

        sp = spotipy.Spotify(auth=access_token)
        response = sp.current_user_saved_tracks(limit=50, offset=offset)

        saved_tracks = pd.json_normalize(response['items'])[[
            'track.id',
            'track.name',
            'track.type',
            'track.duration_ms',
            'track.track_number',
            'track.available_markets',
            'track.popularity',
            'track.album.id',
            'track.album.name',
            'track.album.type',
            'track.album.release_date',
            'added_at',
        ]]
        saved_tracks['user_id'] = user_id
        saved_tracks['requested_at'] = requested_at

        logger.info('Saving to S3 and glue')

        bucket = os.getenv('BUCKET_NAME')
        database = os.getenv('DATABASE_NAME')
        prefix = 'user/saved_tracks'
        table = 'user_saved_tracks'
        wr.s3.to_parquet(
            df=saved_tracks,
            path=f's3://{bucket}/{prefix}',
            dataset=True,
            database=database,
            table=table,
            partition_cols=['user_id', 'requested_at'],
            mode='append',
        )

        logger.info(
            'Saved page %d of saved tracks of user %s to %s',
            offset/50, user_id, f's3://{bucket}/{prefix}'
        )

        sqs = boto3.client("sqs")
        sqs.delete_message(
            QueueUrl=os.getenv('QUEUE_URL'),
            ReceiptHandle=message['receiptHandle']
        )

        logger.info('Deleted message %s', message['messageId'])

        if response['next']:
            offset += 50

            send_sqs_message(
                f'Requesting page {offset / 50} of user {user_id} library',
                {
                    'user_id': user_id,
                    'access_token': access_token,
                    'offset': offset,
                    'requested_at': requested_at,
                }
            )

            logger.info('Next page is available at offset %d', offset)
