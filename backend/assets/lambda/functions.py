import json
import logging
import os
import time

import awswrangler as wr
import boto3
import pandas as pd
import spotipy


logger = logging.getLogger()
logger.setLevel(logging.INFO)


def user_update_saved_tracks(event: dict, context: dict) -> dict:
    body = json.loads(event["body"])

    user_id = body["user_id"]
    access_token = body["access_token"]
    requested_at = event['requestContext']['timeEpoch']

    sqs = boto3.client('sqs')
    sqs.send_message(
        QueueUrl=os.getenv('QUEUE_URL'),
        MessageBody=f'Getting saved tracks for user {user_id}',
        MessageAttributes={
            'user_id': {
                'DataType': 'String',
                'StringValue': user_id
            },
            'access_token': {
                'DataType': 'String',
                'StringValue': access_token
            },
            'requested_at': {
                'DataType': 'Number',
                'StringValue': str(requested_at)
            },
        }
    )

    return {
        "statusCode": 200,
        "body": json.dumps({'message': 'Update request has been done'})
    }


def user_update_saved_tracks_by_page(event: dict, context: dict) -> dict:
    for message in event['Records']:
        attributes = message['messageAttributes']
        user_id = attributes['user_id']['stringValue']
        access_token = attributes['access_token']['stringValue']

        logger.info('Getting saved tracks for user %s', user_id)

        sp = spotipy.Spotify(auth=access_token)

        offset = 0
        saved_tracks = []
        while (resp := sp.current_user_saved_tracks(50, offset))['next']:
            logger.info('Got saved tracks page %d', offset/50)

            saved_tracks.extend(resp['items'])
            offset += 50
            time.sleep(1)

        saved_tracks = pd.json_normalize(saved_tracks)[[
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
        saved_tracks['requested_at'] = pd.Timestamp.fromtimestamp(
            int(attributes['requested_at']['stringValue']) / 1000)

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
            'Saved saved tracks of user %s to %s',
            user_id, f's3://{bucket}/{prefix}'
        )

        sqs = boto3.client("sqs")
        sqs.delete_message(
            QueueUrl=os.getenv('QUEUE_URL'),
            ReceiptHandle=message['receiptHandle']
        )

        logger.info('Deleted message %s', message['messageId'])
