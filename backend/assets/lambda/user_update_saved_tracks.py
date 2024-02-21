import json
import logging
import os
import time

import awswrangler as wr
import pandas as pd
import spotipy

from job_handler import from_job_request


logger = logging.getLogger()
logger.setLevel(logging.INFO)


@from_job_request
def handler(event: dict, context: dict) -> dict:
    record = event['Records'][0]

    parameters = json.loads(record['dynamodb']['NewImage']['Parameters']['S'])
    user_id = parameters['user_id']
    access_token = parameters['access_token']

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
    saved_tracks.columns = saved_tracks.columns\
        .str.replace('track.', '', regex=False)\
        .str.replace('.', '_', regex=False)
    saved_tracks['user_id'] = user_id
    saved_tracks['job_id'] = record['dynamodb']['NewImage']['Id']['S']

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
        partition_cols=['user_id'],
        mode='overwrite_partitions',
    )

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": "Saved %d tracks of user %s to %s" % (
                len(saved_tracks), user_id, f's3://{bucket}/{prefix}'
            )
        })
    }
