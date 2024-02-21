from job_handler import from_job_request


@from_job_request(job_type='/user/update_saved_tracks')
def handler(event: dict, context: dict) -> dict:
    print(event)
    return {
        "statusCode": 200,
        "body": "Hello from Lambda!"
    }
