from job_handler import from_job_request


@from_job_request
def handler(event: dict, context: dict) -> dict:
    print(event)
    return {
        "statusCode": 200,
        "body": "Hello from Lambda!"
    }
