#!/usr/bin/env python3
import aws_cdk as cdk
import os

from storage_stack import MoodKeeperStorageStack


app = cdk.App()
MoodKeeperStorageStack(
    app, "MoodKeeperStorageStack",
    env=cdk.Environment(
        account=os.getenv('CDK_DEFAULT_ACCOUNT'),
        region='sa-east-1'
    )
)

app.synth()
