#!/usr/bin/env python3
import os

import aws_cdk as cdk

from backend.compute_stack import ComputeStack
from backend.storage_stack import StorageStack


app = cdk.App()

storage_stack = StorageStack(
    app, "MoodKeeperBackendStorageStack",
    env=cdk.Environment(
        account=os.getenv('CDK_DEFAULT_ACCOUNT'),
        region='sa-east-1'
    )
)

ComputeStack(
    app, "MoodKeeperBackendComputeStack",
    env=cdk.Environment(
        account=os.getenv('CDK_DEFAULT_ACCOUNT'),
        region='sa-east-1'
    ),
    bucket_name=storage_stack.bucket_name,
    database_name=storage_stack.database_name,
    managed_policy_arn=storage_stack.managed_policy_arn,
)

app.synth()
