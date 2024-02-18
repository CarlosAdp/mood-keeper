#!/usr/bin/env python3
import os

import aws_cdk as cdk

from backend.compute_stack import ComputeStack
from backend.storage_stack import StorageStack


app = cdk.App()
ComputeStack(
    app, "MoodKeeperBackendComputeStack",
    env=cdk.Environment(
        account=os.getenv('CDK_DEFAULT_ACCOUNT'),
        region='sa-east-1'
    )
)

StorageStack(
    app, "MoodKeeperBackendStorageStack",
    env=cdk.Environment(
        account=os.getenv('CDK_DEFAULT_ACCOUNT'),
        region='sa-east-1'
    )
)

app.synth()
