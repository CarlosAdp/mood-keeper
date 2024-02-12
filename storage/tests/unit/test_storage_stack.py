import aws_cdk as core
import aws_cdk.assertions as assertions

from storage_stack import MoodKeeperStorageStack


def test_s3_bucket_created():
    app = core.App()
    stack = MoodKeeperStorageStack(app, "mood-keeper")
    template = assertions.Template.from_stack(stack)

    bucket_name = f'{stack.account}.{stack.region}.mood-keeper.storage'

    template.has_resource_properties("AWS::S3::Bucket", {
        "BucketName": bucket_name
    })
