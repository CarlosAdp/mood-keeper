from aws_cdk import (
    CfnOutput,
    Stack,
    aws_s3 as s3,
    aws_glue as glue,
    aws_iam as iam,
)
from constructs import Construct


class StorageStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        bucket_name = f'{self.account}.{self.region}.mood-keeper'

        s3_bucket = s3.Bucket(
            self, "MoodKeeperBucket",
            bucket_name=bucket_name,
            versioned=True,
            encryption=s3.BucketEncryption.S3_MANAGED,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
        )
        s3_bucket_name_output = CfnOutput(
            self, "MoodKeeperBucketName",
            value=s3_bucket.bucket_name,
            export_name='MoodKeeperBucketName'
        )

        glue_database = glue.CfnDatabase(
            self, "MoodKeeperDatabase",
            catalog_id=self.account,
            database_input=glue.CfnDatabase.DatabaseInputProperty(
                name='mood_keeper',
                description='Database for the MoodKeeper app',
                location_uri=f's3://{s3_bucket.bucket_name}',
            )
        )
        glue_database_name_output = CfnOutput(
            self, "MoodKeeperDatabaseName",
            value=glue_database.ref,
            export_name='MoodKeeperDatabaseName'
        )

        managed_policy = iam.ManagedPolicy(
            self, "MoodKeeperManagedPolicy",
            managed_policy_name='MoodKeeperStoragePolicy',
            statements=[
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        's3:GetObject',
                        's3:PutObject',
                        's3:DeleteObject',
                        's3:ListBucket',
                    ],
                    resources=[
                        s3_bucket.bucket_arn,
                        f'{s3_bucket.bucket_arn}/*',
                    ],
                ),
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        'glue:GetDatabase',
                        'glue:GetDatabases',
                        'glue:CreateDatabase',
                        'glue:UpdateDatabase',
                        'glue:DeleteDatabase',
                        'glue:GetTable',
                        'glue:GetTables',
                        'glue:CreateTable',
                        'glue:UpdateTable',
                        'glue:DeleteTable',
                        'glue:GetPartition',
                        'glue:GetPartitions',
                        'glue:CreatePartition',
                        'glue:UpdatePartition',
                        'glue:DeletePartition',
                        'glue:BatchCreatePartition',
                        'glue:BatchDeletePartition',
                    ],
                    resources=[
                        f'arn:aws:glue:{self.region}:{self.account}:catalog',
                        f'arn:aws:glue:{self.region}:{self.account}:database/{glue_database.ref}',
                        f'arn:aws:glue:{self.region}:{self.account}:table/{glue_database.ref}',
                    ]
                )
            ],
        )
        managed_policy_output = CfnOutput(
            self, "MoodKeeperManagedPolicyName",
            value=managed_policy.managed_policy_name,
            export_name='MoodKeeperManagedPolicyName'
        )
