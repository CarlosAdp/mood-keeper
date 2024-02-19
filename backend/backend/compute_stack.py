from aws_cdk import (
    Duration,
    RemovalPolicy,
    Stack,
    aws_apigatewayv2 as api_gw,
    aws_dynamodb as dynamodb,
    aws_apigatewayv2_integrations as api_gw_integrations,
    aws_iam as iam,
    aws_lambda as _lambda,
    aws_lambda_python_alpha as _lambda_python,
)
from constructs import Construct


class ComputeStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        bucket_name = kwargs.pop('bucket_name')
        database_name = kwargs.pop('database_name')
        managed_policy_arn = kwargs.pop('managed_policy_arn')

        super().__init__(scope, construct_id, **kwargs)

        jobs_table = dynamodb.Table(
            self, 'JobsTable',
            table_name='MoodKeeperJobs',
            partition_key=dynamodb.Attribute(
                name='type',
                type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(
                name='id',
                type=dynamodb.AttributeType.STRING
            ),
            stream=dynamodb.StreamViewType.NEW_IMAGE,
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.DESTROY,
            time_to_live_attribute='ttl',
        )
        jobs_table_access_policy_statement = iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=[
                'dynamodb:DeleteItem',
                'dynamodb:GetItem',
                'dynamodb:PutItem',
                'dynamodb:Query',
                'dynamodb:Scan',
                'dynamodb:UpdateItem',
                'dynamodb:DescribeTable',
                'dynamodb:BatchWriteItem',
                'dynamodb:BatchGetItem'
            ],
            resources=[jobs_table.table_arn]
        )

        # Layers
        layer_main = _lambda_python.PythonLayerVersion(
            self, 'MainLayer',
            entry='assets/lambda',
            compatible_runtimes=[_lambda.Runtime.PYTHON_3_12],
            description='Main layer with all dependencies'
        )
        layer_managed = _lambda.LayerVersion.from_layer_version_arn(
            self, 'ManagedLayer',
            layer_version_arn='arn:aws:lambda:sa-east-1:336392948345:layer:AWSSDKPandas-Python312:4'
        )

        # Functions
        function_job_handler = _lambda.Function(
            self, 'JobHandlerFunction',
            function_name='MoodKeeperJobHandler',
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler='job_handler.handler',
            code=_lambda.Code.from_asset('assets/lambda'),
            timeout=Duration.seconds(10),
            memory_size=1280,
            layers=[layer_managed],
            environment={'JOBS_TABLE': jobs_table.table_name},
        )
        function_job_handler.add_to_role_policy(
            jobs_table_access_policy_statement
        )

        # API
        api = api_gw.HttpApi(
            self, 'MoodKeeperAPI',
            api_name='MoodKeeperAPI',
            description='API for the MoodKeeper backend'
        )

        api.add_routes(
            path='/user/update_saved_tracks',
            methods=[api_gw.HttpMethod.GET, api_gw.HttpMethod.POST],
            integration=api_gw_integrations.HttpLambdaIntegration(
                'UpdateSavedTracksIntegration',
                handler=function_job_handler,
            )
        )
