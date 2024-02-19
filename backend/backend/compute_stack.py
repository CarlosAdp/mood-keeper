from aws_cdk import (
    Duration,
    RemovalPolicy,
    Stack,
    aws_apigatewayv2 as api_gw,
    aws_apigatewayv2_integrations as api_gw_integrations,
    aws_iam as iam,
    aws_lambda as _lambda,
    aws_lambda_event_sources as lambda_event_sources,
    aws_lambda_python_alpha as _lambda_python,
    aws_sqs as sqs,
)
from constructs import Construct


class ComputeStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        bucket_name = kwargs.pop('bucket_name')
        database_name = kwargs.pop('database_name')
        managed_policy_arn = kwargs.pop('managed_policy_arn')

        super().__init__(scope, construct_id, **kwargs)

        # Queues
        queue_user_update_saved_tracks = sqs.Queue(
            self, "UserSavedTracksQueue",
            queue_name='MoodKeeperUserUpdateSavedTracks',
            enforce_ssl=True,
            removal_policy=RemovalPolicy.DESTROY,
            retention_period=Duration.minutes(20),
            visibility_timeout=Duration.minutes(10),
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
        function_user_update_saved_tracks = _lambda.Function(
            self, 'UserUpdateSavedTracksFunction',
            function_name='MoodKeeperUserUpdateSavedTracks',
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler='functions.user_update_saved_tracks',
            code=_lambda.Code.from_asset('assets/lambda'),
            timeout=Duration.seconds(10),
            environment={
                'QUEUE_URL': queue_user_update_saved_tracks.queue_url,
            },
            layers=[
                layer_main,
                layer_managed,
            ],
        )
        function_user_update_saved_tracks.add_to_role_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=['sqs:SendMessage'],
                resources=[queue_user_update_saved_tracks.queue_arn]
            )
        )

        function_user_update_saved_tracks_by_page = _lambda.Function(
            self, 'UserUpdateSavedTracksByPageFunction',
            function_name='MoodKeeperUserUpdateSavedTracksByPage',
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler='functions.user_update_saved_tracks_by_page',
            code=_lambda.Code.from_asset('assets/lambda'),
            timeout=Duration.minutes(7),
            memory_size=1280,
            events=[
                lambda_event_sources.SqsEventSource(
                    queue=queue_user_update_saved_tracks,
                )
            ],
            layers=[
                layer_main,
                layer_managed,
            ],
            environment={
                'BUCKET_NAME': bucket_name,
                'DATABASE_NAME': database_name,
                'QUEUE_URL': queue_user_update_saved_tracks.queue_url,
            },
        )
        function_user_update_saved_tracks_by_page.role.add_managed_policy(
            iam.ManagedPolicy.from_managed_policy_arn(
                self, 'MoodKeeperManagedPolicy', managed_policy_arn
            )
        )

        # API
        api = api_gw.HttpApi(
            self, 'MoodKeeperAPI',
            api_name='MoodKeeperAPI',
            description='API for the MoodKeeper backend'
        )

        api.add_routes(
            path='/update_saved_tracks',
            methods=[api_gw.HttpMethod.POST],
            integration=api_gw_integrations.HttpLambdaIntegration(
                'UpdateSavedTracksIntegration',
                handler=function_user_update_saved_tracks,
            )
        )
