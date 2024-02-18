from aws_cdk import (
    Duration,
    RemovalPolicy,
    Stack,
    aws_apigatewayv2 as api_gw,
    aws_apigatewayv2_integrations as api_gw_integrations,
    aws_iam as iam,
    aws_lambda as _lambda,
    aws_sqs as sqs,
)
from constructs import Construct


class ComputeStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Queues
        queue_user_update_saved_tracks = sqs.Queue(
            self, "UserSavedTracksQueue",
            queue_name='MoodKeeperUserUpdateSavedTracks',
            enforce_ssl=True,
            removal_policy=RemovalPolicy.DESTROY,
            retention_period=Duration.minutes(10),
        )

        # Functions
        function_user_update_saved_tracks = _lambda.Function(
            self, 'UserUpdateSavedTracksFunction',
            function_name='MoodKeeperUserUpdateSavedTracks',
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler='functions.user_update_saved_tracks',
            code=_lambda.Code.from_asset('assets/lambda'),
            environment={
                'QUEUE_URL': queue_user_update_saved_tracks.queue_url,
            }
        )
        function_user_update_saved_tracks.add_to_role_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=['sqs:SendMessage'],
                resources=[queue_user_update_saved_tracks.queue_arn]
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
