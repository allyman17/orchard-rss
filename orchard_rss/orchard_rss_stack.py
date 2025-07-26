#!/usr/bin/env python3
import os
from aws_cdk import (
    App,
    Stack,
    aws_lambda as lambda_,
    aws_apigateway as apigateway,
    aws_dynamodb as dynamodb,
    Duration,
    RemovalPolicy,
    CfnOutput
)
from constructs import Construct

class OrchardRssStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # DynamoDB table to store RSS feed items
        rss_table = dynamodb.Table(
            self, "RssFeedTable",
            partition_key=dynamodb.Attribute(
                name="id",
                type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(
                name="timestamp",
                type=dynamodb.AttributeType.NUMBER
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.DESTROY  # For dev/test only
        )

        # Lambda function for processing and storing data
        process_lambda = lambda_.Function(
            self, "ProcessDataFunction",
            runtime=lambda_.Runtime.PYTHON_3_13,
            code=lambda_.Code.from_asset("lambda"),
            handler="process_data.handler",
            environment={
                "TABLE_NAME": rss_table.table_name
            },
            timeout=Duration.seconds(30)
        )

        # Lambda function for generating RSS feed
        rss_lambda = lambda_.Function(
            self, "GenerateRssFunction",
            runtime=lambda_.Runtime.PYTHON_3_13,
            code=lambda_.Code.from_asset("lambda"),
            handler="generate_rss.handler",
            environment={
                "TABLE_NAME": rss_table.table_name,
                "FEED_TITLE": "My RSS Feed",
                "FEED_DESCRIPTION": "A dynamically generated RSS feed",
                # "FEED_LINK": "https://example.com"  # Will be updated with actual API URL
            },
            timeout=Duration.seconds(30)
        )

        # Grant permissions
        rss_table.grant_write_data(process_lambda)
        rss_table.grant_read_data(rss_lambda)

        # API Gateway
        api = apigateway.RestApi(
            self, "RssFeedApi",
            rest_api_name="RSS Feed Service",
            description="API for posting data and retrieving RSS feed"
        )

        # Create API key for authentication
        api_key = apigateway.ApiKey(
            self, "RssFeedApiKey",
            api_key_name="rss-feed-api-key",
            description="API key for RSS feed POST endpoint"
        )

        # Create usage plan
        usage_plan = apigateway.UsagePlan(
            self, "RssFeedUsagePlan",
            name="RssFeedUsagePlan",
            api_stages=[{
                "api": api,
                "stage": api.deployment_stage
            }],
            throttle={
                "rate_limit": 100,  # requests per second
                "burst_limit": 200  # burst capacity
            },
            quota={
                "limit": 10000,  # requests per day
                "period": apigateway.Period.DAY
            }
        )

        # Associate API key with usage plan
        usage_plan.add_api_key(api_key)

        # POST endpoint for submitting data (requires API key)
        post_integration = apigateway.LambdaIntegration(
            process_lambda,
            request_templates={
                "application/json": '{ "statusCode": "200" }'
            }
        )
        
        post_resource = api.root.add_resource("post")
        post_method = post_resource.add_method(
            "POST", 
            post_integration,
            api_key_required=True  # Require API key for this endpoint
        )

        # GET endpoint for RSS feed (public, no API key required)
        rss_integration = apigateway.LambdaIntegration(
            rss_lambda,
            request_templates={
                "application/json": '{ "statusCode": "200" }'
            }
        )
        
        rss_resource = api.root.add_resource("rss")
        rss_resource.add_method("GET", rss_integration)

        # Update RSS Lambda with actual API URL
        # rss_lambda.add_environment("FEED_LINK", api.url + "rss")

        # Outputs
        CfnOutput(
            self, "ApiUrl",
            value=api.url,
            description="API Gateway URL"
        )
        
        CfnOutput(
            self, "PostEndpoint",
            value=f"{api.url}post",
            description="POST endpoint for submitting data (requires API key)"
        )
        
        CfnOutput(
            self, "RssEndpoint",
            value=f"{api.url}rss",
            description="RSS feed endpoint (public)"
        )
        
        CfnOutput(
            self, "ApiKeyId",
            value=api_key.key_id,
            description="API Key ID - use 'aws apigateway get-api-key' to retrieve the key value"
        )