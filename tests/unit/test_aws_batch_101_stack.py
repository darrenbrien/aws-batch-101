from aws_cdk import (
        core,
        assertions
    )

from aws_batch_101.aws_batch_101_stack import AwsBatch101Stack


def test_sqs_queue_created():
    app = core.App()
    stack = AwsBatch101Stack(app, "aws-batch-101")
    template = assertions.Template.from_stack(stack)

    template.has_resource_properties("AWS::SQS::Queue", {
        "VisibilityTimeout": 300
    })


def test_sns_topic_created():
    app = core.App()
    stack = AwsBatch101Stack(app, "aws-batch-101")
    template = assertions.Template.from_stack(stack)

    template.resource_count_is("AWS::SNS::Topic", 1)
