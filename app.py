#!/usr/bin/env python3

from aws_cdk import core

from aws_batch_101.aws_batch_101_stack import AwsBatch101Stack


app = core.App()
AwsBatch101Stack(app, "aws-batch-101")

app.synth()
