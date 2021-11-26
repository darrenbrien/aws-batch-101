import os
from aws_cdk import (
    aws_iam as iam,
    aws_s3_assets as s3_assets,
    aws_s3 as s3,
    aws_batch as batch,
    aws_ec2 as ec2,
    aws_lambda as lamda,
    aws_events as events,
    aws_events_targets as targets,
    core
)
from aws_cdk.aws_ecr_assets import DockerImageAsset

functionName = "batch-lambda"
jobDefinitionName = "job-definition"
computeEnvironmentName = "compute-environment"
jobQueueName = "job-queue"
cwd = os.getcwd()


class AwsBatch101Stack(core.Stack):

    def __init__(self, scope: core.Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        cdk_bucket = core.DefaultStackSynthesizer.DEFAULT_FILE_ASSETS_BUCKET_NAME
        script_asset = s3_assets.Asset(
            self,
            "bundled_asset",
            path=os.path.join(cwd, "docker", "script")
        )
        bucket = s3.Bucket(self, id="batch")
        vpc = ec2.Vpc(self, "vpc", max_azs=3)

        sg = ec2.SecurityGroup(self, "sg", vpc=vpc)

        s3RoleStatement = iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=["s3:*"],
            resources=["*"]
        )

        stsAssumeRoleStatement = iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=["sts:AssumeRole"],
            resources=["*"]
        )

        jobSubmitStatement = iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=["batch:SubmitJob"],
            resources=["*"]
        )

        spotServiceRole = iam.Role(
            self, "spot-service-role",
            assumed_by=iam.ServicePrincipal("spotfleet.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AmazonEC2SpotFleetTaggingRole")
            ]
        )

        spotServiceRole.add_to_policy(stsAssumeRoleStatement)

        batchServiceRole = iam.Role(
            self, "service-role",
            assumed_by=iam.ServicePrincipal("batch.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name(
                    "service-role/AWSBatchServiceRole")
            ]
        )

        batchServiceRole.add_to_policy(stsAssumeRoleStatement)

        instanceRole = iam.Role(
            self, "instance-role",
            assumed_by=iam.ServicePrincipal("ec2.amazonaws.com"),
            managed_policies=[iam.ManagedPolicy.from_aws_managed_policy_name(
                "service-role/AmazonEC2ContainerServiceforEC2Role")],
        )
        instanceRole.add_to_policy(stsAssumeRoleStatement)
        instanceRole.add_to_policy(s3RoleStatement)

        asset = DockerImageAsset(self,
                                 "MyBuildImage",
                                 directory=os.path.join(cwd, "docker")
                                 )
        instanceProfile = iam.CfnInstanceProfile(
            self, "instance-profile",
            instance_profile_name="instance-profile",
            roles=[instanceRole.role_name]
        )
        resource_requirement = [
            batch.CfnJobDefinition.ResourceRequirementProperty(type='MEMORY', value='7168'),
            batch.CfnJobDefinition.ResourceRequirementProperty(type='VCPU', value='1')
        ]

        container_properties = batch.CfnJobDefinition.ContainerPropertiesProperty(
            command=["main.py", "Ref::bucket", "Ref::job_name"],
            environment=[
                batch.CfnJobDefinition.EnvironmentProperty(
                    name="BATCH_FILE_TYPE", value="zip"),
                batch.CfnJobDefinition.EnvironmentProperty(
                    name="BATCH_FILE_S3_URL", value=script_asset.s3_object_url)
            ],
            image=asset.image_uri,
            resource_requirement=resource_requirement
        )

        jobDefinition = batch.CfnJobDefinition(
            self, "job-definition",
            job_definition_name=jobDefinitionName,
            container_properties=container_properties,
            type="Container",
            retry_strategy=batch.CfnJobDefinition.RetryStrategyProperty(
                attempts=3),
            timeout=batch.CfnJobDefinition.TimeoutProperty(
                attempt_duration_seconds=60),
            parameters={
                "job_name": "no one",
                "bucket": bucket.bucket_name
            }
        )

        computeResources = batch.CfnComputeEnvironment.ComputeResourcesProperty(
            minv_cpus=0,
            desiredv_cpus=0,
            maxv_cpus=32,
            instance_types=[
                "optimal"
            ],
            instance_role=instanceProfile.attr_arn,
            spot_iam_fleet_role=spotServiceRole.role_arn,
            type="SPOT",
            subnets=[i.subnet_id for i in vpc.public_subnets],
            security_group_ids=[sg.security_group_id]
        )

        computeEnvironment = batch.CfnComputeEnvironment(
            self, "compute-environment",
            compute_environment_name=computeEnvironmentName,
            compute_resources=computeResources,
            service_role=batchServiceRole.role_arn,
            type="MANAGED",
            state="ENABLED"
        )
        computeEnvironment.add_depends_on(instanceProfile)

        jobQueue = batch.CfnJobQueue(
            self,
            "job-queue",
            job_queue_name=jobQueueName,
            priority=1,
            state="ENABLED",
            compute_environment_order=[
                batch.CfnJobQueue.ComputeEnvironmentOrderProperty(
                    order=1,
                    compute_environment=computeEnvironment.compute_environment_name)
            ]
        )
        jobQueue.add_depends_on(computeEnvironment)

        lambdaFunction = lamda.Function(
            self,
            "lambda-function",
            function_name=functionName,
            code=lamda.Code.from_inline('''
import os
import uuid
import datetime as dt
import boto3
from botocore.config import Config

region = os.environ['AWS_REGION']
my_config = Config(region_name=region)
client = boto3.client('batch', config=my_config)

def lambda_handler(event, context):
    job_name = f"{dt.datetime.now().strftime('%Y%m%d-%H%M%s')}-{uuid.uuid4()}"
    response = client.submit_job(
                        jobName=job_name,
                        jobQueue=os.environ['JOB_QUEUE'],
                        jobDefinition=os.environ['JOB_DEFINITION'],
                        parameters={"job_name":job_name}
    )
    print(response)
'''),
            handler="index.lambda_handler",
            timeout=core.Duration.seconds(30),
            runtime=lamda.Runtime.PYTHON_3_9,
            environment={
                "JOB_DEFINITION": jobDefinitionName,
                "JOB_QUEUE": jobQueueName},
            initial_policy=[jobSubmitStatement]
        )

        rule = events.Rule(
            self,
            'event-rule',
            schedule=events.Schedule.expression('rate(4 hours)')
        )
        rule.add_target(targets.LambdaFunction(lambdaFunction))
