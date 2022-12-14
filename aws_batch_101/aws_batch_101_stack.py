import os
from aws_cdk import (
    aws_fsx as fsx,
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
        vpc = ec2.Vpc(
            self,
            "vpc",
            gateway_endpoints={
                "S3": ec2.GatewayVpcEndpointOptions(
                    service=ec2.GatewayVpcEndpointAwsService.S3
                )
            },
            max_azs=1,
        )

        sg = ec2.SecurityGroup(self, "sg", vpc=vpc)

        s3RoleStatement = iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=["s3:*"],
            resources=["*"]
        )

        fsxRoleStatement = iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=["fsx:*"],
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

        fsx_security_group = ec2.SecurityGroup(
            self,
            "FsxSg",
            vpc=vpc,
            allow_all_outbound=True,
            security_group_name="fsx-lustre-sg",
        )

        fsx_security_group.add_ingress_rule(
            peer=ec2.Peer.ipv4(vpc.vpc_cidr_block),
            connection=ec2.Port.tcp(988),
            description="FSx Lustre",
        )

        fsx_filesystem = fsx.CfnFileSystem(
            self,
            "Fsx",
            file_system_type="LUSTRE",
            storage_capacity=1200,
            subnet_ids=[
                vpc.select_subnets(subnet_type=ec2.SubnetType.PRIVATE).subnet_ids[0]
            ],
            security_group_ids=[fsx_security_group.security_group_id],
            tags=[core.CfnTag(key="Name", value="fsx-lustre")],
            lustre_configuration=fsx.CfnFileSystem.LustreConfigurationProperty(
                auto_import_policy="NEW_CHANGED",
                deployment_type="SCRATCH_2",
                data_compression_type="LZ4",
                import_path=f"s3://{bucket.bucket_name}/",
                export_path=f"s3://{bucket.bucket_name}/",
            ),
        )

        instanceRole = iam.Role(
            self, "instance-role",
            assumed_by=iam.ServicePrincipal("ec2.amazonaws.com"),
            managed_policies=[iam.ManagedPolicy.from_aws_managed_policy_name(
                "service-role/AmazonEC2ContainerServiceforEC2Role")],
        )
        instanceRole.add_to_policy(stsAssumeRoleStatement)
        instanceRole.add_to_policy(s3RoleStatement)
        instanceRole.add_to_policy(fsxRoleStatement)

        fsx_directory = '/fsx'
        fsx_user_data = f"""MIME-Version: 1.0
Content-Type: multipart/mixed; boundary="==MYBOUNDARY=="

--==MYBOUNDARY==
Content-Type: text/cloud-config; charset="us-ascii"

runcmd:
- fsx_directory=/fsx
- amazon-linux-extras install -y lustre2.10
- mkdir -p ${{fsx_directory}}
- mount -t lustre {fsx_filesystem.attr_dns_name}@tcp:/{fsx_filesystem.attr_lustre_mount_name} ${{fsx_directory}}

--==MYBOUNDARY==--
"""
        fsx_lt = ec2.CfnLaunchTemplate(
            self,
            "FsxLT",
            launch_template_data=ec2.CfnLaunchTemplate.LaunchTemplateDataProperty(
                user_data=core.Fn.base64(fsx_user_data)
            )
        )

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
            batch.CfnJobDefinition.ResourceRequirementProperty(type='MEMORY', value='4096'),
            batch.CfnJobDefinition.ResourceRequirementProperty(type='VCPU', value='1')
        ]

        container_properties = batch.CfnJobDefinition.ContainerPropertiesProperty(
            command=["main.py", "Ref::bucket", "Ref::key", "Ref::direction"],
            environment=[
                batch.CfnJobDefinition.EnvironmentProperty(
                    name="BATCH_FILE_TYPE", value="zip"),
                batch.CfnJobDefinition.EnvironmentProperty(
                    name="BATCH_FILE_S3_URL", value=script_asset.s3_object_url),
                batch.CfnJobDefinition.EnvironmentProperty(
                    name="FSX_ID", value=fsx_filesystem.ref)
            ],
            image=asset.image_uri,
            resource_requirements=resource_requirement,
            mount_points=[batch.CfnJobDefinition.MountPointsProperty(
                container_path=fsx_directory,
                read_only=False,
                source_volume="fsx"
            )],
            volumes=[
                batch.CfnJobDefinition.VolumesProperty(
                    name="fsx",
                    host=batch.CfnJobDefinition.VolumesHostProperty(source_path=fsx_directory)
                )
            ]
        )

        jobDefinition = batch.CfnJobDefinition(
            self, "job-definition",
            job_definition_name=jobDefinitionName,
            container_properties=container_properties,
            type="Container",
            retry_strategy=batch.CfnJobDefinition.RetryStrategyProperty(
                attempts=3),
            timeout=batch.CfnJobDefinition.TimeoutProperty(
                attempt_duration_seconds=600),
            parameters={
                "key": "no one",
                "bucket": bucket.bucket_name,
                "direction": "upload"
            }
        )
        jobDefinition.add_depends_on(fsx_filesystem)

        computeResources = batch.CfnComputeEnvironment.ComputeResourcesProperty(
            minv_cpus=0,
            desiredv_cpus=0,
            maxv_cpus=32,
            instance_types=[
                "optimal"
            ],
            launch_template=batch.CfnComputeEnvironment.LaunchTemplateSpecificationProperty(
                launch_template_id=fsx_lt.ref,
                version="$Latest"
            ),
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
        computeEnvironment.add_depends_on(fsx_lt)

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

        uploadFunction = lamda.Function(
            self,
            "upload-function",
            function_name="batch-upload-lambda",
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
    if "direction" in event:
        direction = event["direction"]
    else:
        direction = "upload"
    job_name = f"{dt.datetime.now().strftime('%Y%m%d-%H%M%s')}-{uuid.uuid4()}"
    response = client.submit_job(
                        jobName=job_name,
                        jobQueue=os.environ['JOB_QUEUE'],
                        jobDefinition=os.environ['JOB_DEFINITION'],
                        parameters={
                            "key":job_name,
                            "direction" : direction
                        }
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

        # rule = events.Rule(
        #     self,
        #     'event-rule',
        #     schedule=events.Schedule.expression('rate(4 hours)')
        # )
        # rule.add_target(targets.LambdaFunction(uploadFunction))
        downloadFunction = lamda.Function(
            self,
            "download-function",
            function_name="batch-download-lambda",
            code=lamda.Code.from_inline('''
import os
import datetime as dt
import boto3
from botocore.config import Config

region = os.environ['AWS_REGION']
my_config = Config(region_name=region)
client = boto3.client('batch', config=my_config)
s3 = boto3.resource("s3", config=my_config)
bucket = s3.Bucket(os.environ['BUCKET_NAME'])

def lambda_handler(event, context):
    if "direction" in event:
        direction = event["direction"]
    else:
        direction = "download"
    for key in bucket.objects.all():
        job_name = f"download-{dt.datetime.now().strftime('%Y%m%d-%H%M%s')}-{key.key}"
        response = client.submit_job(
                            jobName=job_name,
                            jobQueue=os.environ['JOB_QUEUE'],
                            jobDefinition=os.environ['JOB_DEFINITION'],
                            parameters={
                                "key": key.key,
                                "direction" : direction
                            }
        )
        print(response)
'''),
            handler="index.lambda_handler",
            timeout=core.Duration.seconds(60),
            runtime=lamda.Runtime.PYTHON_3_9,
            environment={
                "BUCKET_NAME": bucket.bucket_name,
                "JOB_DEFINITION": jobDefinitionName,
                "JOB_QUEUE": jobQueueName
            },
            initial_policy=[jobSubmitStatement, s3RoleStatement]
        )
