import os
import datetime as dt
import boto3
from botocore.config import Config

region = os.environ['AWS_REGION']
my_config = Config(region_name=region)
client = boto3.client('batch', config=my_config)


def lambda_handler(event, context):
    job_name = f'job_{dt.datetime.now().strftime("%Y%m%d-%H%M%s")}'
    response = client.submit_job(
        jobName=job_name,
        jobQueue=os.environ['JOB_QUEUE'],
        jobDefinition=dt.datetime.now().strftime('%Y%m%d-%H%M%s')
    )
