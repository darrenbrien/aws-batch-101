#!/usr/bin/env python
import logging
import jax.numpy as jnp
from io import BytesIO
import jax.random as random
import boto3
import sys


def main(bucket, key):
    client = boto3.client("s3")
    seed = 19590414
    rng_key = random.PRNGKey(seed)
    columns = 1000
    rows = 500000
    data = random.normal(key=rng_key, shape=(rows, columns))
    bytes_ = BytesIO()
    jnp.save(bytes_, data, allow_pickle=False)
    bytes_.seek(0)
    client.upload_fileobj(
        Fileobj=bytes_,
        Bucket=bucket,
        Key=key,
        Callback=lambda b: logging.info(f"{b} bytes uploaded to s3://{bucket}/{key}")
    )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
    try:
        _, bucket, key, *rest = sys.argv
        logging.info('This message will be logged')
        main(bucket, key)
    except Exception:
        logging.exception("Failed to save data to bucket")
