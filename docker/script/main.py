#!/usr/bin/env python
import time
import logging
import jax.numpy as jnp
from io import BytesIO
import jax.random as random
import boto3
import sys


class Throughput:
    def __init__(self, format_string):
        self._ttl_bytes_ = 0
        self._start_time_ = time.perf_counter()
        self._bytes_increment_ = 2**28
        self._format_string_ = "{ttl_bytes / 2**20:.1f}MiB " + format_string

    def transfer(self, b):
        self.ttl_bytes += b
        if b % self._bytes_increment_ == 0:
            logging.info(self._format_string_.format(ttl_bytes=self._ttl_bytes_))

    def _enter_(self):
        return self

    def _exit_(self, exec_type, exec_value, traceback):
        elapsed = time.perf_counter() - self._start_time_
        ttl_mib = self._ttl_bytes_ / 2**20
        logging.info(f"{ttl_mib}MiB in {elapsed:.1f} secs, {ttl_mib/elapsed:.1f}MiB/Sec")


def download(client, bucket, key):
    bytes_ = BytesIO()
    with Throughput(f"downloaded from s3://{bucket}/{key}") as thr:
        client.download_fileobj(
            Fileobj=bytes_,
            Bucket=bucket,
            Key=key,
            Callback=thr.transfer
        )
    data = jnp.load(bytes_)
    logging.info(f"Array of size {data.shape}")


def upload(client, bucket, key):
    seed = 19590414
    rng_key = random.PRNGKey(seed)
    columns = 1000
    rows = 500000
    data = random.normal(key=rng_key, shape=(rows, columns))
    bytes_ = BytesIO()
    jnp.save(bytes_, data, allow_pickle=False)
    bytes_.seek(0)

    with Throughput(f"uploaded to s3://{bucket}/{key}") as thr:
        client.upload_fileobj(
            Fileobj=bytes_,
            Bucket=bucket,
            Key=key,
            Callback=thr.transfer
        )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
    try:
        _, bucket, key, *rest = sys.argv
        client = boto3.client("s3")
        if rest and rest[0] == "download":
            download(client, bucket, key)
        else:
            upload(client, bucket, key)
    except Exception:
        logging.exception("Failed to transfer data to/from bucket")
