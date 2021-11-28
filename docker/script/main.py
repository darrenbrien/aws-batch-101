#!/usr/bin/env python
import time
import random
import logging
from io import BytesIO
from hashlib import sha256
import boto3
import sys


class CallbackStream(object):
    """Wraps a file-like object in another, but also calls a user
    callback with the number of bytes read whenever its `read()` method
    is called. Used for tracking upload progress, for example for a
    progress bar in a UI application. Idea taken from ActiveState Code Recipe:
    http://code.activestate.com/recipes/578669-wrap-a-string-in-a-file-like-object-that-calls-a-u/
    """

    def __init__(self, file_like, callback):
        self.file_like = file_like
        self._callback = callback

    def __len__(self):
        raise NotImplementedError()

    def read(self, *args):
        chunk = self.file_like.read(*args)
        if len(chunk) > 0:
            self._callback(len(chunk))
        return chunk

    def write(self, bytes_, *args):
        chunk = self.file_like.write(bytes_, *args)
        if chunk > 0:
            self._callback(chunk)
        return chunk


class Throughput:
    def __init__(self, direction):
        self._ttl_bytes_ = 0
        self._start_time_ = time.perf_counter()
        self._bytes_increment_ = 2**28
        self._direction_ = direction

    def transfer(self, b):
        self._ttl_bytes_ += b
        if self._ttl_bytes_ % self._bytes_increment_ == 0:
            logging.info(f"{self._ttl_bytes_ / 2 ** 20:.1f}MiB {self._direction_}")

    def __enter__(self):
        return self

    def __exit__(self, exec_type, exec_value, traceback):
        elapsed = time.perf_counter() - self._start_time_
        ttl_mib = self._ttl_bytes_ / 2**20
        logging.info(f"{ttl_mib:.1f}MiB in {elapsed:.1f} secs, {ttl_mib/elapsed:.1f}MiB/Sec")


def download(client, bucket, key):
    bytes_ = BytesIO()
    with Throughput(f"downloaded from s3://{bucket}/{key}") as thr:
        client.download_fileobj(
            Fileobj=bytes_,
            Bucket=bucket,
            Key=key,
            Callback=thr.transfer
        )
    h1 = sha256()
    h1.update(bytes_.read())
    aux = h1.digest()
    logging.info(f"SHA256 {aux}")


def get_data(num_bytes):
    blocks = 2**16
    ttl_blocks = int(num_bytes // blocks)
    bytes_ = BytesIO(
        bytes.join(
            b'',
            [random.getrandbits(8 * blocks).to_bytes(blocks, sys.byteorder)] * ttl_blocks)
    )
    return bytes_


def upload(client, bucket, key):
    bytes_ = get_data(3.5 * 1024**3)
    with Throughput(f"uploaded to s3://{bucket}/{key}") as thr:
        client.upload_fileobj(
            Fileobj=bytes_,
            Bucket=bucket,
            Key=key,
            Callback=thr.transfer
        )


def read(key):
    with open(f'/fsx/{key}', mode='rb') as file, Throughput(f"read from fsx://{key}") as thr:
        h1 = sha256()
        cs = CallbackStream(file, thr.transfer)
        data = cs.read()
    h1.update(data)
    aux = h1.digest()
    logging.info(f"SHA256 {aux}")


def write(key):
    length = int(3.5 * 1024**3)
    bytes_ = get_data(length)
    with open(f'/fsx/{key}', mode='wb') as file, Throughput(f"write to fsx://{key}") as thr:
        cs = CallbackStream(file, thr.transfer)
        cs.write(bytes_.read())


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(levelname)s %(message)s')
    try:
        _, bucket, key, rest, *_ = sys.argv
        client = boto3.client("s3")
        if rest == "download":
            download(client, bucket, key)
        elif rest == "upload":
            upload(client, bucket, key)
        elif rest == "fsx-up":
            write(key)
        else:
            read(key)
    except Exception:
        logging.exception("Failed to transfer data to/from bucket")
