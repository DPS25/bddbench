import logging
import os
import time
import uuid
import statistics

from behave import given, when, then
from behave.runner import Context
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

@given("an SUT InfluxDB v2 endpoint is configured and reachable")
def step_bucket_from_env(context):
    if not context.sut_influx.client.ping():
        logging.error("SUT InfluxDB endpoint is not reachable")
        raise RuntimeError("SUT InfluxDB endpoint is not reachable")

@given("the target bucket '{bucket}' from environment is available")
def step_target_bucket_available(context: Context, bucket: str) -> None:
    assert context.sut_influx.bucket is not None, "SUT InfluxDB bucket is not configured"
    bucket_api = context.sut_influx.client.buckets_api()
    bucket_response = bucket_api.find_bucket_by_name(bucket)
    assert bucket_response is not None, f"SUT InfluxDB bucket '{bucket}' is not available"

