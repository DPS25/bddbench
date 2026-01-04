from behave import given
import logging

from behave.runner import Context

logger = logging.getLogger(f"bddbench.influx_query_steps")

@given("a SUT InfluxDB v2 endpoint is configured and reachable")
def step_bucket_from_env(context: Context):
    if not context.influxdb.sut.client.ping():
        logger.error("SUT InfluxDB endpoint is not reachable")
        raise RuntimeError("SUT InfluxDB endpoint is not reachable")


@given("the target bucket from the SUT config is available")
def step_target_bucket_available(context: Context) -> None:
    assert context.influxdb.sut.bucket is not None, (
        "SUT InfluxDB bucket is not configured"
    )
    bucket_api = context.influxdb.sut.client.buckets_api()
    bucket_response = bucket_api.find_bucket_by_name(context.influxdb.sut.bucket)
    assert bucket_response is not None, (
        f"SUT InfluxDB bucket '{context.influxdb.sut.bucket}' is not available"
    )