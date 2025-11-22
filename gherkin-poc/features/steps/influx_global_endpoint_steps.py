from behave import given
import os

@given("a generic influxDB v2 endpoint is configured from environment")
def step_generic_endpoint_from_env(context):
  url = os.getenv("INFLUX_URL")
  token = os.getenv("INFLUX_TOKEN")
  org = os.getenv("INFLUX_ORG")

  if not url or not token or not org:
      raise RuntimeError("INFLUX_URL, INFLUX_TOKEN, INFLUX_ORG must be set in environment")

  context.influx_orl = url
  context.influx_token = token
  context.influx_org = org

@given("a generic target bucket from environment is available")
def step_generic_bucket_from_env(context):
    bucket = os.getenv("INFLUX_BUCKET")
    if not bucket:
        raise RuntimeError("INFLUX_BUCKET must be set in environment")
    context.influx_bucket = bucket
