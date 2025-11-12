Feature: Basic InfluxDB write and read-back benchmark
  Background:
    Given an SUT InfluxDB v2 endpoint is configured and reachable
    And the target bucket bddbench from environment is available
