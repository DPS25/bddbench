@delete
Feature: InfluxDB v2 delete benchmark (/api/v2/delete)
    In order to evaluate delete performance characteristics
    As a test engineer
    I want to run delete benchmarks against InfluxDB v2 on existing data

    Background:
        Given a SUT InfluxDB v2 endpoint is configured and reachable
        And the target bucket from the SUT config is available

    Scenario: delete benchmark (smoke+load by default, bucket wipe with -D delete_all=true)
        When I run the delete benchmark
