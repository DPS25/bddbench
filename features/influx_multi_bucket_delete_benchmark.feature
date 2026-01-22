@delete @multibucket
Feature: InfluxDB v2 multi-bucket delete benchmark (/api/v2/delete)
    In order to evaluate delete performance across multiple buckets
    As a test engineer
    I want to delete multi-bucket benchmark data written by the multi-bucket write benchmark

    Background:
        Given a SUT InfluxDB v2 endpoint is configured and reachable
        And the target bucket from the SUT config is available

    Scenario: multi-bucket delete benchmark (smoke+load by default, bucket-set wipe with -D delete_all=true)
        When I run the multi-bucket delete benchmark
