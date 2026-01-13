@delete
Feature: InfluxDB v2 delete benchmark (/api/v2/delete)
    In order to evaluate delete performance characteristics
    As a test engineer
    I want to run delete benchmarks against InfluxDB v2 on existing data
    Background:
        Given a SUT InfluxDB v2 endpoint is configured and reachable
        And the target bucket from the SUT config is available

    @delete_smoke
    Scenario: delete only SMOKE dataset (measurement + run_id)
        Given I load the write benchmark context from "reports/write-context-smoke.json"
        When I delete all points for measurement "bddbench_single_write" in the SUT bucket
        Then no points for measurement "bddbench_single_write" shall remain in the SUT bucket
        And I store the generic delete benchmark result as "reports/delete-smoke.json"

    @delete_load
    Scenario: delete only LOAD dataset (measurement + run_id)
        Given I load the write benchmark context from "reports/write-context-load.json"
        When I delete all points for measurement "bddbench_single_write" in the SUT bucket
        Then no points for measurement "bddbench_single_write" shall remain in the SUT bucket
        And I store the generic delete benchmark result as "reports/delete-load.json"

    @delete_all
    Scenario: delete EVERYTHING in the bucket
        When I delete ALL points in the SUT bucket
