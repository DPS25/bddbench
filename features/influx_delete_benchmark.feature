Feature: InfluxDB v2 delete benchmark (/api/v2/delete)
    In order to evaluate delete performance characteristics
    As a test engineer
    I want to run delete benchmarks against InfluxDB v2 on existing data

    Background:
        Given a SUT InfluxDB v2 endpoint is configured and reachable
        And the target bucket from the SUT config is available

    @influx @delete
    Scenario Outline: generic delete benchmark run for a measurement
        When I delete all points for measurement "<measurement>" in the SUT bucket
        Then the delete duration shall be <= <max_delete_ms> ms
        And no points for measurement "<measurement>" shall remain in the SUT bucket
        And I store the generic delete benchmark result as "reports/delete-<id>.json"

        Examples:
            | id         | measurement          | max_delete_ms |
            | smoke      | bddbench_write_poc   | 500           |
            | heavy_load | bddbench_write_load  | 2000          |
