@delete
Feature: InfluxDB v2 delete benchmark (/api/v2/delete)
    In order to evaluate delete performance characteristics
    As a test engineer
    I want to run delete benchmarks against InfluxDB v2 on existing data

    Background:
        Given a SUT InfluxDB v2 endpoint is configured and reachable
        And the target bucket from the SUT config is available

    Scenario Outline: generic delete benchmark run for a measurement
        Given I load the write benchmark context from "<write_context_file>"
        When I delete all points for measurement "<measurement>" in the SUT bucket
        Then no points for measurement "<measurement>" shall remain in the SUT bucket
        And I store the generic delete benchmark result as "reports/delete-<id>.json"

        @normal
        Examples:
            | id    | measurement          |  write_context_file                    |
            | smoke | bddbench_single_delete   | reports/write-context-smoke.json      |
            | load  | bddbench_single_delete  |  reports/write-context-load.json |
