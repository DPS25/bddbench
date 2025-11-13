Feature: InfluxDB v2 generic benchmark
  In order to describe reusable performance checks in BDD
  As a test engineer
  I want to run write-benchmarks against InfluxDB v2 with adjustable parameters

  Background:
    Given an InfluxDB v2 endpoint is configured from environment
    And a target bucket from environment is available

  @poc @influx @generic
  Scenario Outline: generic write-latency run
    When I run a generic write benchmark with <points_per_second> points per second for <duration_seconds> seconds using measurement "<measurement>"
    And I store the generic benchmark result as "gherkin-poc/reports/generic-<id>.json"

    Examples:
      | id    | points_per_second | duration_seconds | measurement        |
      | smoke | 200               | 5                | bddbench_generic   |
      | load  | 1000              | 10               | bddbench_generic   |
