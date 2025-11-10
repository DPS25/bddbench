Feature: InfluxDB v2 write path performance (POC)
  In order to express performance benchmarks in BDD
  As the team
  I want to define a write workload and check basic latency rules

  Background:
    Given an InfluxDB endpoint is configured
    And a bucket "dsp25" is defined

# no @Influx to test Gherkin features!
  @poc @m1 @write 
  Scenario Outline: Write load with basic thresholds
    When I write <points> points per second for <duration> seconds
    Then the median latency shall be <= <p50_ms> ms
    And I store the benchmark result as "gherkin-poc/reports/write-latency-<id>.json"

    Examples:
      | id  | points | duration | p50_ms |
      | p1  | 200    | 5        | 50     |
