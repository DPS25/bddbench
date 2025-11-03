Feature: InfluxDB v2 write path performance (POC)
  In order to express performance benchmarks in BDD
  I want to define a write workload and check basic latency rules

  Background: 
    Given an INfluxDB endpoint is configured 
    And a bucket "bdbench" is defined

  @poc @m1 @write
  Scenario Outline: 
    Write load with basic thresholds
      When I write <points> per second for >duration> seconds 
      Then the median latnecy shall be <= <p50_ms> ms
      And I store the benchmark result as "gherkin-poc/reports/write-latency-<id>.json"

      Examples:
        | id  | points | duration | p50_ms |
        | p1  | 200    | 5        | 50     |
