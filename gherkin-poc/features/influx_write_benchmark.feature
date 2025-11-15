Feature: InfluxDB v2 write benchmark (/api/v2/write)
  In order to evaluate write performance characteristics
  As a test engineer
  I want to run write benchmarks against InfluxDB v2 with adjustable parameters

  Background:
    Given a generic InfluxDB v2 endpoint is configured from environment
    And a generic target bucket from environment is available

  @poc @influx @write
  Scenario Outline: generic write benchmark run (batch + parallel)
    When I run a write benchmark on measurement "<measurement>" with batch size <batch_size>, <parallel_writers> parallel writers, compression "<compression>", timestamp precision "<precision>", point complexity "<point_complexity>", tag cardinality <tag_cardinality> and time ordering "<time_ordering>" for <batches> batches
    Then I store the write benchmark result as "reports/write-<id>.json"

    Examples:
      | id    | measurement        | batch_size | parallel_writers | compression | precision | point_complexity | tag_cardinality | time_ordering | batches |
      | smoke | bddbench_write_poc | 100        | 1                | none        | ns        | low              | 10              | in_order      | 10      |
      | load  | bddbench_write_poc | 1000       | 4                | gzip        | ns        | high             | 1000            | out_of_order  | 50      |
