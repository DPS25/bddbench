Feature: InfluxDB v2 write + delete benchmark
  In order to evaluate delete performance under realistic load
  As a test engineer
  I want to delete the points that were written by the generic write benchmark with the same settings

  Background:
    Given a generic InfluxDB v2 endpoint is configured from environment
    And a generic target bucket from environment is available

  @influx @write @delete
  Scenario Outline: generic write benchmark followed by delete (same params)
    When I run a generic write benchmark on measurement "<measurement>" with batch size <batch_size>, <parallel_writers> parallel writers, compression "<compression>", timestamp precision "<precision>", point complexity "<point_complexity>", tag cardinality <tag_cardinality> and time ordering "<time_ordering>" for <batches> batches
    And I delete all points for this generic write benchmark
    Then the delete duration shall be <= <max_ms> ms
    And no points for this generic write benchmark shall remain

    Examples:
      | id    | measurement        | batch_size | parallel_writers | compression | precision | point_complexity | tag_cardinality | time_ordering | batches | max_ms |
      | smoke | bddbench_write_poc | 100        | 1                | none        | ns        | low              | 10              | in_order      | 10      | 5000   |
      | load  | bddbench_write_poc | 1000       | 4                | gzip        | ns        | high             | 1000            | out_of_order  | 50      | 20000  |
