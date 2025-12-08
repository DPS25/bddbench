Feature: InfluxDB v2 multi-bucket write benchmark (/api/v2/write)
  In order to evaluate write performance with multiple buckets
  As a test engineer
  I want to create multiple buckets and write into them for a defined time

  Background:
    Given a SUT InfluxDB v2 endpoint is configured and reachable
    And the target bucket from the SUT config is available

  @influx @write @multibucket
  Scenario Outline: multi-bucket write benchmark run (duration based)
    When I run a multi-bucket write benchmark on base measurement "<measurement>" with bucket prefix "<bucket_prefix>" creating <bucket_count> buckets, batch size <batch_size>, <parallel_writers> parallel writers per bucket, compression "<compression>", timestamp precision "<precision>", point complexity "<point_complexity>", tag cardinality <tag_cardinality> and time ordering "<time_ordering>" for <duration_s> seconds
    Then I store the multi-bucket write benchmark result as "reports/multi-write-<id>.json"

    Examples:
      | id    | measurement         | bucket_prefix     | bucket_count | batch_size | parallel_writers | compression | precision | point_complexity | tag_cardinality | time_ordering | duration_s |
      | smoke | bddbench_multiwrite | bddbench_mb_smoke | 3            | 100        | 1                | none        | ns        | low              | 10              | in_order      | 10         |
      | load  | bddbench_multiwrite | bddbench_mb_load  | 5            | 250        | 2                | none        | ns        | medium           | 100             | in_order      | 20         |
