Feature: InfluxDB v2 multi-bucket write benchmark (/api/v2/write)
  In order to evaluate write performance with multiple buckets
  As a test engineer
  I want to create multiple buckets and write into them for a defined time

  Background:
    Given a generic InfluxDB v2 endpoint is configured from environment
    And a generic target bucket from environment is available

  @poc @influx @write @multibucket
  Scenario Outline: multi-bucket write benchmark run (duration based)
    When I run a multi-bucket write benchmark on base measurement "<measurement>" with bucket prefix "<bucket_prefix>" creating <bucket_count> buckets, batch size <batch_size>, <parallel_writers> parallel writers per bucket, compression "<compression>", timestamp precision "<precision>", point complexity "<point_complexity>", tag cardinality <tag_cardinality> and time ordering "<time_ordering>" for <duration_s> seconds
    Then I store the multi-bucket write benchmark result as "reports/multi-write-<id>.json"

    Examples:
      | id    | measurement         | bucket_prefix     | bucket_count | batch_size | parallel_writers | compression | precision | point_complexity | tag_cardinality | time_ordering | duration_s |
      | smoke | bddbench_multiwrite | bddbench_mb_smoke | 3            | 100        | 1                | none        | ns        | low              | 10              | in_order      | 10         |
      | load  | bddbench_multiwrite | bddbench_mb_load  | 10           | 1000       | 2                | gzip        | ns        | high             | 1000            | out_of_order  | 60         |
