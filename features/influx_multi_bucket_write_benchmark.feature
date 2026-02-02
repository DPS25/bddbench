@write @multibucket
Feature: InfluxDB v2 multi-bucket write benchmark (/api/v2/write)
  In order to evaluate write performance with multiple buckets
  As a test engineer
  I want to create multiple buckets and write into them for a defined time

  Background:
    Given a SUT InfluxDB v2 endpoint is configured and reachable
    And the target bucket from the SUT config is available

  Scenario Outline: multi-bucket write benchmark run (duration based)
    When I run a multi-bucket write benchmark on base measurement "<measurement>" with bucket prefix "<bucket_prefix>" creating <bucket_count> buckets, batch size <batch_size>, <parallel_writers> parallel writers per bucket, compression "<compression>", timestamp precision "<precision>", point complexity "<point_complexity>", tag cardinality <tag_cardinality> and time ordering "<time_ordering>" for <duration_s> seconds
    Then I store the multi-bucket write benchmark result as "reports/multi-write-<id>.json"
    And I store the multi-bucket write benchmark context as "reports/multi-write-context-<id>.json"

    @normal
    Examples:
      | id    | measurement           | bucket_prefix      | bucket_count | batch_size | parallel_writers | compression | precision | point_complexity | tag_cardinality | time_ordering | duration_s |
      | smoke | bddbench_multi_write  | bddbench_mb_smoke  | 3            | 100        | 1                | none        | ns        | low              | 10              | in_order      | 10         |
      | load  | bddbench_multi_write  | bddbench_mb_load   | 5            | 250        | 2                | none        | ns        | medium           | 100             | in_order      | 20         |
      | stress| bddbench_multi_write  | bddbench_mb_stress | 10           | 1000       | 8                | none        | ns        | high             | 2000            | in_order      | 60         |
      | spike | bddbench_multi_write  | bddbench_mb_spike  | 10           | 250        | 32               | none        | ns        | medium           | 100             | in_order      | 10         |
      | soak  | bddbench_multi_write  | bddbench_mb_soak   | 5            | 250        | 4                | none        | ns        | medium           | 100             | in_order      | 300        |

    @experimental
    Examples:
      | id        | measurement           | bucket_prefix           | bucket_count | batch_size | parallel_writers | compression | precision | point_complexity | tag_cardinality | time_ordering | duration_s |
      | smoke     | bddbench_multi_write  | bddbench_mb_smoke_exp   | 5           | 250         | 2                | gzip        | ns        | medium           | 100             | out_of_order    | 15       |
      | load      | bddbench_multi_write  | bddbench_mb_exp         | 10          | 1000        | 2                | gzip        | ns        | high             | 1000            | out_of_order    | 60       |
      | stress    | bddbench_multi_write  | bddbench_mb_stress_exp  | 20          | 2000        | 12               | gzip        | ns        | high             | 5000            | out_of_order    | 60       |
      | spike     | bddbench_multi_write  | bddbench_mb_spike_exp   | 20          | 250         | 64               | gzip        | ns        | medium           | 200             | out_of_order    | 10       |
      | soak      | bddbench_multi_write  | bddbench_mb_soak_exp    | 10          | 500         | 4                | gzip        | ns        | medium           | 200             | out_of_order    | 600      |
      | breakpoint| bddbench_multi_write  | bddbench_mb_breakpoint  | 10          | 1000        | 32               | gzip        | ns        | high             | 10000           | out_of_order    | 120      |
