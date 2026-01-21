@query @multibucket
Feature: InfluxDB v2 multi-bucket query benchmark (/api/v2/query)
  In order to evaluate query performance across multiple buckets
  As a test engineer
  I want to run query benchmarks against multiple buckets in parallel

  Background:
    Given a SUT InfluxDB v2 endpoint is configured and reachable

  Scenario Outline: generic multi-bucket query benchmark run
    When I run a generic multi-bucket query benchmark on measurement "<measurement>" with time range "<time_range>" using query type "<query_type>" and result size "<result_size>" against bucket prefix "<bucket_prefix>" querying <bucket_count> buckets with <concurrent_clients> concurrent clients per bucket, output format "<output_format>" and compression "<compression>"
    Then I store the generic multi-bucket query benchmark result as "reports/multi-query-<id>.json"

    @normal
    Examples:
      | id    | measurement          | time_range | query_type | result_size | bucket_prefix     | bucket_count | concurrent_clients | output_format | compression |
      | smoke | bddbench_multi_write | 10s        | filter     | small       | bddbench_mb_smoke | 3            | 2                  | csv           | none        |
      | load  | bddbench_multi_write | 1h         | aggregate  | large       | bddbench_mb_load  | 5            | 2                  | csv           | gzip        |
