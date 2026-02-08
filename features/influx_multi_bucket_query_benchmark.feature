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
      | stress| bddbench_multi_write | 1h         | aggregate  | large       | bddbench_mb_stress| 8            | 3                  | csv           | gzip        |
      | spike | bddbench_multi_write | 5m         | filter     | small       | bddbench_mb_spike | 5            | 5                  | csv           | none        |
      | soak  | bddbench_multi_write | 24h        | aggregate  | large       | bddbench_mb_soak  | 5            | 5                  | csv           | gzip        |

    @experimental
    Examples:
      | id        | measurement           | time_range | query_type | result_size | bucket_prefix           | bucket_count | concurrent_clients | output_format | compression |
      | smoke     | bddbench_multi_write  | 10s        | filter     | small       | bddbench_mb_smoke_exp   | 5            | 5                  | csv           | gzip        |
      | load      | bddbench_multi_write  | 1h         | aggregate  | large       | bddbench_mb_load_exp    | 10           | 10                 | csv           | gzip        |
      | stress    | bddbench_multi_write  | 1h         | aggregate  | large       | bddbench_mb_stress_exp  | 20           | 50                 | csv           | gzip        |
      | spike     | bddbench_multi_write  | 5m         | filter     | small       | bddbench_mb_spike_exp   | 10           | 200                | csv           | none        |
      | soak      | bddbench_multi_write  | 7d         | aggregate  | large       | bddbench_mb_soak_exp    | 10           | 20                 | csv           | gzip        |
      | breakpoint| bddbench_multi_write  | 10m        | filter     | small       | bddbench_mb_breakpoint  | 10           | 300                | csv           | none        |
