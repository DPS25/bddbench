@influx @e2e
Feature: InfluxDB v2 end-to-end benchmark (bucket create -> write -> query -> delete)
  Create buckets, write data, query data and delete data to simulate WQD lifecycle.

  Background:
    Given a SUT InfluxDB v2 endpoint is configured and reachable
    
  @singlebucket 
  Scenario Outline: single bucket WQD E2E (normal) - <profile>
    When I run an end-to-end benchmark with bucket prefix "<bucket_prefix>" on base measurement "<measurement>" with <bucket_count> buckets, write batch size <batch_size>, <parallel_writers> parallel writers, write compression "<write_compression>", timestamp precision "<precision>", point complexity "<point_complexity>", tag cardinality <tag_cardinality> and time ordering "<time_ordering>" for <batches> batches, expecting <expected_points_per_bucket> points per bucket and <expected_total_points> total points, then query with time range "<time_range>", query type "<query_type>", result size "<result_size>", <concurrent_clients> concurrent clients, <query_repeats> repeats per client, output format "<output_format>" and query compression "<query_compression>", and finally delete all written points
    Then I store the end-to-end benchmark result as "reports/e2e-<id>.json"

    # ==========================================================
    # SINGLE BUCKET WQD - NORMAL
    # ==========================================================
    @normal
    Examples:
      | id         | bucket_prefix | measurement  | bucket_count | batch_size | parallel_writers | batches | expected_points_per_bucket | expected_total_points | write_compression | precision | point_complexity | tag_cardinality | time_ordering | time_range | query_type | result_size | concurrent_clients | query_repeats | output_format | query_compression |
      | smoke      | bddbench_e2e  | bddbench_e2e | 1            | 100        | 1                | 10      | 1000                       | 1000                  | none              | ns        | low              | 10              | in_order      | 10s        | filter     | small       | 1                  | 1             | csv           | none              |
      | average    | bddbench_e2e  | bddbench_e2e | 1            | 250        | 2                | 10      | 5000                       | 5000                  | none              | ns        | medium           | 100             | in_order      | 1h         | aggregate  | small       | 2                  | 2             | csv           | gzip              |
      | stress     | bddbench_e2e  | bddbench_e2e | 1            | 500        | 2                | 20      | 20000                      | 20000                 | none              | ns        | medium           | 250             | in_order      | 6h         | aggregate  | large       | 4                  | 2             | csv           | gzip              |
      | spike      | bddbench_e2e  | bddbench_e2e | 1            | 250        | 4                | 10      | 10000                      | 10000                 | none              | ns        | medium           | 250             | in_order      | 1h         | filter     | large       | 8                  | 1             | csv           | gzip              |
      | break      | bddbench_e2e  | bddbench_e2e | 1            | 1000       | 4                | 25      | 100000                     | 100000                | none              | ns        | high             | 500             | in_order      | 1h         | aggregate  | large       | 8                  | 1             | csv           | gzip              |
      | soak       | bddbench_e2e  | bddbench_e2e | 1            | 100        | 1                | 300     | 30000                      | 30000                 | none              | ns        | low              | 100             | in_order      | 72h        | filter     | small       | 2                  | 5             | csv           | none              |

    # ==========================================================
    # SINGLE BUCKET WQD - EXPERIMENTAL 
    # ==========================================================
    @experimental
    Examples:
      | id          | bucket_prefix | measurement  | bucket_count | batch_size | parallel_writers | batches | expected_points_per_bucket | expected_total_points | write_compression | precision | point_complexity | tag_cardinality | time_ordering  | time_range | query_type | result_size | concurrent_clients | query_repeats | output_format | query_compression |
      | smoke       | bddbench_e2e  | bddbench_e2e | 1            | 200        | 1                | 5       | 1000                       | 1000                  | none              | ms        | low              | 10              | out_of_order   | 1h         | group_by   | small       | 1                  | 1             | csv           | none              |
      | average     | bddbench_e2e  | bddbench_e2e | 1            | 500        | 1                | 20      | 10000                      | 10000                 | gzip              | ms        | medium           | 250             | out_of_order   | 6h         | pivot      | small       | 2                  | 2             | csv           | gzip              |
      | stress      | bddbench_e2e  | bddbench_e2e | 1            | 1000       | 2                | 50      | 100000                     | 100000                | gzip              | ns        | high             | 2000            | out_of_order   | 24h        | pivot      | large       | 4                  | 2             | csv           | gzip              |
      | spike       | bddbench_e2e  | bddbench_e2e | 1            | 2000       | 4                | 10      | 80000                      | 80000                 | none              | ns        | medium           | 1000            | out_of_order   | 1h         | group_by   | large       | 8                  | 2             | csv           | none              |
      | break       | bddbench_e2e  | bddbench_e2e | 1            | 5000       | 4                | 10      | 200000                     | 200000                | gzip              | ns        | high             | 5000            | out_of_order   | 1h         | pivot      | large       | 12                 | 1             | csv           | gzip              |
      | soak        | bddbench_e2e  | bddbench_e2e | 1            | 300        | 1                | 400     | 120000                     | 120000                | none              | ms        | low              | 500             | out_of_order   | 72h        | filter     | small       | 2                  | 4             | csv           | none              |

  @multibucket 
  Scenario Outline: multi bucket WQD E2E (normal) - <profile>
    When I run an end-to-end benchmark with bucket prefix "<bucket_prefix>" on base measurement "<measurement>" with <bucket_count> buckets, write batch size <batch_size>, <parallel_writers> parallel writers, write compression "<write_compression>", timestamp precision "<precision>", point complexity "<point_complexity>", tag cardinality <tag_cardinality> and time ordering "<time_ordering>" for <batches> batches, expecting <expected_points_per_bucket> points per bucket and <expected_total_points> total points, then query with time range "<time_range>", query type "<query_type>", result size "<result_size>", <concurrent_clients> concurrent clients, <query_repeats> repeats per client, output format "<output_format>" and query compression "<query_compression>", and finally delete all written points
    Then I store the end-to-end benchmark result as "reports/e2e-<id>.json"

    # ==========================================================
    # MULTI BUCKET WQD - NORMAL
    # ==========================================================
    @normal
    Examples:
      | id         | bucket_prefix | measurement  | bucket_count | batch_size | parallel_writers | batches | expected_points_per_bucket | expected_total_points | write_compression | precision | point_complexity | tag_cardinality | time_ordering | time_range | query_type | result_size | concurrent_clients | query_repeats | output_format | query_compression |
      | smoke      | bddbench_e2e  | bddbench_e2e | 3            | 100        | 1                | 10      | 1000                       | 3000                  | none              | ns        | low              | 10              | in_order      | 1h         | filter     | small       | 1                  | 1             | csv           | none              |
      | average    | bddbench_e2e  | bddbench_e2e | 5            | 250        | 2                | 10      | 5000                       | 25000                 | none              | ns        | medium           | 100             | in_order      | 6h         | aggregate  | small       | 2                  | 1             | csv           | gzip              |
      | stress     | bddbench_e2e  | bddbench_e2e | 8            | 500        | 2                | 20      | 20000                      | 160000                | none              | ns        | medium           | 250             | in_order      | 24h        | aggregate  | large       | 4                  | 1             | csv           | gzip              |
      | spike      | bddbench_e2e  | bddbench_e2e | 10           | 250        | 4                | 10      | 10000                      | 100000                | none              | ns        | medium           | 250             | in_order      | 1h         | filter     | large       | 6                  | 1             | csv           | gzip              |
      | break      | bddbench_e2e  | bddbench_e2e | 12           | 500        | 4                | 20      | 40000                      | 480000                | none              | ns        | high             | 500             | in_order      | 1h         | aggregate  | large       | 8                  | 1             | csv           | gzip              |
      | soak       | bddbench_e2e  | bddbench_e2e | 5            | 100        | 1                | 300     | 30000                      | 150000                | none              | ns        | low              | 100             | in_order      | 72h        | filter     | small       | 2                  | 2             | csv           | none              |

    # ==========================================================
    # MULTI BUCKET WQD - EXPERIMENTAL
    # ==========================================================
    @experimental
    Examples:
      | id           | bucket_prefix | measurement  | bucket_count | batch_size | parallel_writers | batches | expected_points_per_bucket | expected_total_points | write_compression | precision | point_complexity | tag_cardinality | time_ordering | time_range | query_type | result_size | concurrent_clients | query_repeats | output_format | query_compression |
      | smoke        | bddbench_e2e  | bddbench_e2e | 4            | 200        | 1                | 5       | 1000                       | 4000                  | none              | ms        | low              | 10              | out_of_order  | 1h         | group_by   | small       | 1                  | 1             | csv           | none              |
      | average      | bddbench_e2e  | bddbench_e2e | 8            | 500        | 1                | 20      | 10000                      | 80000                 | gzip              | ms        | medium           | 250             | out_of_order  | 6h         | pivot      | small       | 2                  | 1             | csv           | gzip              |
      | stress       | bddbench_e2e  | bddbench_e2e | 16           | 1000       | 2                | 50      | 100000                     | 1600000               | gzip              | ns        | high             | 2000            | out_of_order  | 24h        | pivot      | large       | 4                  | 1             | csv           | gzip              |
      | spike        | bddbench_e2e  | bddbench_e2e | 32           | 2000       | 2                | 10      | 40000                      | 1280000               | none              | ns        | medium           | 1000            | out_of_order  | 1h         | group_by   | large       | 6                  | 1             | csv           | none              |
      | break        | bddbench_e2e  | bddbench_e2e | 48           | 2000       | 2                | 10      | 40000                      | 1920000               | gzip              | ns        | high             | 5000            | out_of_order  | 1h         | pivot      | large       | 8                  | 1             | csv           | gzip              |
      | soak         | bddbench_e2e  | bddbench_e2e | 8            | 300        | 1                | 400     | 120000                     | 960000                | none              | ms        | low              | 500             | out_of_order  | 72h        | filter     | small       | 2                  | 2             | csv           | none              |
