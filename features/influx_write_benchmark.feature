Feature: InfluxDB v2 write benchmark (/api/v2/write)
  In order to evaluate write performance characteristics
  As a test engineer
  I want to run write benchmarks against InfluxDB v2 with adjustable parameters

  Background:
    Given a SUT InfluxDB v2 endpoint is configured and reachable
    And the target bucket from the SUT config is available

  Scenario Outline: generic write benchmark run (batch + parallel)
    When I run a generic write benchmark on measurement "<measurement>" with batch size <batch_size>, <parallel_writers> parallel writers, compression "<compression>", timestamp precision "<precision>", point complexity "<point_complexity>", tag cardinality <tag_cardinality> and time ordering "<time_ordering>" for <batches> batches
    Then I store the generic write benchmark result as "reports/write-<id>.json"

    @influx @write @smoke
    Examples:
      | id    | measurement        | batch_size | parallel_writers | compression | precision | point_complexity | tag_cardinality | time_ordering | batches |
      | smoke | bddbench_write_poc | 100        | 1                | none        | ns        | low              | 10              | in_order      | 10      |

    @influx @write @load
    Examples:
      | id    | measurement        | batch_size | parallel_writers | compression | precision | point_complexity | tag_cardinality | time_ordering | batches |
      | load  | bddbench_write_poc | 250        | 2                | none        | ns        | medium           | 100             | in_order      | 10      |

    @experimental
    Examples:
      | id           | measurement        | batch_size | parallel_writers | compression | precision | point_complexity | tag_cardinality | time_ordering | batches |
      | experimental | bddbench_write_poc | 1000       | 4                | gzip        | ns        | high             | 1000            | out_of_order  | 50      |
    
