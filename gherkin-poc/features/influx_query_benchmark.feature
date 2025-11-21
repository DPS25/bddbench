Feature: InfluxDB v2 query benchmark (/api/v2/query)
  In order to evaluate query performance characeteristics 
  As a test engineer
  I want to run query benchmarks against InfluxDB v2 with adjustable parameters

  Background:
    Given a generic InfluxDB v2 endpoint is configured from environment 
    And a generic target bucket from environment is available

  @poc @influx @query 
  Scenario Outline: generic query benchmark run
    When I run a generic query benchmark on measurement "<measurement>" with time range "<time_range>" using query type "<query_type>" and result size "<result_size>" with <concurrent_clients> concurrent clients, output format "<output_format>" and compression "<compression>"
    Then I store the generic query benchmark result as "reports/query-<id>.json"

     Examples:
      | id    | measurement      | time_range | query_type | result_size | concurrent_clients | output_format | compression |
      | smoke | bddbench_generic | 10s        | filter     | small       | 1                  | csv           | none        |
      | load  | bddbench_generic | 1h         | aggregate  | large       | 5                  | csv           | gzip        |
