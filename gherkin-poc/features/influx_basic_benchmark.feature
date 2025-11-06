Feature: Basic InfluxDB write and read-back benchmark
  Background:
    Given an InfluxDB v2 endpoint is configured from environment 
    And a target bucket from environment is available 

  @poc @influx @basic
  Scenario: write and read back a small batch
    When I write 10 points with measurement "bddbench_write"
    Then I can read back 10 points with the same run id
    And the averaage write latency shall be <= 500 ms
