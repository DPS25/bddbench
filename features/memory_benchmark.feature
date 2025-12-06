Feature: Memory benchmark (sysbench)
  In order to interpret ingestion and query performance
  As a test engineer
  I want to measure memory throughput and latency of the benchmark VM

  @memory
  Scenario Outline: sysbench memory benchmark
    Given sysbench is installed
    When I run a sysbench memory benchmark with mode "<mode>", access mode "<access_mode>", block size "<block_size>", total size "<total_size>", threads <threads> and time limit <time_limit_s> seconds
    Then I store the memory benchmark result as "<report_path>"
    And I write the memory benchmark result to main influx measurement "bddbench_memory"

    Examples:
      | mode  | access_mode | block_size | total_size | threads | time_limit_s | report_path                   |
      | read  | seq         | 1M         | 4G         | 1       | 10           | reports/memory-read-seq.json  |
      | write | seq         | 1M         | 4G         | 1       | 10           | reports/memory-write-seq.json |
      | read  | rnd         | 1M         | 4G         | 1       | 10           | reports/memory-read-rnd.json  |
      | write | rnd         | 1M         | 4G         | 1       | 10           | reports/memory-write-rnd.json |
