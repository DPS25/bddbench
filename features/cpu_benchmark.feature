Feature: CPU benchmark (sysbench)
  In order to interpret ingestion and ingestion limits across environments
  As a test engineer
  I want to measure CPU throughput of the benchmark VM

  @cpu @benchmark
  Scenario Outline: sysbench cpu benchmark
    Given sysbench is installed
    When I run a sysbench cpu benchmark with max prime <max_prime>, threads <threads> and time limit <time_limit_s> seconds
    Then I store the cpu benchmark result as "<report_path>"

    Examples:
      | max_prime | threads | time_limit_s | report_path                      |
      | 20000     | 1       | 20           | reports/cpu-maxprime-20000-t1.json |
      | 20000     | 2       | 20           | reports/cpu-maxprime-20000-t2.json |
      | 20000     | 4       | 20           | reports/cpu-maxprime-20000-t4.json |

