Feature: Storage benchmark (fio)
  In order to interpret ingestion and query performance
  As a test engineer
  I want to measure storage throughput and latency of the benchmark VM

  @storage
  Scenario Outline: fio storage benchmark
    Given fio is installed
    When I run a fio storage benchmark with profile "<profile>", target directory "<target_dir>", file size "<file_size>", block size "<block_size>", jobs <jobs>, iodepth <iodepth> and time limit <time_limit_s> seconds
    Then I store the storage benchmark result as "<report_path>"

    Examples:
      | profile   | target_dir          | file_size | block_size | jobs | iodepth | time_limit_s | report_path                         |
      | seq-read  | /var/lib/influxdb  | 4G        | 1M         | 1    | 16      | 30           | reports/storage-read-seq.json       |
      | seq-write | /var/lib/influxdb  | 4G        | 1M         | 1    | 16      | 30           | reports/storage-write-seq.json      |
      | rand-read | /var/lib/influxdb  | 4G        | 4k         | 4    | 64      | 30           | reports/storage-read-rnd.json       |
      | rand-write| /var/lib/influxdb  | 4G        | 4k         | 4    | 64      | 30           | reports/storage-write-rnd.json      |
      | rand-rw   | /var/lib/influxdb  | 4G        | 4k         | 4    | 64      | 30           | reports/storage-randreadwrite.json  |
