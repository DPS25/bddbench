@network
Feature: VM-to-VM network benchmark (health check)
  In order to interpret benchmark results correctly
  As a platform engineer
  I want to measure VM-to-VM throughput, latency, and packet loss in our OpenStack environment.

  # Notes:
  # - target_host:
  #     Use "__SUT__" to derive the target host from INFLUXDB_SUT_URL (recommended).
  #     You may override target_host in the Examples table only when explicitly needed.
  #
  # - direction:
  #     Scenario intent must be self-describing; direction lives in the feature, not in env.
  #     Allowed values: "runner->target" or "target->runner"
  #
  # - Port:
  #     Default is 5201 unless BDD_NET_IPERF_PORT is set.
  #     If 5201 is busy, the step will try a small range (5202, 5203, ...) automatically,
  #     unless you explicitly pinned a port via BDD_NET_IPERF_PORT.

  Scenario Outline: Measure tcp/udp throughput with iperf3
    When I run an iperf3 "<protocol>" benchmark "<direction>" to "<target_host>" with <streams> streams for <duration_s> seconds
    Then I store the network benchmark result as "reports/network-iperf3-<id>.json"

    @iperf3 @smoke
    Examples:
      | id          | direction       | target_host | protocol | streams | duration_s |
      | tcp-1-r2t   | runner->target  | __SUT__     | tcp      | 1       | 10         |
      | tcp-1-t2r   | target->runner  | __SUT__     | tcp      | 1       | 10         |
      | tcp-4-r2t   | runner->target  | __SUT__     | tcp      | 4       | 10         |
      | tcp-4-t2r   | target->runner  | __SUT__     | tcp      | 4       | 10         |
      | udp-1-r2t   | runner->target  | __SUT__     | udp      | 1       | 10         |
      | udp-1-t2r   | target->runner  | __SUT__     | udp      | 1       | 10         |

  Scenario Outline: Measure ICMP latency and packet loss with ping
    When I run a ping benchmark to "<target_host>" with <packets> packets
    Then I store the network benchmark result as "reports/network-ping-<id>.json"

    @ping @smoke
    Examples:
      | id    | target_host | packets |
      | short | __SUT__     | 20      |

    # NOTE: load test is intentionally excluded from smoke runs
    @ping @load
    Examples:
      | id   | target_host | packets |
      | long | __SUT__     | 200     |

