@network
Feature: Network benchmark between benchmark runner and peer VM
  In order to interpret InfluxDB benchmark results correctly
  As a platform engineer
  I want to know the VM-to-VM throughput, latency, and packet loss in our OpenStack environment.

  # IMPORTANT:
  # - Use BDD_NET_TARGET_HOST to override the peer VM (e.g. 192.168.8.116).
  # - For your environment, recommend:
  #     BDD_NET_DIRECTION=target->runner
  #   because main cannot open inbound ports, but it can run iperf3 client via ssh.

  Scenario Outline: Measure tcp/udp throughput with iperf3
    When I run an iperf3 "<protocol>" benchmark to "<target_host>" with <streams> streams for <duration_s> seconds
    Then I store the network benchmark result as "reports/network-iperf3-<id>.json"

    @iperf3 @smoke
    Examples:
      | id    | target_host         | protocol | streams | duration_s |
      | tcp-1 | __SET_BY_ENV__      | tcp      | 1       | 10         |
      | tcp-4 | __SET_BY_ENV__      | tcp      | 4       | 10         |
      | udp-1 | __SET_BY_ENV__      | udp      | 1       | 10         |

  Scenario Outline: Measure ICMP latency and packet loss with ping
    When I run a ping benchmark to "<target_host>" with <packets> packets
    Then I store the network benchmark result as "reports/network-ping-<id>.json"

    @ping @smoke
    Examples:
      | id    | target_host    | packets |
      | short | __SET_BY_ENV__ | 20      |

    @ping @load
    Examples:
      | id   | target_host    | packets |
      | long | __SET_BY_ENV__ | 200     |

