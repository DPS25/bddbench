Feature: Configurable VM stress test with stress-ng
  As a test engineer
  I want to stress memory, storage and network of the VM host
  So that I can evaluate InfluxDB under host load conditions

  Background:
    Given stress-ng is installed on the host

  @vm_stress @poc
  Scenario Outline: Run host stress with different resource settings
    When I run the vm stress test for "<duration>" with vm <vm_workers>, "<vm_bytes>", hdd <hdd_workers>, "<hdd_bytes>" and network <network_all>
    Then the vm stress test should complete successfully

    Examples:
      | duration | vm_workers | vm_bytes | hdd_workers | hdd_bytes | network_all |
      | 10s      | 1          | 50%      | 1           | 256M      | 0           |
      | 60s      | 2          | 70%      | 1           | 1G        | 0           |
