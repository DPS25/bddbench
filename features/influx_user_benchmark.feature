Feature: InfluxDB v2 User API benchmark (/api/v2/me, /api/v2/users)
  In order to benchmark user management endpoints
  As a platform engineer
  I want to measure latency/throughput/errors for /api/v2/me and user lifecycle operations

  Background:
    Given a SUT InfluxDB v2 endpoint is configured and reachable

  Scenario Outline: Benchmark /api/v2/me endpoint latency
    When I run a "/me" benchmark with <concurrent_clients> concurrent clients for <duration_s> seconds
    Then I store the user benchmark result as "reports/user-me-<id>.json"

    @influx @user @me @smoke
    Examples:
      | id    | concurrent_clients | duration_s |
      | smoke | 1                  | 5          |

    @influx @user @me @load
    Examples:
      | id   | concurrent_clients | duration_s |
      | load | 10                 | 10         |


  Scenario Outline: Benchmark User CRUD lifecycle (Create/Update/Retrieve/Delete)
    When I run a user lifecycle benchmark with username complexity "<username_complexity>", password complexity "<password_complexity>", <concurrent_clients> parallel threads for <iterations> iterations
    Then I store the user benchmark result as "reports/user-lifecycle-<id>.json"

    @influx @user @crud @smoke
    Examples:
      | id          | username_complexity | password_complexity | concurrent_clients | iterations |
      | simple_crud | low                 | low                 | 1                  | 10         |

    @influx @user @crud @load
    Examples:
      | id         | username_complexity | password_complexity | concurrent_clients | iterations |
      | heavy_crud | high                | high                | 4                  | 50         |

