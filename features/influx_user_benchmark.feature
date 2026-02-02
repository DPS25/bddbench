@user
Feature: InfluxDB v2 User API benchmark (/api/v2/me, /api/v2/users)
  In order to benchmark user management endpoints
  As a platform engineer
  I want to measure latency/throughput/errors for /api/v2/me and user lifecycle operations

  Background:
    Given a SUT InfluxDB v2 endpoint is configured and reachable

  @me
  Scenario Outline: Benchmark /api/v2/me endpoint latency
    When I run a "/me" benchmark with <concurrent_clients> concurrent clients for <duration_s> seconds
    Then I store the user benchmark result as "reports/user-me-<id>.json"

    @normal
    Examples:
      | id    | concurrent_clients | duration_s |
      | smoke | 1                  | 5          |
      | load  | 10                 | 10         |
      | stress| 50                 | 15         |
      | spike | 200                | 5          |
      | soak  | 20                 | 300        |

    @experimental
    Examples:
      | id        | concurrent_clients | duration_s |
      | smoke     | 2                  | 5          |
      | load      | 20                 | 10         |
      | stress    | 100                | 15         |
      | spike     | 400                | 5          |
      | soak      | 50                 | 600        |
      | breakpoint| 800                | 10         |

  @crud
  Scenario Outline: Benchmark User CRUD lifecycle (Create/Update/Retrieve/Delete)
    When I run a user lifecycle benchmark with username complexity "<username_complexity>", password complexity "<password_complexity>", <concurrent_clients> parallel threads for <iterations> iterations
    Then I store the user benchmark result as "reports/user-lifecycle-<id>.json"

    @normal
    Examples:
      | id          | username_complexity | password_complexity | concurrent_clients | iterations |
      | smoke       | low                 | low                 | 1                  | 10         |
      | load        | high                | high                | 4                  | 50         |
      | stress      | high                | high                | 10                 | 100        |
      | spike       | medium              | medium              | 50                 | 20         |
      | soak        | medium              | medium              | 2                  | 500        |

    @experimental
    Examples:
      | id        | username_complexity | password_complexity | concurrent_clients | iterations |
      | load      | high                | high                | 8                  | 100        |
      | stress    | high                | high                | 20                 | 200        |
      | spike     | high                | high                | 100                | 50         |
      | soak      | high                | high                | 5                  | 1000       |
      | breakpoint| high                | high                | 200                | 50         |


