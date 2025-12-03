Feature: InfluxDB v2 User API Benchmark
  In order to ensure InfluxDB can handle high concurrency of user operations
  As a platform engineer
  I want to benchmark the /api/v2/me endpoint and user management lifecycle

  Background:
    Given a SUT InfluxDB v2 endpoint is configured and reachable

  @influx @user @me
  Scenario Outline: Benchmark /api/v2/me endpoint latency
    When I run a "/me" benchmark with <concurrent_clients> concurrent clients for <duration_s> seconds
    Then I store the user benchmark result as "reports/user-me-<id>.json"

    Examples:
      | id    | concurrent_clients | duration_s |
      | smoke | 1                  | 5          |
      | load  | 10                 | 10         |
      # | stress | 50              | 30         |

  @influx @user @crud
  Scenario Outline: Benchmark User CRUD Lifecycle (Create/Update/Retrieve/Delete)
    When I run a user lifecycle benchmark with <complexity> username complexity, <concurrent_clients> parallel threads for <iterations> iterations
    Then I store the user benchmark result as "reports/user-lifecycle-<id>.json"

    Examples:
      | id          | complexity | concurrent_clients | iterations |
      | simple_crud | low        | 1                  | 10         |
      | heavy_crud  | high       | 4                  | 50         |

