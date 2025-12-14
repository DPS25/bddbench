from typing import Optional, Dict

from pydantic import Field
from influxdb_client import Point

from src.measurements.BasePoint import BasePoint


class UserPerformancePoint(BasePoint):
    """
    User management performance metrics.
    Targets /api/v2/me and CRUD operations on users.
    """

    # scenario id derived from report filename (smoke/load/...)
    scenario_id: str = ""

    # Operation type: "me", "lifecycle_crud", ...
    operation: str

    # SUT dimensions (align with write/multi-bucket tags)
    sut_influx_url: str = ""
    sut_org: str = ""
    sut_bucket: str = ""

    # Configuration dimensions
    username_complexity: str = "none"
    password_complexity: str = "none"
    concurrency: int = 1

    # Metrics
    throughput: Optional[float] = Field(None, gt=0)  # ops/sec
    latency_avg_ms: Optional[float] = Field(None, gt=0)
    latency_min_ms: Optional[float] = Field(None, gt=0)
    latency_max_ms: Optional[float] = Field(None, gt=0)
    error_count: Optional[int] = Field(None, ge=0)

    # Extra metrics (for extensibility)
    extra_metrics: Optional[Dict[str, float]] = None

    def to_point(self) -> Point:
        """
        Convert this model into an InfluxDB Point,
        extending the base fields from BasePoint.
        """
        point = super().to_point()

        # --- Tags (dimensions) ---
        point = (
            point
            .tag("scenario_id", self.scenario_id or "")
            .tag("operation", self.operation)
            .tag("username_complexity", self.username_complexity)
            .tag("password_complexity", self.password_complexity)
            .tag("sut_influx_url", self.sut_influx_url)
            .tag("sut_org", self.sut_org)
            .tag("sut_bucket", self.sut_bucket)
            .tag("concurrency", str(self.concurrency))
        )

        # --- Fields (values) ---
        if self.throughput is not None:
            point = point.field("ops_per_sec", self.throughput)
        if self.latency_avg_ms is not None:
            point = point.field("latency_avg_ms", self.latency_avg_ms)
        if self.latency_min_ms is not None:
            point = point.field("latency_min_ms", self.latency_min_ms)
        if self.latency_max_ms is not None:
            point = point.field("latency_max_ms", self.latency_max_ms)
        if self.error_count is not None:
            point = point.field("error_count", self.error_count)

        # Extra fields
        if self.extra_metrics:
            for k, v in self.extra_metrics.items():
                point = point.field(k, v)

        return point

