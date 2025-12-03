from typing import Optional, Dict

from pydantic import Field
from influxdb_client import Point

from src.measurements.BasePoint import BasePoint


class UserPerformancePoint(BasePoint):
    """
    User management performance metrics.
    Targets /api/v2/me and CRUD operations on users.
    """

    # Operation type: "me", "lifecycle_crud", ...
    operation: str

    # Metrics
    throughput: Optional[float] = Field(None, gt=0)  # ops/sec
    latency_avg_ms: Optional[float] = Field(None, gt=0)
    latency_min_ms: Optional[float] = Field(None, gt=0)
    latency_max_ms: Optional[float] = Field(None, gt=0)
    error_count: Optional[int] = Field(None, ge=0)

    # Configuration
    # e.g. "low" / "high" username complexity
    complexity: str = "low"
    concurrency: int = 1

    # Extra metrics (for extensibility)
    extra_metrics: Optional[Dict[str, float]] = None

    def to_point(self) -> Point:
        """
        Convert this Pydantic model into an InfluxDB Point,
        extending the base fields from BasePoint.
        """
        point = super().to_point()

        # --- Tags specific to user benchmark ---
        point = (
            point.tag("operation", self.operation)
            .tag("complexity", self.complexity)
        )

        # --- Fields ---
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

        point = point.field("concurrency", self.concurrency)

        # Extra fields if needed
        if self.extra_metrics:
            for k, v in self.extra_metrics.items():
                point = point.field(k, v)

        return point

