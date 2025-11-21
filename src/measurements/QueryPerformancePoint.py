from typing import Optional, Dict
from pydantic import Field
from influxdb_client import Point
from src.measurements.BasePoint import BasePoint


class QueryPerformancePoint(BasePoint):
    """
    Query performance metrics.
    """

    # Query metrics
    query_throughput: Optional[float] = Field(None, gt=0)  # queries/sec
    query_latency_avg_ms: Optional[float] = Field(None, gt=0)
    query_latency_min_ms: Optional[float] = Field(None, gt=0)
    query_latency_max_ms: Optional[float] = Field(None, gt=0)
    query_error_count: Optional[int] = Field(None, ge=0)

    # Extra metrics
    extra_metrics: Optional[Dict[str, float]] = None

    def to_point(self) -> Point:
        point = super().to_point()

        if self.query_throughput is not None:
            point = point.field("query_throughput", self.query_throughput)
        if self.query_latency_avg_ms is not None:
            point = point.field("query_latency_avg_ms", self.query_latency_avg_ms)
        if self.query_latency_min_ms is not None:
            point = point.field("query_latency_min_ms", self.query_latency_min_ms)
        if self.query_latency_max_ms is not None:
            point = point.field("query_latency_max_ms", self.query_latency_max_ms)
        if self.query_error_count is not None:
            point = point.field("query_error_count", self.query_error_count)

        # Extra metrics
        if self.extra_metrics:
            for k, v in self.extra_metrics.items():
                point = point.field(k, v)

        return point
