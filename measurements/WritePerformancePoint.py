from typing import Optional, Dict
from pydantic import Field
from influxdb_client import Point
from measurements.BasePoint import BasePoint

class WritePerformancePoint(BasePoint):
    """
    Write performance metrics.
    """
    # Write metrics
    write_throughput: Optional[float] = Field(None, gt=0)  # points/sec
    write_latency_avg_ms: Optional[float] = Field(None, gt=0)
    write_latency_min_ms: Optional[float] = Field(None, gt=0)
    write_latency_max_ms: Optional[float] = Field(None, gt=0)
    write_error_count: Optional[int] = Field(None, ge=0)

    # Extra metrics
    extra_metrics: Optional[Dict[str, float]] = None

    def to_point(self) -> Point:
        point = super().to_point()

        if self.write_throughput is not None:
            point = point.field("write_throughput", self.write_throughput)
        if self.write_latency_avg_ms is not None:
            point = point.field("write_latency_avg_ms", self.write_latency_avg_ms)
        if self.write_latency_min_ms is not None:
            point = point.field("write_latency_min_ms", self.write_latency_min_ms)
        if self.write_latency_max_ms is not None:
            point = point.field("write_latency_max_ms", self.write_latency_max_ms)
        if self.write_error_count is not None:
            point = point.field("write_error_count", self.write_error_count)

        # Extra metrics
        if self.extra_metrics:
            for k, v in self.extra_metrics.items():
                point = point.field(k, v)

        return point
