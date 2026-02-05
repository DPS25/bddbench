from typing import Optional, Literal
from pydantic import Field
from influxdb_client import Point
from src.measurements.BasePoint import BasePoint


class NetworkPingPoint(BasePoint):
    """
    Network benchmark point for both ping and iperf3.
    (Kept in a single file/class due to repo constraints.)
    """

    # ---- tags (stored as tags) ----
    mode: Literal["ping", "iperf3"] = Field(...)
    protocol: Literal["icmp", "tcp", "udp"] = Field(...)
    direction: str = Field(..., min_length=3)

    runner_host: str = Field(..., min_length=1)
    runner_ip: str = Field(..., min_length=7)
    peer_host: str = Field(..., min_length=1)
    peer_ip: str = Field(..., min_length=7)

    report_name: str = Field(..., min_length=1)  # e.g. network-ping-short

    # ---- ping fields ----
    packet_count: Optional[int] = Field(None, gt=0)
    rtt_min_ms: Optional[float] = Field(None, ge=0)
    rtt_avg_ms: Optional[float] = Field(None, ge=0)
    rtt_max_ms: Optional[float] = Field(None, ge=0)
    rtt_mdev_ms: Optional[float] = Field(None, ge=0)

    # ---- iperf3 fields ----
    parallel_streams: Optional[int] = Field(None, ge=1)
    duration_s: Optional[int] = Field(None, gt=0)
    iperf_port: Optional[int] = Field(None, ge=1, le=65535)
    udp_bitrate: Optional[str] = None

    throughput_mbps: Optional[float] = Field(None, ge=0)
    jitter_ms: Optional[float] = Field(None, ge=0)

    # shared-ish
    packet_loss_pct: Optional[float] = Field(None, ge=0)

    def to_point(self) -> Point:
        point = super().to_point()

        # tags
        point = (
            point.tag("mode", self.mode)
            .tag("protocol", self.protocol)
            .tag("direction", self.direction)
            .tag("runner_host", self.runner_host)
            .tag("runner_ip", self.runner_ip)
            .tag("peer_host", self.peer_host)
            .tag("peer_ip", self.peer_ip)
            .tag("report_name", self.report_name)
        )

        # fields (only if not None)
        if self.packet_count is not None:
            point = point.field("packet_count", self.packet_count)
        if self.rtt_min_ms is not None:
            point = point.field("rtt_min_ms", self.rtt_min_ms)
        if self.rtt_avg_ms is not None:
            point = point.field("rtt_avg_ms", self.rtt_avg_ms)
        if self.rtt_max_ms is not None:
            point = point.field("rtt_max_ms", self.rtt_max_ms)
        if self.rtt_mdev_ms is not None:
            point = point.field("rtt_mdev_ms", self.rtt_mdev_ms)

        if self.parallel_streams is not None:
            point = point.field("parallel_streams", self.parallel_streams)
        if self.duration_s is not None:
            point = point.field("duration_s", self.duration_s)
        if self.iperf_port is not None:
            point = point.field("iperf_port", self.iperf_port)
        if self.udp_bitrate is not None:
            point = point.field("udp_bitrate", self.udp_bitrate)

        if self.throughput_mbps is not None:
            point = point.field("throughput_mbps", self.throughput_mbps)
        if self.jitter_ms is not None:
            point = point.field("jitter_ms", self.jitter_ms)

        if self.packet_loss_pct is not None:
            point = point.field("packet_loss_pct", self.packet_loss_pct)

        return point

