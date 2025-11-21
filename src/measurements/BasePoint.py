from pydantic import BaseModel, Field
from datetime import datetime
from influxdb_client import Point, WritePrecision


class BasePoint(BaseModel):
    """
    Base point for all benchmark metrics.
    Contains common tags for all measurements.
    """

    run_uuid: str
    feature_name: str
    sut: str
    test_type: str
    time: datetime

    def to_point(self) -> Point:
        """
        Convert the BasePoint to an InfluxDB Point with tags only.
        Fields will be added in subclasses.
        """
        point = (
            Point(self.__class__.__name__)
            .tag("run_uuid", self.run_uuid)
            .tag("feature_name", self.feature_name)
            .tag("sut", self.sut)
            .tag("test_type", self.test_type)
            .time(self.time, WritePrecision.NS)
        )
        return point
