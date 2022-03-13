import faust
from sqlalchemy import true

class Zone(faust.Record, validation=True):
    LocationID: int
    Borough: str
    Zone: str
    service_zone: str