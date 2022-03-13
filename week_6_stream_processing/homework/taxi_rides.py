import faust
from sqlalchemy import true

class TaxiRide(faust.Record, validation=True):
    vendorId: str
    passenger_count: int
    trip_distance: float
    payment_type: int
    total_amount: float
    PULocationId: int
    DOLocationId: int