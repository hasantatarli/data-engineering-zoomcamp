import csv
from json import dumps
from kafka import KafkaProducer
from time import sleep
from datetime import date, datetime


def json_serial(obj):
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError("Type %s not serializable" % type(obj))


producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         key_serializer=lambda x: dumps(x).encode('utf-8'),
                         value_serializer=lambda x: dumps(x).encode('utf-8'))

file = open('data/rides.csv')

csvreader = csv.reader(file)
csvheader = next(csvreader)

for row in csvreader:
    key = {"vendorId": int(row[0])}
    value={"vendorId":int(row[0]), 
           "passenger_count": int(row[3]), 
           "trip_distance": float(row[4]), 
           "payment_type": int(row[9]), 
           "total_amount": float(row[16]), 
           "PULocationID": int(row[7]), 
           "DOLocationID": int(row[8]),
           "RCollectionTime": dumps(datetime.now(), default=json_serial)
        }

    producer.send('datatalkclub.yellow_taxi_ride.json.v1',value=value, key=key)
    print("producing")
    sleep(1)

