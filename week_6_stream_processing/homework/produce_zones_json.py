
import csv
from datetime import datetime, timedelta
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

file = open('data/zones.csv')
now = datetime.now()
csvreader = csv.reader(file)
csvheader = next(csvreader)

for row in csvreader:
    key = {"LocationID": int(row[0])}
    value={"LocationID":int(row[0]), 
           "Borough": str(row[1]), 
           "Zone": str(row[2]), 
           "service_zone": str(row[3]),
           "ZCollectionTime": dumps(datetime.now() + timedelta(minutes=10), default=json_serial)
        }

    producer.send('datatalkclub.zones.json.v1',value=value, key=key)
    print("producing")
    sleep(1)

