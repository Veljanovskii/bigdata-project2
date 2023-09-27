from kafka import KafkaProducer
import csv
import json
import time


msgProducer = KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer = lambda x: x.encode('utf-8'))

with open('baywheels//baywheels.csv') as csvFile:  
    data = csv.DictReader(csvFile)
    for row in data:
        msgProducer.send('test', json.dumps(row))
        msgProducer.flush()

        print('Message sent: ' + json.dumps(row))
        time.sleep(0.2)

print('Kafka message producer done!')
