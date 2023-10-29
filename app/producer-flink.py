from kafka import KafkaProducer
import csv
import json
import time

if __name__ == '__main__':
    msgProducer = KafkaProducer(bootstrap_servers=['0.0.0.0:9092'],
                                api_version=(0, 10, 1),
                                value_serializer=lambda x: x.encode('utf-8'))

    print('Kafka Producer has been initiated')

    with open('baywheels//baywheels.csv') as csvFile:
        data = csv.DictReader(csvFile)
        for row in data:
            row['ride_id'] = str(row['ride_id'])
            row['rideable_type'] = str(row['rideable_type'])
            row['started_at'] = str(row['started_at'])
            row['ended_at'] = str(row['ended_at'])
            row['start_station_name'] = str(row['start_station_name'])
            row['start_station_id'] = str(row['start_station_id'])
            row['end_station_name'] = str(row['end_station_name'])
            row['end_station_id'] = str(row['end_station_id'])
            row['start_lat'] = float(row['start_lat'])
            row['start_lng'] = float(row['start_lng'])
            row['end_lat'] = float(row['end_lat'])
            row['end_lng'] = float(row['end_lng'])
            row['member_casual'] = str(row['member_casual'])

            print(json.dumps(row))
            msgProducer.send('flink', json.dumps(row))
            msgProducer.flush()

            time.sleep(0.2)

    print('Kafka message producer done!')
