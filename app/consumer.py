import time

from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os
from dotenv import load_dotenv

keyspace = 'baywheels_keyspace'


def write_to_cassandra(df, epoch, rideable_type):
    print("Epoch " + str(epoch))
    df = df.withColumn("duration", unix_timestamp("ended_at") - unix_timestamp("started_at"))

    task1 = df
    task2 = df
    task1.show()
    task1 = task1.filter(
        task1.rideable_type == rideable_type) \
        .agg(
        min(col("duration").cast('int')).alias('duration_min'),
        max(col("duration").cast('int')).alias('duration_max'),
        mean(col("duration").cast('int')).alias('duration_avg'),
        count("rideable_type").alias('num_of_rides')
    ).collect()

    if task1[0]['duration_avg']:
        duration_avg = task1[0]['duration_avg']
        duration_max = task1[0]['duration_max']
        duration_min = task1[0]['duration_min']
        num_of_rides = task1[0]['num_of_rides']

        cassandra_session.execute(f"""
                            INSERT INTO baywheels_keyspace.station_statistics(time, duration_avg, duration_max, duration_min, num_of_rides)
                            VALUES (toTimeStamp(now()), {duration_avg}, {duration_max}, {duration_min}, {num_of_rides})
                            """)

    task2 = task2.groupBy(
        task2.start_station_name) \
        .agg(count('start_station_name').alias('num_of_rides')).sort(desc('num_of_rides')).take(3)

    stations = ['Unknown', 'Unknown', 'Unknown']
    num_rides = [-1, -1, -1]
    for i, row in enumerate(task2):
        stations[i] = row['start_station_name']
        num_rides[i] = row['num_of_rides']

    cassandra_session.execute(f"""
                    INSERT INTO baywheels_keyspace.popular_stations(time, start_station_name1, num_of_rides1, start_station_name2, num_of_rides2, start_station_name3, num_of_rides3)
                    VALUES (toTimeStamp(now()), '{stations[0]}', {num_rides[0]}, '{stations[1]}', {num_rides[1]}, '{stations[2]}', {num_rides[2]})
                    """)


def build_database(cassandra_session):
    cassandra_session.execute("""
        CREATE KEYSPACE IF NOT EXISTS %s
        WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '1' }
        """ % keyspace)

    cassandra_session.set_keyspace(keyspace)

    cassandra_session.execute("""
        CREATE TABLE IF NOT EXISTS station_statistics (
            time timestamp ,
            duration_min float,
            duration_max float,
            duration_avg float,
            num_of_rides float,
            PRIMARY KEY (time)
        )
        """)

    cassandra_session.execute("""
        CREATE TABLE IF NOT EXISTS popular_stations (
            time timestamp ,
            start_station_name1 text,
            num_of_rides1 float,
            start_station_name2 text,
            num_of_rides2 float,
            start_station_name3 text,
            num_of_rides3 float,
            PRIMARY KEY (time)
        )
        """)


if __name__ == '__main__':
    load_dotenv()
    kafka_url = str(os.getenv('KAFKA_URL'))
    kafka_topic = str(os.getenv('KAFKA_TOPIC'))
    rideable_type = str(os.getenv('RIDEABLE_TYPE'))
    cassandra_host = str(os.getenv('CASSANDRA_HOST'))
    process_time = str(os.getenv('PROCESS_TIME'))

    cassandra_cluster = Cluster([cassandra_host], port=9042)
    cassandra_session = cassandra_cluster.connect()
    build_database(cassandra_session)
    print('Connected to Cassandra!')

    dataSchema = StructType() \
        .add("ride_id", "string") \
        .add("rideable_type", "string") \
        .add("started_at", "timestamp") \
        .add("ended_at", "timestamp") \
        .add("start_station_name", "string") \
        .add("start_station_id", "string") \
        .add("end_station_name", "string") \
        .add("end_station_id", "string") \
        .add("start_lat", "string") \
        .add("start_lng", "string") \
        .add("end_lat", "string") \
        .add("end_lng", "string") \
        .add("member_casual", "string") \

    # spark = (
    #     SparkSession.builder.appName("Baywheels")
    #         .config("spark.jars.packages",
    #                 "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2,com.datastax.spark:spark-cassandra-connector_2.12:3.0.0")
    #         .getOrCreate()
    # )
    spark = (
        SparkSession.builder.appName("Baywheels")
            .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    sampleDataframe = (
        spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", kafka_url)
            .option("subscribe", kafka_topic)
            .option("startingOffsets", "earliest")
            .load()
    ).selectExpr("CAST(value as STRING)", "timestamp").select(
        from_json(col("value"), dataSchema).alias("sample"), "timestamp"
    ).select("sample.*")

    sampleDataframe.writeStream \
        .option("spark.cassandra.connection.host", cassandra_host+':'+str(9042)) \
        .foreachBatch(lambda df, epoch_id: write_to_cassandra(df, epoch_id, rideable_type)) \
        .outputMode("update") \
        .trigger(processingTime=process_time) \
        .start().awaitTermination()