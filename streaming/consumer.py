import logging
import math
from pyspark.sql.types import DoubleType
from influxdb_client.client.write_api import SYNCHRONOUS
from influxdb_client import InfluxDBClient
import os
from datetime import datetime, timezone
from typing import List
from pyspark.sql import SparkSession, Window, DataFrame, functions as func, types as tp

logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger("consumer")
kafka_server = os.getenv("KAFKA_BROKER", "localhost:9092")
topic_name = os.getenv("KAFKA_TOPIC_NAME", "first_kafka_topic")
influx_host = os.getenv("INFLUXDB_HOST", "localhost")
influx_token = os.getenv("INFLUXDB_TOKEN", "token_influx_aco_admin")
postgres_host = os.getenv("POSTGRES_DB", "localhost")


def haversine(lat1, lon1, lat2, lon2):
    """
    Calculate the distance between two points on earth given by their coordinates in degrees using the Haversine formula.

    Args:
        lat1 (float): Latitude of the first coordinate.
        lon1 (float): Longitude of the first coordinate.
        lat2 (float): Latitude of the second coordinate.
        lon2 (float): Longitude of the second coordinate.


    Returns:
        float: Distance in kilometers.
    """
    # Convert coordinates to radians
    lat1, lon1, lat2, lon2 = map(math.radians, [float(
        lat1), float(lon1), float(lat2), float(lon2)])

    # Haversine formula
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = math.sin(dlat / 2) ** 2 + math.cos(lat1) * \
        math.cos(lat2) * math.sin(dlon / 2) ** 2
    c = 2 * math.asin(math.sqrt(a))

    # Earth's radius in kilometers
    r = 6371
    return c * r


# Registering haversine function as a User-Defined Function (UDF) for Spark
haversine_udf = func.udf(haversine, DoubleType())


def process_batch(df: DataFrame, batch_id):
    """
    Processes each batch of data, identifies violations, and writes the data to the appropriate databases.

    Args:
        df (DataFrame): The input DataFrame.
        batch_id (int): The batch ID.
    Returns:
        -
    """
    window_spec = Window.partitionBy(
        "car_id", "timestamp").orderBy("avg_distance")
    enriched_df = (
        df.withColumn("rn", func.row_number().over(window_spec))
        # road_segment with the smallest avg_distance
        .filter("rn = 1").drop("rn")
        .withColumn("speed_limit_exceeded",
                    # If speed is greater than speed limit + 10
                    func.when(func.col('speed') >
                              (func.col('speed_limit') + func.lit(10)), func.lit(True))
                    .otherwise(func.lit(False))
                    )
        .withColumn("stopping",
                    # If speed is 0
                    func.when(func.col('speed') == func.lit(0),
                              func.lit(True)).otherwise(func.lit(False))
                    )
    )
    # Write traffic data
    write_traffic_data_to_influx(enriched_df)
    # Count cars driving over the speed limit and write to InfluxDB
    write_exceeded_speed_counts_to_influx(enriched_df)
    # Count stopped cars (traffic jam) and write to InfluxDB
    write_exceeded_stopping_counts_to_influx(enriched_df)
    # Write speeding violations to PostgreSQL
    write_speed_violations_to_postgres(enriched_df)


def write_speed_violations_to_postgres(enriched_df: DataFrame):
    speed_limit_exceeded_df = (
        enriched_df.filter(func.col('speed_limit_exceeded') == func.lit(True))
        .groupBy('car_id', 'road_name', 'speed_limit')
        .agg(func.max('speed').alias('speed'))
        .withColumn("timestamp", func.lit(datetime.now(timezone.utc)))
        .select("car_id", "speed", "speed_limit", "road_name", "timestamp")
    )
    write_to_postgres(speed_limit_exceeded_df)


def write_exceeded_stopping_counts_to_influx(enriched_df: DataFrame):
    stopping_road_df = (
        enriched_df
        .select("car_id", "road_name", "stopping")
        .distinct()  # Da ne bi brojao vise puta isti automobil
        .filter(func.col('stopping') == func.lit(True))
        .groupBy("road_name")
        .agg(func.count('road_name').alias("stopping_count"))
    )
    stopping_road_data_points = [{"measurement": "stopping_road",
                                  "tags": {"road_name": item['road_name']},
                                  "fields": {
                                      "stopping_count": item['stopping_count']
                                  }
                                  } for item in [
        row.asDict() for row in stopping_road_df.collect()]]
    write_to_infuxdb(stopping_road_data_points)


def write_exceeded_speed_counts_to_influx(enriched_df: DataFrame):
    speed_limit_exceeded_road_df = (
        enriched_df.select("car_id", "road_name", "speed_limit_exceeded")
        .distinct()  # Da ne bi brojao vise puta isti automobil
        .filter(func.col('speed_limit_exceeded') == func.lit(True))
        .groupBy("road_name")
        .agg(func.count('road_name').alias("speed_limit_exceeded_count"))
    )
    speed_limit_exceeded_data_points = [{"measurement": "speed_limit_exceeded",
                                         "tags": {"road_name": item['road_name']},
                                         "fields": {
                                             "speed_limit_exceeded_count": item['speed_limit_exceeded_count']
                                         }
                                         } for item in [
        row.asDict() for row in speed_limit_exceeded_road_df.collect()]]
    write_to_infuxdb(speed_limit_exceeded_data_points)


def write_traffic_data_to_influx(enriched_df: DataFrame):
    traffic_data_points = [{"measurement": "traffic",
                            "tags": {"car_id": item['car_id'], "road_segment": item['road_name']},
                            "fields": {
                                "lat": item['lat'],
                                "lon": item['lon'],
                                "speed": item['speed'],
                                "road_segment": item['road_name']
                            },
                            } for item in [row.asDict() for row in enriched_df.collect()]]
    write_to_infuxdb(traffic_data_points)


def write_to_infuxdb(points: List):
    if points:
        messurment = points[0]['measurement']
        LOGGER.info(f"\n * Write to influx db to traffic.{messurment}")
        org = "ftn"
        bucket = "traffic"
        client = InfluxDBClient(
            url=f"http://{influx_host}:8086", token=influx_token)
        write_api = client.write_api(write_options=SYNCHRONOUS)
        write_api.write(bucket, org, points)


def write_to_postgres(df: DataFrame):
    if not df.rdd.isEmpty():
        LOGGER.info("\n * Write to postgres db")
        (
            df.write.format("jdbc")
            .option("url", f"jdbc:postgresql://{postgres_host}:5432/accidents")
            .option("driver", "org.postgresql.Driver")
            .option("dbtable", "speeding_penalties")
            .option("user", "postgres")
            .option("password", "admin")
            .save(mode="append")
        )


def load_road_segments_data(spark: SparkSession) -> DataFrame:
    road_segment_schema = tp.StructType([
        tp.StructField("road_id", tp.IntegerType()),
        tp.StructField("start_lat", tp.StringType()),
        tp.StructField("start_lon", tp.StringType()),
        tp.StructField("end_lat", tp.StringType()),
        tp.StructField("end_lon", tp.StringType()),
        tp.StructField("speed_limit", tp.DoubleType()),
        tp.StructField("road_name", tp.StringType())
    ])

    return spark.read.csv(
        "./road_segments.csv", header=True, schema=road_segment_schema)


def run():
    spark = (
        SparkSession.builder
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2,org.postgresql:postgresql:42.4.0")
        .config("spark.sql.shuffle.partitions", "10")
        .appName("KafkaSparkStreaming")
        .getOrCreate()
    )
    road_segments_df = load_road_segments_data(spark)
    # Define the schema for the incoming data
    schema = tp.StructType([
        tp.StructField("lat", tp.StringType()),
        tp.StructField("lon", tp.StringType()),
        tp.StructField("speed", tp.DoubleType()),
        tp.StructField("car_id", tp.IntegerType()),
        tp.StructField("timestamp", tp.TimestampType())
    ])
    # Create a DataFrame representing the stream of input from Kafka
    df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_server)
        .option("subscribe", topic_name)
        .load()
        .select(func.from_json(func.col("value").cast("string"), schema).alias("data"))
        .select("data.*")
    )

    car_road_df = (
        df.crossJoin(road_segments_df)
        .withColumn("start_distance", haversine_udf("lat", "lon", "start_lat", "start_lon"))
        .withColumn("end_distance", haversine_udf("lat", "lon", "end_lat", "end_lon"))
        .withColumn("avg_distance", (func.col("start_distance") + func.col("end_distance")) / 2)
        .drop("start_distance", "end_distance")
    )

    query = (
        car_road_df.writeStream
        .trigger(processingTime="10 seconds")
        .foreachBatch(process_batch)
        .start()
    )
    query.awaitTermination()


if __name__ == "__main__":
    run()
