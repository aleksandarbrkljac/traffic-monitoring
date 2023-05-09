import os
from pyspark.sql import SparkSession, functions as func, types as tp


def main():
    spark = (
        SparkSession.builder
        .appName("Batch processing")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.4.0")
        .getOrCreate()
    )
    HDFS_NAMENODE = os.getenv(
        "CORE_CONF_fs_defaultFS", 'hdfs://localhost:9870')
    source_path = f'{HDFS_NAMENODE}/us_accidents.csv'
    # source_path = "../docker-spark/source_data/us_accidents.csv"
    df = spark.read.csv(
        source_path, inferSchema=True, header=True)

    df = (
        df.withColumn('date', func.to_date('start_time'))
        .withColumn('day_of_week', func.date_format(func.col("start_time"), "E"))
        .withColumn('month', func.date_format('start_time', 'M').cast(tp.IntegerType()))
        .withColumn('year', func.date_format('start_time', 'yyyy').cast(tp.StringType()))
        .withColumn('hours', func.date_format('start_time', 'HH').cast(tp.IntegerType()))
        .withColumn('part_of_day', func.when(func.col('hours').between(6, 23), 'PART_1').otherwise('PART_2'))
        .withColumn('season', func.floor(func.col('month') % func.lit(12) / func.lit(3)) + func.lit(1))
        .withColumn('temperature', (func.col('Temperature(F)') - func.lit(32)) * func.lit(5) / func.lit(9))
    )
    (
        df.write.format("jdbc")
        .option("url", "jdbc:postgresql://db:5432/accidents")
        .option("driver", "org.postgresql.Driver")
        .option("dbtable", "accidents")
        .option("user", "postgres")
        .option("password", "admin")
        .save(mode="overwrite")
    )


if __name__ == '__main__':
    main()
