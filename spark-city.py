from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType
from config import configuration
from pyspark.sql import DataFrame

def main():
    spark = SparkSession.builder.appName("SmartCityStreaming") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.0,"
                                       "org.apache.hadoop:hadoop-aws:3.3.1,"
                                       "com.amazonaws:aws-java-sdk:1.11.901,") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.access.key", configuration.get('AWS_ACCESS_KEY')) \
        .config("spark.hadoop.fs.s3a.secret.key", configuration.get('AWS_SECRET_KEY')) \
        .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider') \
        .getOrCreate()

    # Adjust the log level to minimize the console output on executors
    spark.sparkContext.setLogLevel('WARN')

    # Vehicle schema
    vehicleSchema = StructType([
        StructField("id", StringType(), nullable=True),
        StructField("deviceId", StringType(), nullable=True),
        StructField("timestamp", TimestampType(), nullable=True),
        StructField("location", StringType(), nullable=True),
        StructField("speed", DoubleType(), nullable=True),
        StructField("direction", StringType(), nullable=True),
        StructField("make", StringType(), nullable=True),
        StructField("model", StringType(), nullable=True),
        StructField("year", IntegerType(), nullable=True),
        StructField("fuelType", StringType(), nullable=True)
    ])

    # GPS schema
    gpsSchema = StructType([
        StructField("id", StringType(), nullable=True),
        StructField("deviceId", StringType(), nullable=True),
        StructField("timestamp", TimestampType(), nullable=True),
        StructField("speed", DoubleType(), nullable=True),
        StructField("direction", StringType(), nullable=True),
        StructField("vehicleType", StringType(), nullable=True)
    ])

    # Traffic schema
    trafficSchema = StructType([
        StructField("id", StringType(), nullable=True),
        StructField("deviceId", StringType(), nullable=True),
        StructField("timestamp", TimestampType(), nullable=True),
        StructField("cameraId", StringType(), nullable=True),
        StructField("location", StringType(), nullable=True),
        StructField("snapshot", StringType(), nullable=True)
    ])

    # Weather schema
    weatherSchema = StructType([
        StructField("id", StringType(), nullable=True),
        StructField("deviceId", StringType(), nullable=True),
        StructField("timestamp", TimestampType(), nullable=True),
        StructField("location", StringType(), nullable=True),
        StructField("temperature", DoubleType(), nullable=True),
        StructField("weatherCondition", StringType(), nullable=True),
        StructField("precipitation", DoubleType(), nullable=True),
        StructField("windSpeed", DoubleType(), nullable=True),
        StructField("humidity", IntegerType(), nullable=True),
        StructField("airQualityIndex", DoubleType(), nullable=True)
    ])

    # Emergency schema
    emergencySchema = StructType([
        StructField("id", StringType(), nullable=True),
        StructField("deviceId", StringType(), nullable=True),
        StructField("timestamp", TimestampType(), nullable=True),
        StructField("incidentId", StringType(), nullable=True),
        StructField("location", StringType(), nullable=True),
        StructField("type", StringType(), nullable=True),
        StructField("status", StringType(), nullable=True),
        StructField("description", StringType(), nullable=True)
    ])

    def read_kafka_topic(topic, schema):
        return (spark.readStream
               .format('kafka')
               .option('kafka.bootstrap.servers', 'broker:29092')
               .option('subscribe', topic)
               .option('startingOffsets', 'earliest')
               .load()
               .selectExpr('CAST(value AS STRING)')
               .select(from_json(col('value'), schema).alias('data'))
               .select('data.*')
               .withWatermark('timestamp', '2 minutes'))

    def streamWriter(input: DataFrame, checkpointFolder, output):
        return (input.writeStream
                .format('parquet')
                .option('checkpointLocation', checkpointFolder)
                .option('path', output)
                .outputMode('append')
                .start())
    vehicleDF = read_kafka_topic('vehicle_data', vehicleSchema).alias('vehicle')
    gpsDF = read_kafka_topic('gps_data', gpsSchema).alias('gps')
    trafficDF = read_kafka_topic('traffic_data', trafficSchema).alias('traffic')
    weatherDF = read_kafka_topic('weather_data', weatherSchema).alias('weather')
    emergencyDF = read_kafka_topic('emergency_data', emergencySchema).alias('emergency')

    # Here you can perform joins or other transformations as needed
    # Example join on id and timestamp could be added below
    query1 = streamWriter(vehicleDF, 's3a://streaming-data-smartcity/checkpoints/vehicle_data',
                              's3a://streaming-data-smartcity/data/vehicle_data')
    query2 = streamWriter(gpsDF, 's3a://streaming-data-smartcity/checkpoints/gps_data',
                              's3a://streaming-data-smartcity/data/gps_data')
    query3 = streamWriter(trafficDF, 's3a://streaming-data-smartcity/checkpoints/traffic_data',
                              's3a://streaming-data-smartcity/data/traffic_data')
    query4 = streamWriter(weatherDF, 's3a://streaming-data-smartcity/checkpoints/weather_data',
                              's3a://streaming-data-smartcity/data/weather_data')
    query5 = streamWriter(emergencyDF, 's3a://streaming-data-smartcity/checkpoints/emergency_data',
                              's3a://streaming-data-smartcity/data/emergency_data')
    query5.awaitTermination()
if __name__ == "__main__":
    main()
