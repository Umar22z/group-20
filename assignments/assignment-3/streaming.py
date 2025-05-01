from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window, avg, count, sum, desc
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType  

#///////////////////initialising the spark session///////////////////////////
spark = SparkSession.builder \
    .appName("RealTimeTrafficMonitoring") \
    .config("spark.hadoop.io.nativeio.disable", "true") \
    .getOrCreate()

#/////////////readding data from kafka//////////////////
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "traffic_data") \
    .option("startingOffsets", "latest") \
    .load()

#////////////converting value to string/////////////////// 
traffic_data = df.selectExpr("CAST(value AS STRING)")

#////////////defining the schema for json data//////////////
schema = StructType([
    StructField("sensor_id", StringType(), True),  
    StructField("average_speed", FloatType(), True),  
    StructField("vehicle_count", IntegerType(), True),
    StructField("congestion_level", StringType(), True),
    StructField("timestamp", TimestampType(), True)
])

#/////////////////applying schema to the encoming data///////////////////
traffic_data = traffic_data.select(from_json(col("value"), schema).alias("data")).select("data.*")

#//////////data quality checks//////////////////////

#//////////Missing Values: Remove or flag records with null sensor_id or timestamp////////////////
traffic_data = traffic_data.dropna(subset=["sensor_id", "timestamp"])  # Fixed case for sensor_id

#//////////////////////Range Validation: Ensure vehicle_count >= 0 and average_speed > 0/////////////////////
traffic_data = traffic_data.filter((col("vehicle_count") >= 0) & (col("average_speed") > 0)) 

#//////////////////////////Duplicate Handling: Remove duplicate events by using deduplication logic//////////
traffic_data = traffic_data.dropDuplicates(["sensor_id", "timestamp"])  
traffic_data = traffic_data.withWatermark("timestamp", "10 minutes")

#/////////////////Compute Real-Time Traic Volume per Sensor/////////////////////////
traffic_volume = traffic_data \
    .groupBy(window(col("timestamp"), "5 minutes"), col("sensor_id")) \
    .sum("vehicle_count") \
    .withColumnRenamed("sum(vehicle_count)", "total_vehicles")  

#////////////////////////// Detect Congestion Hotspots in Real Time//////////////////
congestion_hotspots = traffic_data \
    .filter(col("congestion_level") == "HIGH") \
    .groupBy(window(col("timestamp"), "5 minutes"), col("sensor_id")) \
    .count() \
    .filter(col("count") >= 3)  

#/////////////////Calculate the Average Speed per Sensor with Windowing//////////
avg_speed = traffic_data \
    .groupBy(window(col("timestamp"), "10 minutes"), col("sensor_id")) \
    .agg(avg("average_speed").alias("average_speed"))  

#////////////dentify Sudden Speed Drops////////////
speed_drops = traffic_data \
    .withColumn("prev_avg_speed", col("average_speed") * 1.5) \
    .filter(col("average_speed") < col("prev_avg_speed"))  # Fixed column name

#/////////////Find the Busiest Sensors in the Last 30 Minutes//////////////
busiest_sensors = traffic_data \
    .groupBy(window(col("timestamp"), "30 minutes"), col("sensor_id")) \
    .sum("vehicle_count") \
    .withColumnRenamed("sum(vehicle_count)", "total_vehicles") \
    .orderBy(desc("total_vehicles")) \
    .limit(3)

#///////////STREAMINGGGGGGG/////

query1 = traffic_volume.writeStream.outputMode("update").format("console").start()
query2 = congestion_hotspots.writeStream.outputMode("update").format("console").start()
query3 = avg_speed.writeStream.outputMode("update").format("console").start()
query4 = speed_drops.writeStream.outputMode("update").format("console").start()
query5 = busiest_sensors.writeStream.outputMode("complete").format("console").start()  

query1.awaitTermination()
query2.awaitTermination()
query3.awaitTermination()
query4.awaitTermination()
query5.awaitTermination()
