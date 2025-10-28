from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window, sum as Fsum, count as Fcount, approx_count_distinct as Fdistinct
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType

spark = SparkSession.builder.appName("EcommerceStreaming").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("event_type", StringType()),
    StructField("user_id", IntegerType()),
    StructField("product_id", IntegerType()),
    StructField("category", StringType()),
    StructField("quantity", IntegerType()),
    StructField("price", DoubleType()),
    StructField("amount", DoubleType()),
    StructField("timestamp", TimestampType()),
])

raw = (spark.readStream.format("kafka")
       .option("kafka.bootstrap.servers", "localhost:9092")
       .option("subscribe", "ecommerce_events")
       .load())

events = raw.select(from_json(col("value").cast("string"), schema).alias("e")).select("e.*")

metrics = (events
    .withWatermark("timestamp", "2 minutes")
    .groupBy(window(col("timestamp"), "1 minute", "30 seconds"), col("category"))
    .agg(Fcount("*").alias("events"),
         Fsum("amount").alias("revenue"),
         Fdistinct("user_id").alias("unique_users"),
         Fsum("quantity").alias("items"))
    .orderBy(col("window").start.asc(), col("category").asc()))

query = (metrics.writeStream
         .outputMode("update")
         .format("console")
         .option("truncate", "false")
         .option("numRows", 1000)
         .start())

query.awaitTermination()
