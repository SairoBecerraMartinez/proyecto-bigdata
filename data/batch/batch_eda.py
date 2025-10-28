from pyspark.sql import SparkSession, functions as F, types as T

spark = SparkSession.builder.appName("EcommerceBatchEDA").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# 1) Carga
df = (spark.read
      .option("header", True)
      .csv("data/transactions.csv"))

# 2) Limpieza / Transformación
df = (df
    .withColumn("user_id", F.col("user_id").cast(T.IntegerType()))
    .withColumn("product_id", F.col("product_id").cast(T.IntegerType()))
    .withColumn("quantity", F.col("quantity").cast(T.IntegerType()))
    .withColumn("price", F.col("price").cast(T.DoubleType()))
    .withColumn("amount",
        F.when(F.col("amount").isNotNull(), F.col("amount").cast(T.DoubleType()))
         .otherwise(F.col("quantity") * F.col("price")))
    .withColumn("timestamp", F.to_timestamp("timestamp"))
    .withColumn("category", F.coalesce(F.col("category"), F.lit("unknown")))
    .dropna(subset=["user_id","product_id","quantity","price","timestamp"]))

# (Demo RDD) Conteo por categoría
rdd_counts = (df.select("category").rdd.map(lambda r: (r["category"],1)).reduceByKey(lambda a,b:a+b).collect())
print("RDD category counts:", rdd_counts)

# 3) EDA/KPIs
daily = (df.groupBy(F.window("timestamp","1 day").alias("day"))
           .agg(F.count("*").alias("events"),
                F.sum("amount").alias("revenue"),
                F.approx_count_distinct("user_id").alias("unique_users"),
                F.sum("quantity").alias("items")))

by_cat = (df.groupBy("category")
            .agg(F.sum("amount").alias("revenue"),
                 F.count("*").alias("events"),
                 F.sum("quantity").alias("items"))
            .orderBy(F.col("revenue").desc()))

stats = (df.agg(F.count("*").alias("n"),
                F.mean("price").alias("avg_price"),
                F.expr("percentile(price, 0.5)").alias("p50_price"),
                F.expr("percentile(price, 0.9)").alias("p90_price"),
                F.mean("amount").alias("avg_ticket")))

# 4) Guardado
daily.write.mode("overwrite").parquet("out/daily")
by_cat.write.mode("overwrite").parquet("out/by_category")
stats.write.mode("overwrite").parquet("out/summary")

spark.stop()
