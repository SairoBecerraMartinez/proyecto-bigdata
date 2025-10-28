# E-commerce: Batch + Streaming (Spark + Kafka)

## Estructura
- `data/generate_transactions.py` → genera `data/transactions.csv` (~500k filas)
- `batch/batch_eda.py` → KPIs diarios y por categoría, salida Parquet en `out/`
- `streaming/kafka_producer.py` → simula compras a Kafka (topic `ecommerce_events`)
- `streaming/spark_streaming_consumer.py` → ventanas 1 min / slide 30 s / watermark 2 min

## Ejecución
Batch:
```bash
python3 data/generate_transactions.py
spark-submit batch/batch_eda.py
