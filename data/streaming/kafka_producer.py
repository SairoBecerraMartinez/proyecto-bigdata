import time, json, random
from datetime import datetime, timezone
from kafka import KafkaProducer

CATEGORIES = ["electronics","fashion","home","beauty","sports","toys"]

def generate_event():
    user_id = random.randint(1, 5000)
    product_id = random.randint(1000, 9999)
    category = random.choice(CATEGORIES)
    quantity = random.randint(1, 4)
    price = round(random.uniform(5.0, 300.0), 2)
    ts = datetime.now(timezone.utc).isoformat()
    return {
        "event_type": "purchase",
        "user_id": user_id,
        "product_id": product_id,
        "category": category,
        "quantity": quantity,
        "price": price,
        "amount": round(quantity*price, 2),
        "timestamp": ts
    }

producer = KafkaProducer(
    bootstrap_servers=["localhost:9092"],
    value_serializer=lambda x: json.dumps(x).encode("utf-8"),
    linger_ms=50,
)

print("Produciendo eventos en 'ecommerce_events' (Ctrl+C para detener)…")
while True:
    e = generate_event()
    producer.send("ecommerce_events", value=e)
    print("Sent:", e)
    time.sleep(0.5)
