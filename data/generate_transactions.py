import csv, random, uuid
from datetime import datetime, timedelta

random.seed(7)
N = 500_000
CATEGORIES = ["electronics","fashion","home","beauty","sports","toys"]
start = datetime(2024,1,1,0,0,0)

with open("data/transactions.csv", "w", newline="", encoding="utf-8") as f:
    w = csv.writer(f)
    w.writerow(["transaction_id","user_id","product_id","category","quantity","price","amount","timestamp"])
    for i in range(N):
        user = random.randint(1, 50000)
        prod = random.randint(1000, 9999)
        cat = random.choice(CATEGORIES)
        qty = random.randint(1, 5)
        price = round(random.uniform(5.0, 300.0), 2)
        amt = round(qty*price, 2)
        ts = start + timedelta(seconds=i%2000000)
        w.writerow([str(uuid.uuid4()), user, prod, cat, qty, price, amt, ts.isoformat(sep=" ")])
print("OK -> data/transactions.csv")
