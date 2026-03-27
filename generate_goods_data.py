import pandas as pd
import random
from datetime import datetime, timedelta
import os

os.makedirs("goods_data", exist_ok=True)

products = [
    ("P001", "Laptop", "Electronics", 50000),
    ("P002", "Printer", "Electronics", 15000),
    ("P003", "Office Chair", "Office", 5000),
    ("P004", "Desk", "Office", 8000),
    ("P005", "Software License", "Software", 20000),
    ("P006", "Flight Ticket", "Travel", 7000),
    ("P007", "Hotel Booking", "Travel", 6000),
    ("P008", "Food Catering", "Food", 3000)
]

vendors = ["Amazon", "Flipkart", "MakeMyTrip", "Swiggy", "Uber"]

date_ranges = [
    ("2024-01-01", "2024-01-15"),
    ("2024-01-16", "2024-01-31"),
    ("2024-02-01", "2024-02-15"),
    ("2024-02-16", "2024-02-28"),
    ("2024-03-01", "2024-03-15"),
    ("2024-03-16", "2024-03-31"),
    ("2024-04-01", "2024-04-15"),
    ("2024-04-16", "2024-04-30"),
    ("2024-05-01", "2024-05-15"),
    ("2024-05-16", "2024-05-31"),
    ("2024-06-01", "2024-06-15"),
    ("2024-06-16", "2024-06-30"),
]

for i, (start, end) in enumerate(date_ranges, start=1):
    start_date = datetime.strptime(start, "%Y-%m-%d")
    end_date = datetime.strptime(end, "%Y-%m-%d")

    rows = []
    current_date = start_date

    while current_date <= end_date:
        for _ in range(50):  # 50 transactions per day

            product_id, name, category, base_price = random.choice(products)

            quantity = random.randint(1, 10)

            # price variation
            unit_price = int(base_price * random.uniform(0.9, 1.2))
            total_price = unit_price * quantity

            # 🔥 Inject fraud cases
            if random.random() < 0.05:
                total_price += random.randint(1000, 5000)  # mismatch

            rows.append([
                current_date.strftime("%Y-%m-%d"),
                product_id,
                name,
                category,
                unit_price,
                quantity,
                total_price,
                random.choice(vendors)
            ])

        current_date += timedelta(days=1)

    df = pd.DataFrame(rows, columns=[
        "date",
        "product_id",
        "product_name",
        "category",
        "unit_price",
        "quantity",
        "total_price",
        "vendor"
    ])

    df.to_csv(f"goods_data/goods_{i}.csv", index=False)

    print(f"Created: goods_{i}.csv")