import pandas as pd
import random
from datetime import datetime, timedelta

rows = []
start_date = datetime(2024, 1, 1)

vendors = ["Uber", "Swiggy", "Amazon", "Flipkart", "MakeMyTrip", "UnknownVendor"]
categories = ["Travel", "Food", "Software", "Office", "Electronics"]
departments = ["Finance", "HR", "IT", "Operations"]

for i in range(1000):
    date = start_date + timedelta(days=random.randint(0, 90))

    amount = random.randint(50, 5000)

    # Inject anomalies
    if random.random() < 0.05:
        amount = random.randint(10000, 1000000)

    vendor = random.choice(vendors)
    category = random.choice(categories)
    department = random.choice(departments)

    is_duplicate = random.choice([0, 1]) if random.random() < 0.1 else 0
    authorized_vendor = 0 if vendor == "UnknownVendor" else 1

    # Risk logic
    if amount > 50000 or authorized_vendor == 0:
        risk = "critical"
    elif amount > 10000:
        risk = "high"
    elif amount > 5000 or is_duplicate == 1:
        risk = "medium"
    else:
        risk = "low"

    rows.append([
        date.strftime("%Y-%m-%d"),
        amount,
        f"ACC{i}",
        vendor,
        category,
        department,
        "expense",
        is_duplicate,
        authorized_vendor,
        risk
    ])

df = pd.DataFrame(rows, columns=[
    "date", "amount", "account_id", "vendor",
    "category", "department", "transaction_type",
    "is_duplicate", "authorized_vendor", "risk_label"
])

df.to_csv("training_data.csv", index=False)

print("✅ Dataset generated: training_data.csv")