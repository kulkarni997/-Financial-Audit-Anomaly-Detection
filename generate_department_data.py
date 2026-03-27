import pandas as pd
import random
from datetime import datetime, timedelta
import os

os.makedirs("department_data", exist_ok=True)

departments = ["Finance", "IT", "HR", "Operations"]

base_budget = {
    "Finance": 500000,
    "IT": 700000,
    "HR": 300000,
    "Operations": 600000
}

expense_limit = {
    "Finance": 10000,
    "IT": 15000,
    "HR": 8000,
    "Operations": 12000
}

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
        for dept in departments:

            budget = int(base_budget[dept] * random.uniform(0.95, 1.1))
            avg_tx = int(random.uniform(2000, 6000))

            rows.append([
                current_date.strftime("%Y-%m-%d"),
                dept,
                budget,
                expense_limit[dept],
                50,
                avg_tx
            ])

        current_date += timedelta(days=1)

    df = pd.DataFrame(rows, columns=[
        "date",
        "department",
        "monthly_budget",
        "expense_limit",
        "head_count",
        "avg_transaction"
    ])

    df.to_csv(f"department_data/department_{i}.csv", index=False)

    print(f"Created: department_{i}.csv")