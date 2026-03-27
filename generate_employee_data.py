import pandas as pd
import random
from datetime import datetime, timedelta
import os

os.makedirs("employee_data", exist_ok=True)

vendors = ["Uber", "Swiggy", "Amazon", "Flipkart", "MakeMyTrip"]
categories = ["Travel", "Food", "Software", "Office", "Electronics"]

departments = [
    ("Finance", "FIN"),
    ("IT", "IT"),
    ("HR", "HR"),
    ("Operations", "OPS")
]

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

# ✅ FIXED EMPLOYEES
employee_master = []
for dept_name, prefix in departments:
    for emp in range(1, 51):
        emp_id = f"{prefix}{emp:04d}"
        employee_master.append((emp_id, dept_name))

# 🔥 MAIN LOOP
for i, (start, end) in enumerate(date_ranges, start=1):
    start_date = datetime.strptime(start, "%Y-%m-%d")
    end_date = datetime.strptime(end, "%Y-%m-%d")

    rows = []
    current_date = start_date

    while current_date <= end_date:
        for emp_id, dept_name in employee_master:

            # NEW behavior each file/day
            vendor = random.choice(vendors)
            category = random.choice(categories)

            amount = random.randint(200, 4000)

            # anomalies
            if random.random() < 0.05:
                amount = random.randint(10000, 200000)

            rows.append([
                current_date.strftime("%Y-%m-%d"),
                amount,
                emp_id,
                vendor,
                category,
                dept_name
            ])

        current_date += timedelta(days=1)

    df = pd.DataFrame(rows, columns=[
        "date", "amount", "emp_id", "vendor", "category", "department"
    ])

    df.to_csv(f"employee_data/employee_{i}.csv", index=False)

    print(f"Created: employee_{i}.csv")