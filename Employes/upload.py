import os
import pandas as pd
import psycopg2

# ============================
# CONFIG
# ============================
DB_CONFIG = {
    "dbname": "AuditAI",
    "user": "postgres",
    "password": "Next@123",
    "host": "localhost",
    "port": "5432"
}

# ✅ DEFINE BASE_DIR FIRST
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# ⚠️ use YOUR folder name (uplods OR uploads)
FOLDER_PATH = os.path.join(BASE_DIR, "uplods")

# ✅ Create folder if not exists
os.makedirs(FOLDER_PATH, exist_ok=True)

print("Exists:", os.path.exists(FOLDER_PATH))
print("Path:", FOLDER_PATH)

# ============================
# CONNECT DB
# ============================
conn = psycopg2.connect(**DB_CONFIG)
cur = conn.cursor()

# ============================
# LOOP FILES
# ============================
for file in os.listdir(FOLDER_PATH):

    if file.endswith(".csv"):
        file_path = os.path.join(FOLDER_PATH, file)
        print(f"Processing: {file}")

        df = pd.read_csv(file_path)

        df.columns = df.columns.str.strip().str.lower()

        df.rename(columns={
            'emp id': 'emp_id',
            'employee_id': 'emp_id'
        }, inplace=True)

        df['amount'] = pd.to_numeric(df['amount'], errors='coerce')
        df = df.dropna(subset=['amount'])

        df['date'] = pd.to_datetime(df['date'], errors='coerce')

        for _, row in df.iterrows():
            cur.execute("""
                INSERT INTO expense (emp_id, amount, vendor, category, department, date)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (
                row.get('emp_id'),
                float(row.get('amount')),
                row.get('vendor'),
                row.get('category'),
                row.get('department'),
                row.get('date')
            ))

# ============================
# FINALIZE
# ============================
conn.commit()
cur.close()
conn.close()

print("✅ All files uploaded successfully!")