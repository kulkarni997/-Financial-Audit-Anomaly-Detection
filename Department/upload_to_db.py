import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import urllib.parse
import os
import glob

# ==============================
# 1. DIRECTORY CONFIG
# ==============================
folder_path = r"C:\Users\HP\OneDrive\Desktop\AuditAI\Department\uplods"
# Find all CSV files in that folder
csv_files = glob.glob(os.path.join(folder_path, "*.csv"))

if not csv_files:
    print(f"❌ No CSV files found in {folder_path}")
    exit()

print(f"📂 Found {len(csv_files)} files. Starting bulk upload...")

# ==============================
# 2. DB CONNECTION DETAILS
# ==============================
DB_NAME = "AuditAI"
DB_USER = "postgres"
DB_PASSWORD = "Next@123"   
DB_HOST = "localhost"
DB_PORT = "5432"

# Encode password for SQLAlchemy
safe_password = urllib.parse.quote_plus(DB_PASSWORD)

# ==============================
# 3. CREATE DATABASE (if not exists)
# ==============================
try:
    conn = psycopg2.connect(
        dbname="postgres", user=DB_USER, password=DB_PASSWORD, 
        host=DB_HOST, port=DB_PORT
    )
    conn.autocommit = True
    cursor = conn.cursor()
    cursor.execute(f"SELECT 1 FROM pg_database WHERE datname = '{DB_NAME}'")
    if not cursor.fetchone():
        cursor.execute(f"CREATE DATABASE {DB_NAME}")
        print(f"Database '{DB_NAME}' created ✅")
    cursor.close()
    conn.close()
except Exception as e:
    print("DB Connection Error:", e)

# ==============================
# 4. LOOP & UPLOAD
# ==============================
engine = create_engine(
    f"postgresql://{DB_USER}:{safe_password}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

TABLE_NAME = "transactions"

for i, file in enumerate(csv_files):
    file_name = os.path.basename(file)
    try:
        data = pd.read_csv(file)
        data = data.dropna()
        data['date'] = pd.to_datetime(data['date'])

        # First file REPLACES the table, others APPEND to it
        mode = "replace" if i == 0 else "append"
        
        data.to_sql(TABLE_NAME, engine, if_exists=mode, index=False)
        print(f"🚀 [{i+1}/{len(csv_files)}] Uploaded: {file_name}")

    except Exception as e:
        print(f"⚠️ Failed to upload {file_name}: {e}")

print(f"\n✅ All data consolidated into table '{TABLE_NAME}'.")

# ==============================
# 5. FINAL ROW COUNT CHECK
# ==============================
with engine.connect() as conn:
    # Use text() to wrap your raw SQL string
    query = text(f"SELECT COUNT(*) FROM {TABLE_NAME}")
    count = conn.execute(query).fetchone()[0]
    print(f"📊 Total records now in DB: {count}")