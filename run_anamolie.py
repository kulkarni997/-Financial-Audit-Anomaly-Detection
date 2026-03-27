import os
import zipfile
import pandas as pd

from fraud_detection.emp_fraud_predictor import process_employee_audit
from fraud_detection.dept_fraud_predictor import process_department_audit
from fraud_detection.goods_fraud_predictor import process_goods_audit

def extract_zip(zip_path, extract_dir):
    """Extracts the given ZIP file into extract_dir."""
    if not os.path.exists(extract_dir):
        os.makedirs(extract_dir)
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_dir)
    return extract_dir

def main():
    zip_path = r"C:\Users\HP\OneDrive\Desktop\EDUNET\-Financial-Audit-Anomaly-Detection\testing.zip"
    extract_dir = r"C:\Users\HP\OneDrive\Desktop\EDUNET\-Financial-Audit-Anomaly-Detection\test_data"

    print("Extracting ZIP...")
    extract_zip(zip_path, extract_dir)

    # Build file paths
    emp_file = os.path.join(extract_dir, "employee_audit.csv")
    dept_file = os.path.join(extract_dir, "department_audit.csv")
    goods_file = os.path.join(extract_dir, "goods_audit.csv")

    print("\n=== Employee Audit ===")
    try:
        history_df = pd.DataFrame()
        emp_anomalies = process_employee_audit(emp_file, history_df)
        print(emp_anomalies if not emp_anomalies.empty else "No employee anomalies found.")
    except Exception as e:
        print("Employee error:", e)

    print("\n=== Department Audit ===")
    try:
        dept_anomalies = process_department_audit(dept_file)
        print(dept_anomalies if not dept_anomalies.empty else "No department anomalies found.")
    except Exception as e:
        print("Department error:", e)

    print("\n=== Goods Audit ===")
    try:
        goods_anomalies = process_goods_audit(goods_file)
        print(goods_anomalies if not goods_anomalies.empty else "No goods anomalies found.")
    except Exception as e:
        print("Goods error:", e)

if __name__ == "__main__":
    main()
