import pandas as pd
import joblib
from sklearn.preprocessing import LabelEncoder

MODEL_DIR = r"C:\Users\HP\OneDrive\Desktop\EDUNET\-Financial-Audit-Anomaly-Detection\audit\ml_assets\Emp_Models"

model = joblib.load(f"{MODEL_DIR}/model.pkl")
scaler = joblib.load(f"{MODEL_DIR}/scaler.pkl")
le_vendor = joblib.load(f"{MODEL_DIR}/le_vendor.pkl")
le_category = joblib.load(f"{MODEL_DIR}/le_category.pkl")
le_department = joblib.load(f"{MODEL_DIR}/le_department.pkl")
le_emp = joblib.load(f"{MODEL_DIR}/le_emp.pkl")

def safe_transform(le, series):
    mapping = {label: idx for idx, label in enumerate(le.classes_)}
    return series.map(lambda x: mapping.get(x, 0))

def process_employee_audit(file_path, history_df=pd.DataFrame()):
    df = pd.read_csv(file_path)
    df.columns = df.columns.str.strip().str.lower()
    df.rename(columns={'emp id':'emp_id','employee_id':'emp_id'}, inplace=True)

    if "emp_id" not in df.columns or "amount" not in df.columns:
        raise KeyError("Missing emp_id or amount column")

    df["emp_id_original"] = df["emp_id"]

    df["amount"] = df["amount"].replace("[₹,]", "", regex=True)
    df["amount"] = pd.to_numeric(df["amount"], errors="coerce")
    df = df.dropna(subset=["amount"])

    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df["day_of_week"] = df["date"].dt.dayofweek
    else:
        df["day_of_week"] = 0

    # History features
    if not history_df.empty:
        history_df.columns = history_df.columns.str.strip().str.lower()
        avg_amount_map = history_df.groupby("emp_id")["amount"].mean()
        freq_map = history_df.groupby("emp_id").size()
        vendor_freq_map = history_df.groupby(["emp_id","vendor"]).size()
    else:
        avg_amount_map, freq_map, vendor_freq_map = {}, {}, {}

    df["avg_amount"] = df["emp_id"].map(avg_amount_map).fillna(df["amount"])
    df["frequency"] = df["emp_id"].map(freq_map).fillna(1)
    df["vendor_freq"] = df.set_index(["emp_id","vendor"]).index.map(vendor_freq_map).fillna(1)

    # Encoding
    df["vendor"] = safe_transform(le_vendor, df.get("vendor", pd.Series([0]*len(df))))
    df["category"] = safe_transform(le_category, df.get("category", pd.Series([0]*len(df))))
    df["department"] = safe_transform(le_department, df.get("department", pd.Series([0]*len(df))))
    df["emp_id"] = safe_transform(le_emp, df["emp_id"])

    features = ["amount","avg_amount","frequency","vendor_freq","day_of_week","vendor","category","department"]
    X_test = df[features].fillna(0)

    X_scaled = scaler.transform(X_test)
    df["risk_score"] = model.decision_function(X_scaled)
    df["is_anomaly"] = df["risk_score"].apply(lambda x: "ANOMALY" if x < -0.05 else "Normal")

    return df[df["is_anomaly"]=="ANOMALY"][["emp_id_original","amount","risk_score","is_anomaly"]]
