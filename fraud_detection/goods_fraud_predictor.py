import os
import pandas as pd
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import LabelEncoder

def process_goods_audit(file_path: str):
    # Ensure we are pointing to a file, not a folder
    if not os.path.isfile(file_path):
        raise FileNotFoundError(f"CSV file not found: {file_path}")

    df = pd.read_csv(file_path)
    df.columns = df.columns.str.strip().str.lower()

    # Handle date column
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df["day_of_week"] = df["date"].dt.dayofweek
    else:
        df["day_of_week"] = 0

    # Encode categorical columns
    cat_cols = ["product_id", "category", "vendor"]
    for col in cat_cols:
        if col in df.columns:
            le = LabelEncoder()
            df[f"{col}_enc"] = le.fit_transform(df[col].astype(str))
        else:
            df[f"{col}_enc"] = 0

    # Features for anomaly detection
    features = ["unit_price", "quantity", "total_price", "day_of_week"] + [f"{col}_enc" for col in cat_cols]
    X = df[features].fillna(0)

    # Train Isolation Forest
    model = IsolationForest(n_estimators=100, contamination=0.2, random_state=42)
    model.fit(X)

    # Predict anomalies
    df["raw_score"] = model.decision_function(X)
    df["is_anomaly"] = df["raw_score"].apply(lambda x: "ANOMALY" if x < -0.05 else "Normal")

    # Return anomalies only
    return df[df["is_anomaly"] == "ANOMALY"][[
        "product_name", "unit_price", "quantity", "total_price", "vendor", "raw_score", "is_anomaly"
    ]]
