import pandas as pd
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import LabelEncoder, StandardScaler

def process_department_audit(file_path):
    data = pd.read_csv(file_path).dropna()
    data.columns = data.columns.str.strip().str.lower()

    if "date" in data.columns:
        data["date"] = pd.to_datetime(data["date"], errors="coerce")
        data["day"] = data["date"].dt.day
        data["month"] = data["date"].dt.month
    else:
        data["day"], data["month"] = 0, 0

    if "department" in data.columns:
        le = LabelEncoder()
        data["department_original"] = data["department"]
        data["department"] = le.fit_transform(data["department"].astype(str))
    else:
        data["department_original"] = "Unknown"
        data["department"] = 0

    data["budget_utilization"] = data["avg_transaction"]/data["expense_limit"]
    data["cost_per_employee"] = data["monthly_budget"]/data["head_count"]

    features = ["monthly_budget","expense_limit","head_count","avg_transaction",
                "budget_utilization","cost_per_employee","day","month","department"]

    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(data[features].fillna(0))

    model = IsolationForest(n_estimators=200, contamination=0.05, random_state=42)
    model.fit(X_scaled)

    data["anomaly_score"] = model.decision_function(X_scaled)
    data["is_anomaly"] = data["anomaly_score"].apply(lambda x: "ANOMALY" if x < -0.05 else "Normal")

    return data[data["is_anomaly"]=="ANOMALY"][["department_original","monthly_budget","expense_limit","anomaly_score","is_anomaly"]]
