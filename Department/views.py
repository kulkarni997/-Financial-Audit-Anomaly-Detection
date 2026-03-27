import os
import joblib
import pandas as pd
import urllib.parse
import plotly.express as px
from django.shortcuts import render
from django.core.files.storage import FileSystemStorage
from sqlalchemy import create_engine

# Configuration (Move paths to settings.py in a real production app)
MODEL_PATH = r"C:\Users\HP\OneDrive\Desktop\AuditAI\Department\Models"
password = urllib.parse.quote_plus("Next@123")
DB_URL = f"postgresql://postgres:{password}@localhost:5432/AuditAI"
engine = create_engine(DB_URL)

def load_ml_resources():
    model = joblib.load(os.path.join(MODEL_PATH, "dept_model.pkl"))
    scaler = joblib.load(os.path.join(MODEL_PATH, "dept_scaler.pkl"))
    return model, scaler

def audit_dashboard(request):
    context = {}
    model, scaler = load_ml_resources()

    if request.method == 'POST' and request.FILES.get('csv_file'):
        # 1. Handle File Upload
        uploaded_file = request.FILES['csv_file']
        fs = FileSystemStorage()
        filename = fs.save(uploaded_file.name, uploaded_file)
        file_path = fs.path(filename)

        # 2. Load Data
        history_df = pd.read_sql("SELECT * FROM transactions", engine)
        new_data = pd.read_csv(file_path)
        
        # 3. Processing Logic
        combined_df = pd.concat([history_df, new_data], ignore_index=True)
        combined_df['date'] = pd.to_datetime(combined_df['date'])
        combined_df['day'] = combined_df['date'].dt.day
        combined_df['month'] = combined_df['date'].dt.month
        
        dept_map = {'Finance': 0, 'IT': 1, 'HR': 2, 'Operations': 3}
        combined_df['dept_name'] = combined_df['department'] 
        combined_df['department'] = combined_df['department'].map(dept_map).fillna(-1)
        combined_df['budget_utilization'] = combined_df['avg_transaction'] / combined_df['expense_limit']
        combined_df['cost_per_employee'] = combined_df['monthly_budget'] / combined_df['head_count']

        features = ['monthly_budget', 'expense_limit', 'head_count', 'avg_transaction', 
                    'budget_utilization', 'cost_per_employee', 'day', 'month', 'department']

        # 4. Prediction
        X_scaled = scaler.transform(combined_df[features])
        combined_df['anomaly_score'] = model.decision_function(X_scaled)
        combined_df['prediction'] = model.predict(X_scaled)
        combined_df['Status'] = combined_df['prediction'].map({1: "Normal", -1: "🚨 Anomaly"})

        # 5. Generate Visual (Plotly to HTML)
        fig = px.scatter(combined_df, x="date", y="avg_transaction", 
                         color="Status", size="budget_utilization",
                         hover_data=['dept_name'],
                         color_discrete_map={"Normal": "#636EFA", "🚨 Anomaly": "#EF553B"},
                         template="plotly_dark")
        graph_html = fig.to_html(full_html=False)

        # 6. Filter Anomalies for display
        new_anomalies_df = combined_df.iloc[len(history_df):] 
        anomalies_only = new_anomalies_df[new_anomalies_df['prediction'] == -1].to_dict('records')

        context = {
            'graph': graph_html,
            'anomalies': anomalies_only,
            'hist_count': len(history_df),
            'new_count': len(new_data),
            'anomaly_count': len(anomalies_only)
        }
        
        # Cleanup uploaded file after processing
        fs.delete(filename)

    return render(request, 'audit_dashboard.html', context)