import os
import pandas as pd
import joblib
import psycopg2
from django.shortcuts import render
from django.conf import settings
from django.core.files.storage import FileSystemStorage

# Helper to load ML artifacts safely (Ideally called once in AppConfig.ready)
def load_assets():
    model_path = os.path.join(settings.BASE_DIR, "Models")
    try:
        return {
            "model": joblib.load(os.path.join(model_path, "model.pkl")),
            "scaler": joblib.load(os.path.join(model_path, "scaler.pkl")),
            "le_vendor": joblib.load(os.path.join(model_path, "le_vendor.pkl")),
            "le_category": joblib.load(os.path.join(model_path, "le_category.pkl")),
            "le_department": joblib.load(os.path.join(model_path, "le_department.pkl")),
            "le_emp": joblib.load(os.path.join(model_path, "le_emp.pkl")),
        }
    except Exception as e:
        print(f"Error loading models: {e}")
        return None

ASSETS = load_assets()

def get_history():
    """Fetch historical expense data using Django's DB settings or psycopg2"""
    db = settings.DATABASES['default']
    try:
        conn = psycopg2.connect(
            dbname=db['NAME'],
            user=db['USER'],
            password=db['PASSWORD'],
            host=db['HOST'],
            port=db['PORT']
        )
        df = pd.read_sql("SELECT * FROM expense", conn)
        conn.close()
        df.columns = df.columns.str.strip().str.lower()
        return df
    except Exception:
        return pd.DataFrame()

def safe_transform(le, series):
    mapping = {label: idx for idx, label in enumerate(le.classes_)}
    return series.map(lambda x: mapping.get(x, 0))

def index(request):
    context = {}
    
    if request.method == "POST" and request.FILES.get('file'):
        uploaded_file = request.FILES['file']
        
        # Load data
        if uploaded_file.name.endswith(".csv"):
            df = pd.read_csv(uploaded_file)
        else:
            df = pd.read_excel(uploaded_file)

        # Standardize Columns
        df.columns = df.columns.str.strip().str.lower()
        df.rename(columns={'emp id': 'emp_id', 'employee_id': 'emp_id'}, inplace=True)
        
        # 1. Feature Engineering
        df['amount'] = pd.to_numeric(df['amount'], errors='coerce')
        df = df.dropna(subset=['amount'])
        df['date'] = pd.to_datetime(df['date'], errors='coerce')
        df['day_of_week'] = df['date'].dt.dayofweek.fillna(0)

        history = get_history()
        if not history.empty:
            df['avg_amount'] = df['emp_id'].map(history.groupby('emp_id')['amount'].mean()).fillna(df['amount'])
            df['frequency'] = df['emp_id'].map(history.groupby('emp_id').size()).fillna(1)
            df['vendor_freq'] = 1 
        else:
            df['avg_amount'], df['frequency'], df['vendor_freq'] = df['amount'], 1, 1

        # 2. Encoding
        df['vendor_encoded'] = safe_transform(ASSETS['le_vendor'], df['vendor'])
        df['category_encoded'] = safe_transform(ASSETS['le_category'], df['category'])
        df['department_encoded'] = safe_transform(ASSETS['le_department'], df['department'])

        X = pd.DataFrame({
            'amount': df['amount'],
            'avg_amount': df['avg_amount'],
            'frequency': df['frequency'],
            'vendor_freq': df['vendor_freq'],
            'day_of_week': df['day_of_week'],
            'vendor': df['vendor_encoded'],
            'category': df['category_encoded'],
            'department': df['department_encoded']
        })

        # 3. Inference
        X_scaled = ASSETS['scaler'].transform(X)
        df['risk_score'] = ASSETS['model'].decision_function(X_scaled)
        df['prediction'] = df['risk_score'].apply(lambda x: "Fraud" if x < -0.05 else "Normal")

        # 4. Context Preparation
        fraud_df = df[df['prediction'] == "Fraud"].copy()
        
        if not fraud_df.empty:
            table_df = fraud_df[['date', 'amount', 'emp_id', 'avg_amount', 'risk_score']].copy()
            table_df['date'] = table_df['date'].dt.strftime('%Y-%m-%d')
            table_df['risk_score'] = table_df['risk_score'].round(4)
            
            context['table'] = table_df.to_html(classes="table table-hover align-middle", index=False)
            context['summary'] = {"count": len(fraud_df), "total": fraud_df['amount'].sum()}
            context['chart_data'] = {
                "labels": df['emp_id'].astype(str).head(20).tolist(), 
                "scores": df['risk_score'].head(20).tolist()
            }

    return render(request, "dashboard.html", context)