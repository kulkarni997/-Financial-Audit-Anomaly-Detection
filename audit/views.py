from django.shortcuts import render
from django.http import JsonResponse
from django.views.decorators.http import require_http_methods
from django.views.decorators.csrf import csrf_exempt, ensure_csrf_cookie
from django.middleware.csrf import get_token
import json
import os
from pathlib import Path
import pandas as pd
import numpy as np
from datetime import datetime

# Create uploads directory if it doesn't exist
UPLOADS_DIR = Path('uploaded_files')
UPLOADS_DIR.mkdir(exist_ok=True)

ANOMALIES_FILE = Path('anomalies.json')
AUDIT_HISTORY_FILE = Path('audit_history.json')

def dashboard(request):
    return render(request, 'Dashboard.html')

def upload(request):
    # Ensure CSRF cookie is set
    get_token(request)
    return render(request, 'upload.html')

def anomalies(request):
    return render(request, 'anomalies.html')

def audits(request):
    return render(request, 'audits.html')

def reports(request):
    return render(request, 'reports.html')

def settings_view(request):
    return render(request, 'settings.html')


# ═════════════════════════════════════════════════════════════════
# AUDIT HISTORY FUNCTIONS
# ═════════════════════════════════════════════════════════════════

def load_audit_history():
    """Load audit history from JSON file"""
    if AUDIT_HISTORY_FILE.exists():
        try:
            with open(AUDIT_HISTORY_FILE, 'r') as f:
                return json.load(f)
        except:
            return []
    return []

def save_audit_history(history):
    """Save audit history to JSON file"""
    with open(AUDIT_HISTORY_FILE, 'w') as f:
        json.dump(history, f, indent=2)

def log_audit_event(event_type, title, description, details=None):
    """Log an audit event"""
    history = load_audit_history()
    
    event = {
        'id': f"audit_{int(datetime.now().timestamp() * 1000)}",
        'timestamp': datetime.now().isoformat(),
        'event_type': event_type,  # upload, detection, export, system
        'title': title,
        'description': description,
        'details': details or {}
    }
    
    history.append(event)
    save_audit_history(history)
    return event


# ═════════════════════════════════════════════════════════════════
# ANOMALY DETECTION FUNCTIONS
# ═════════════════════════════════════════════════════════════════

def load_anomalies():
    """Load anomalies from JSON file"""
    if ANOMALIES_FILE.exists():
        try:
            with open(ANOMALIES_FILE, 'r') as f:
                return json.load(f)
        except:
            return []
    return []

def save_anomalies(anomalies):
    """Save anomalies to JSON file"""
    with open(ANOMALIES_FILE, 'w') as f:
        json.dump(anomalies, f, indent=2)

def detect_outliers_iqr(df, column):
    """Detect outliers using Interquartile Range method"""
    Q1 = df[column].quantile(0.25)
    Q3 = df[column].quantile(0.75)
    IQR = Q3 - Q1
    lower_bound = Q1 - 1.5 * IQR
    upper_bound = Q3 + 1.5 * IQR
    return (df[column] < lower_bound) | (df[column] > upper_bound)

def detect_outliers_zscore(df, column, threshold=3):
    """Detect outliers using Z-score method"""
    z_scores = np.abs((df[column] - df[column].mean()) / df[column].std())
    return z_scores > threshold

def analyze_file_for_anomalies(file_path):
    """Analyze uploaded file for anomalies"""
    anomalies = []
    
    try:
        # Load file
        if str(file_path).endswith('.csv'):
            df = pd.read_csv(file_path)
        else:
            df = pd.read_excel(file_path)
        
        # Ensure we have required columns
        amount_col = None
        for col in df.columns:
            if col.lower() in ['amount', 'value', 'transaction_amount']:
                amount_col = col
                break
        
        if not amount_col:
            return anomalies
        
        # Convert to numeric
        df['amount_numeric'] = pd.to_numeric(df[amount_col], errors='coerce')
        df = df.dropna(subset=['amount_numeric'])
        
        if len(df) < 5:  # Need at least 5 rows for statistical analysis
            return anomalies
        
        # Detect outliers using IQR
        outliers_iqr = detect_outliers_iqr(df, 'amount_numeric')
        
        # Detect outliers using Z-score
        outliers_zscore = detect_outliers_zscore(df, 'amount_numeric')
        
        # Combine detections
        for idx, row in df.iterrows():
            is_outlier_iqr = outliers_iqr.iloc[idx]
            is_outlier_zscore = outliers_zscore.iloc[idx]
            
            if is_outlier_iqr or is_outlier_zscore:
                amount = row['amount_numeric']
                mean_amount = df['amount_numeric'].mean()
                std_amount = df['amount_numeric'].std()
                
                # Calculate risk level
                deviation = abs(amount - mean_amount) / (std_amount if std_amount > 0 else 1)
                
                if deviation > 4:
                    risk_level = 'critical'
                elif deviation > 3:
                    risk_level = 'high'
                elif deviation > 2:
                    risk_level = 'medium'
                else:
                    risk_level = 'low'
                
                # Determine reason
                if amount > mean_amount * 2:
                    reason = f"Unusually high transaction amount (${amount:.2f} vs avg ${mean_amount:.2f})"
                    anomaly_type = "outlier"
                else:
                    reason = f"Statistical outlier detected (z-score: {deviation:.2f})"
                    anomaly_type = "outlier"
                
                # Get date
                date_col = None
                for col in df.columns:
                    if col.lower() in ['date', 'transaction_date', 'datetime']:
                        date_col = col
                        break
                
                date_str = str(row[date_col]) if date_col else datetime.now().isoformat()
                
                # Get account
                account_col = None
                for col in df.columns:
                    if col.lower() in ['account', 'account_id', 'account_number']:
                        account_col = col
                        break
                
                account_id = str(row[account_col]) if account_col else None
                
                anomaly = {
                    'id': f"anom_{int(datetime.now().timestamp() * 1000)}_{idx}",
                    'date': date_str,
                    'amount': float(amount),
                    'account_id': account_id,
                    'type': anomaly_type,
                    'risk_level': risk_level,
                    'reason': reason,
                    'description': f"Row {idx} from {Path(file_path).name}",
                    'source_file': Path(file_path).name,
                    'created_at': datetime.now().isoformat()
                }
                anomalies.append(anomaly)
    
    except Exception as e:
        print(f"Error analyzing file: {e}")
    
    return anomalies


# ═════════════════════════════════════════════════════════════════
# API ENDPOINTS
# ═════════════════════════════════════════════════════════════════

@require_http_methods(["POST"])
@csrf_exempt  # Allow uploads without CSRF for API access
def api_upload_file(request):
    """Handle file upload"""
    try:
        if 'file' not in request.FILES:
            return JsonResponse({'error': 'No file provided'}, status=400)
        
        uploaded_file = request.FILES['file']
        
        # Validate file
        if uploaded_file.size > 50 * 1024 * 1024:  # 50 MB
            return JsonResponse({'error': 'File too large (max 50 MB)'}, status=400)
        
        # Save file
        file_path = UPLOADS_DIR / uploaded_file.name
        with open(file_path, 'wb+') as f:
            for chunk in uploaded_file.chunks():
                f.write(chunk)
        
        # Process file to count rows
        row_count = 0
        try:
            if uploaded_file.name.endswith('.csv'):
                df = pd.read_csv(file_path)
            elif uploaded_file.name.endswith(('.xlsx', '.xls')):
                df = pd.read_excel(file_path)
            else:
                return JsonResponse({'error': 'Unsupported file format'}, status=400)
            
            row_count = len(df)
        except Exception as e:
            row_count = 0
        
        # Log audit event
        log_audit_event(
            'upload',
            f'File Uploaded: {uploaded_file.name}',
            f'Successfully uploaded transaction data file',
            {
                'filename': uploaded_file.name,
                'size': f"{uploaded_file.size / 1024:.2f} KB",
                'rows': row_count,
                'count': row_count
            }
        )
        
        return JsonResponse({
            'success': True,
            'filename': uploaded_file.name,
            'size': uploaded_file.size,
            'row_count': row_count
        }, status=201)
    
    except Exception as e:
        log_audit_event(
            'upload',
            'File Upload Failed',
            f'Error uploading file: {str(e)}',
            {'error': str(e)}
        )
        return JsonResponse({'error': str(e)}, status=500)


@require_http_methods(["GET"])
def api_get_uploads(request):
    """Get list of uploaded files"""
    try:
        files = []
        if UPLOADS_DIR.exists():
            for file_path in sorted(UPLOADS_DIR.glob('*'), key=lambda p: p.stat().st_mtime, reverse=True):
                if file_path.is_file():
                    stat = file_path.stat()
                    
                    # Try to get row count
                    row_count = 0
                    try:
                        if file_path.suffix == '.csv':
                            df = pd.read_csv(file_path)
                        elif file_path.suffix in ('.xlsx', '.xls'):
                            df = pd.read_excel(file_path)
                        else:
                            continue
                        row_count = len(df)
                    except:
                        pass
                    
                    files.append({
                        'name': file_path.name,
                        'size': stat.st_size,
                        'uploaded_at': pd.Timestamp.fromtimestamp(stat.st_mtime).isoformat(),
                        'row_count': row_count
                    })
        
        return JsonResponse({'files': files})
    
    except Exception as e:
        return JsonResponse({'error': str(e)}, status=500)


@require_http_methods(["GET"])
def api_get_anomalies(request):
    """Get list of anomalies"""
    try:
        anomalies = load_anomalies()
        return JsonResponse({'anomalies': anomalies})
    except Exception as e:
        return JsonResponse({'error': str(e)}, status=500)


@require_http_methods(["GET"])
def api_anomaly_stats(request):
    """Get anomaly statistics"""
    try:
        anomalies = load_anomalies()
        
        stats = {
            'total': len(anomalies),
            'critical': len([a for a in anomalies if a.get('risk_level') == 'critical']),
            'high': len([a for a in anomalies if a.get('risk_level') == 'high']),
            'medium': len([a for a in anomalies if a.get('risk_level') == 'medium']),
            'low': len([a for a in anomalies if a.get('risk_level') == 'low']),
        }
        
        return JsonResponse(stats)
    except Exception as e:
        return JsonResponse({'error': str(e)}, status=500)


@require_http_methods(["POST"])
@csrf_exempt
def api_detect_anomalies(request):
    """Detect anomalies in uploaded files"""
    try:
        all_anomalies = []
        
        # Analyze each uploaded file
        if UPLOADS_DIR.exists():
            for file_path in UPLOADS_DIR.glob('*'):
                if file_path.is_file():
                    file_anomalies = analyze_file_for_anomalies(file_path)
                    all_anomalies.extend(file_anomalies)
        
        # Save anomalies
        save_anomalies(all_anomalies)
        
        # Log audit event
        log_audit_event(
            'detection',
            'Anomaly Detection Completed',
            f'Analyzed uploaded files for anomalies',
            {
                'anomalies_detected': len(all_anomalies),
                'critical': len([a for a in all_anomalies if a.get('risk_level') == 'critical']),
                'high': len([a for a in all_anomalies if a.get('risk_level') == 'high']),
                'medium': len([a for a in all_anomalies if a.get('risk_level') == 'medium']),
                'count': len(all_anomalies)
            }
        )
        
        return JsonResponse({
            'success': True,
            'detected_count': len(all_anomalies),
            'message': f'Detected {len(all_anomalies)} anomalies'
        })
    
    except Exception as e:
        log_audit_event(
            'detection',
            'Anomaly Detection Failed',
            f'Error detecting anomalies: {str(e)}',
            {'error': str(e)}
        )
        return JsonResponse({'error': str(e)}, status=500)


@require_http_methods(["GET"])
def api_audit_history(request):
    """Get audit history"""
    try:
        history = load_audit_history()
        return JsonResponse({'events': history})
    except Exception as e:
        return JsonResponse({'error': str(e)}, status=500)


@require_http_methods(["GET"])
def api_audit_stats(request):
    """Get audit statistics"""
    try:
        history = load_audit_history()
        
        stats = {
            'total': len(history),
            'uploads': len([e for e in history if e.get('event_type') == 'upload']),
            'detections': len([e for e in history if e.get('event_type') == 'detection']),
            'exports': len([e for e in history if e.get('event_type') == 'export']),
        }
        
        return JsonResponse(stats)
    except Exception as e:
        return JsonResponse({'error': str(e)}, status=500)
