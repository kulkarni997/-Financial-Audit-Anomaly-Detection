from django.shortcuts import render
from django.http import JsonResponse
from django.views.decorators.http import require_http_methods
from django.views.decorators.csrf import csrf_exempt, ensure_csrf_cookie
from .ocr_utils import extract_text_from_image, extract_amount
from django.middleware.csrf import get_token
import json
import os
from pathlib import Path
from io import StringIO
import pandas as pd
import numpy as np
from datetime import datetime
from rest_framework.decorators import api_view
from rest_framework.response import Response
from .ocr_utils import extract_text_from_image, extract_amount, detect_price_mismatch
import csv
import uuid
from rest_framework.decorators import parser_classes
from drf_yasg.utils import swagger_auto_schema
from drf_yasg import openapi
from rest_framework.parsers import MultiPartParser, FormParser

@api_view(['GET'])
def dashboard_summary(request):
    data = {
        "total_transactions": 1000,
        "flagged_count": 120,
        "critical_count": 10,
        "avg_risk_score": 35
    }
    return Response(data)

@api_view(['GET'])
def trends_view(request):
    data = {
        "labels": ["Nov 1", "Nov 3", "Nov 5", "Nov 7"],
        "flagged": [4, 7, 3, 11],
        "total": [120, 134, 98, 145]
    }
    return Response(data)

@swagger_auto_schema(
    method='post',
    manual_parameters=[
        openapi.Parameter(
            'file',
            openapi.IN_FORM,
            description="Upload invoice image",
            type=openapi.TYPE_FILE,
            required=True
        )
    ]
)
@api_view(['POST'])
@csrf_exempt
@parser_classes([MultiPartParser, FormParser])
def upload_file(request):
    try:
        if 'file' not in request.FILES:
            return Response({'error': 'No file provided'}, status=400)

        uploaded_file = request.FILES['file']

        file_path = UPLOADS_DIR / uploaded_file.name
        with open(file_path, 'wb+') as f:
            for chunk in uploaded_file.chunks():
                f.write(chunk)

        return Response({
            'success': True,
            'filename': uploaded_file.name
        })

    except Exception as e:
        return Response({'error': str(e)}, status=500)

# Create uploads directory if it doesn't exist
UPLOADS_DIR = Path('uploaded_files')
UPLOADS_DIR.mkdir(exist_ok=True)

ANOMALIES_FILE = Path('anomalies.json')
AUDIT_HISTORY_FILE = Path('audit_history.json')

def dashboard(request):
    return render(request, 'Dashboard.html')

def upload(request):
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


@require_http_methods(["POST"])
def api_logout(request):
    try:
        log_audit_event('system', 'User Logout', 'User signed out of the system', {})
        return JsonResponse({'success': True, 'message': 'Logged out successfully'})
    except Exception as e:
        return JsonResponse({'error': str(e)}, status=500)


# ═════════════════════════════════════════════════════════════════
# AUDIT HISTORY FUNCTIONS
# ═════════════════════════════════════════════════════════════════

def load_audit_history():
    if AUDIT_HISTORY_FILE.exists():
        try:
            with open(AUDIT_HISTORY_FILE, 'r') as f:
                return json.load(f)
        except:
            return []
    return []

def save_audit_history(history):
    with open(AUDIT_HISTORY_FILE, 'w') as f:
        json.dump(history, f, indent=2)

def log_audit_event(event_type, title, description, details=None):
    history = load_audit_history()
    event = {
        'id': f"audit_{uuid.uuid4().hex}",
        'timestamp': datetime.now().isoformat(),
        'event_type': event_type,
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
    if ANOMALIES_FILE.exists():
        try:
            with open(ANOMALIES_FILE, 'r') as f:
                return json.load(f)
        except:
            return []
    return []

def save_anomalies(anomalies):
    print("💾 Saving anomalies:", len(anomalies))
    
    with open(ANOMALIES_FILE, 'w') as f:
        json.dump(anomalies, f, indent=2)

    print("✅ Saved to file:", ANOMALIES_FILE.resolve())


def analyze_file_for_anomalies(file_path):
    """Analyze uploaded file for anomalies. Assumes columns: date, amount, account_id, vendor, category"""
    anomalies = []

    try:
        # ── 1. Load file ──────────────────────────────────────────────
        if str(file_path).endswith('.csv'):
            df = pd.read_csv(file_path)
        else:
            df = pd.read_excel(file_path)

        # Normalise column names: strip whitespace, lowercase for lookup
        df.columns = df.columns.str.strip()
        col_map = {c.lower(): c for c in df.columns}

        # ✅ ADD THIS
        data_type = "unknown"

        if 'emp_id' in col_map:
            data_type = "employee"
        elif 'product_id' in col_map:
            data_type = "goods"
        elif 'department' in col_map:
            data_type = "department"

        # ── 2. Resolve 'amount' column ────────────────────────────────
        amount_col = None
        for candidate in ['amount', 'transaction_amount', 'value', 'total_price']:
            if candidate in col_map:
                amount_col = col_map[candidate]
                break

        if amount_col is None:
            print(f"[SKIP] No amount column in {file_path}. Columns: {list(df.columns)}")
            return []

        # ── 3. Clean & validate data ──────────────────────────────────
        df['_amount'] = pd.to_numeric(df[amount_col], errors='coerce')
        df = df.dropna(subset=['_amount']).reset_index(drop=True)  # ← reset_index is key

        if len(df) < 5:
            print(f"[SKIP] Too few rows ({len(df)}) in {file_path}")
            return []

        # ── 4. Resolve optional columns ───────────────────────────────
        date_col    = next((col_map[k] for k in ['date', 'transaction_date', 'datetime'] if k in col_map), None)
        account_col = next((col_map[k] for k in ['account_id', 'account', 'account_number', 'emp_id'] if k in col_map), None)

        # ── 5. Compute statistics ─────────────────────────────────────
        mean_amt = df['_amount'].mean()
        std_amt  = df['_amount'].std()

        Q1  = df['_amount'].quantile(0.25)
        Q3  = df['_amount'].quantile(0.75)
        IQR = Q3 - Q1
        iqr_lower = Q1 - 1.5 * IQR
        iqr_upper = Q3 + 1.5 * IQR

        # ── 6. Flag outliers (boolean Series, aligned with reset index) ──
        # FIX: compute boolean masks directly — no .iloc[idx] lookups
        iqr_mask    = (df['_amount'] < iqr_lower) | (df['_amount'] > iqr_upper)
        zscore_mask = (np.abs((df['_amount'] - mean_amt) / (std_amt if std_amt > 0 else 1)) > 3)
        outlier_mask = iqr_mask | zscore_mask

        outlier_df = df[outlier_mask]

        # ── 7. Build anomaly objects ──────────────────────────────────
        for _, row in outlier_df.iterrows():
            amount    = float(row['_amount'])
            deviation = abs(amount - mean_amt) / (std_amt if std_amt > 0 else 1)

            if deviation > 4:
                risk = 'critical'
            elif deviation > 3:
                risk = 'high'
            elif deviation > 2:
                risk = 'medium'
            else:
                risk = 'low'

            if amount > mean_amt * 2:
                reason = f"Unusually high transaction (${amount:,.2f} vs avg ${mean_amt:,.2f})"
            elif amount < 0:
                reason = f"Negative transaction amount (${amount:,.2f})"
            else:
                reason = f"Statistical outlier (deviation: {deviation:.2f}x std)"

            anomalies.append({
                'id':         f"anom_{uuid.uuid4().hex}",   # FIX: unique IDs
                'date':       str(row[date_col]) if date_col else datetime.now().date().isoformat(),
                'amount':     amount,
                'account':    str(row[account_col]) if account_col else "N/A",
                'type':       'data_type',
                'risk':       risk,
                'risk_level': risk,
                'reason':     reason,
            })

        # ── 8. Fallback: top 5 if nothing flagged ─────────────────────
        if not anomalies:
            print(f"[FALLBACK] No statistical outliers in {file_path}, using top 5")
            for _, row in df.nlargest(5, '_amount').iterrows():
                amount = float(row['_amount'])
                anomalies.append({
                    'id':         f"anom_{uuid.uuid4().hex}",
                    'date':       str(row[date_col]) if date_col else datetime.now().date().isoformat(),
                    'amount':     amount,
                    'account':    str(row[account_col]) if account_col else "N/A",
                    'type':       'top_value',
                    'risk':       'medium',
                    'risk_level': 'medium',
                    'reason':     f"Top 5 highest transaction (${amount:,.2f})",
                })

    except Exception as e:
        print(f"[ERROR] analyze_file_for_anomalies({file_path}): {e}")

    print(f"[RESULT] {file_path}: {len(anomalies)} anomalies")
    return anomalies


# ═════════════════════════════════════════════════════════════════
# API ENDPOINTS
# ═════════════════════════════════════════════════════════════════

#################

@swagger_auto_schema(
    method='post',
    manual_parameters=[
        openapi.Parameter(
            'file',
            openapi.IN_FORM,
            description="Upload invoice image",
            type=openapi.TYPE_FILE,
            required=True
        )
    ]
)
@api_view(['POST'])
@csrf_exempt
@parser_classes([MultiPartParser, FormParser])
def upload_invoice(request):

    file = request.FILES.get("file") or request.data.get("file")

    if not file:
        return Response({"error": "No file uploaded"}, status=400)

    os.makedirs("media", exist_ok=True)

    file_path = f"media/{file.name}"

    with open(file_path, "wb+") as f:
        for chunk in file.chunks():
            f.write(chunk)

    text = extract_text_from_image(file_path)
    amount = extract_amount(text)
    mismatch = detect_price_mismatch(text)

    risk = "low"
    if amount > 50000:
        risk = "high"
    if mismatch:
        risk = "critical"

    return Response({
        "text": text[:200],
        "amount": amount,
        "risk": risk,
        "mismatch": mismatch
    })

@require_http_methods(["GET"])
def api_get_uploads(request):
    try:
        files = []
        if UPLOADS_DIR.exists():
            for file_path in sorted(UPLOADS_DIR.glob('*'), key=lambda p: p.stat().st_mtime, reverse=True):
                if not file_path.is_file():
                    continue
                stat = file_path.stat()
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

def api_upload_file(request):
    return JsonResponse({"msg": "upload working"})

@require_http_methods(["GET"])
def api_get_anomalies(request):
    try:
        if not ANOMALIES_FILE.exists():
            print("❌ anomalies.json not found")
            return JsonResponse({'anomalies': []})

        with open(ANOMALIES_FILE, 'r') as f:
            anomalies = json.load(f)

        print("📤 Returning anomalies:", len(anomalies))

        return JsonResponse({'anomalies': anomalies})

    except Exception as e:
        print("ERROR reading anomalies:", e)
        return JsonResponse({'error': str(e)}, status=500)


@require_http_methods(["GET"])
def api_anomaly_stats(request):
    try:
        anomalies = load_anomalies()
        stats = {
            'total':    len(anomalies),
            'critical': len([a for a in anomalies if a.get('risk_level') == 'critical']),
            'high':     len([a for a in anomalies if a.get('risk_level') == 'high']),
            'medium':   len([a for a in anomalies if a.get('risk_level') == 'medium']),
            'low':      len([a for a in anomalies if a.get('risk_level') == 'low']),
        }
        return JsonResponse(stats)
    except Exception as e:
        return JsonResponse({'error': str(e)}, status=500)


@require_http_methods(["POST"])
@csrf_exempt
def api_detect_anomalies(request):
    try:
        # 📂 Get uploaded files
        files = list(UPLOADS_DIR.glob('*'))

        if not files:
            return JsonResponse({'error': 'No uploaded files found'}, status=400)

        # ✅ Use ONLY latest uploaded file
        latest_file = max(files, key=lambda f: f.stat().st_mtime)

        print("📄 Processing file:", latest_file)

        # 🔍 Run detection
        all_anomalies = analyze_file_for_anomalies(latest_file)

        print("🚨 Detected anomalies:", len(all_anomalies))

        # 💾 Save anomalies
        save_anomalies(all_anomalies)

        # 📝 Log event
        log_audit_event(
            'detection',
            'Anomaly Detection Completed',
            f'Analyzed file: {latest_file.name}',
            {
                'anomalies_detected': len(all_anomalies),
                'critical': len([a for a in all_anomalies if a.get('risk_level') == 'critical']),
                'high':     len([a for a in all_anomalies if a.get('risk_level') == 'high']),
                'medium':   len([a for a in all_anomalies if a.get('risk_level') == 'medium']),
                'count':    len(all_anomalies)
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
            f'Error: {str(e)}',
            {'error': str(e)}
        )
        return JsonResponse({'error': str(e)}, status=500)


@require_http_methods(["GET"])
def api_audit_history(request):
    try:
        history = load_audit_history()
        return JsonResponse({'events': history})
    except Exception as e:
        return JsonResponse({'error': str(e)}, status=500)


@require_http_methods(["GET"])
def api_audit_stats(request):
    try:
        history = load_audit_history()
        stats = {
            'total':      len(history),
            'uploads':    len([e for e in history if e.get('event_type') == 'upload']),
            'detections': len([e for e in history if e.get('event_type') == 'detection']),
            'exports':    len([e for e in history if e.get('event_type') == 'export']),
        }
        return JsonResponse(stats)
    except Exception as e:
        return JsonResponse({'error': str(e)}, status=500)