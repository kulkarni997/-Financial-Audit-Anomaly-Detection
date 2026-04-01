import os
import io
import json
import logging
import tempfile
import pandas as pd
from pathlib import Path
from datetime import datetime
from io import BytesIO
from dotenv import load_dotenv
import numpy as np


# audit/views.py

import joblib
from django.shortcuts import render
from .forms import ProjectAuditForm

# --- 2. ADVANCED PDF GENERATION ---
from django.db.models import Avg, Count, Q

# Load the environment variables from .env
load_dotenv()

# 1. MATPLOTLIB CONFIGURATION (Must be in this order)



# 2. DJANGO & REST FRAMEWORK
from django.shortcuts import render, redirect
from django.http import JsonResponse, FileResponse
from django.utils.safestring import mark_safe
from rest_framework.decorators import api_view
from rest_framework.response import Response

# --- 3. AUDIT VIEWS ---
from .models import Anomaly

# 3. REPORTLAB (PDF GENERATION)
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_CENTER, TA_LEFT
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, 
    TableStyle, PageBreak, Image
)

# 4. EXTERNAL LIBS
import qrcode
from google.genai import Client   # ✅ new import

# 5. LOCAL ML BUSINESS LOGIC
from fraud_detection.emp_fraud_predictor import process_employee_audit
from fraud_detection.dept_fraud_predictor import process_department_audit
from fraud_detection.goods_fraud_predictor import process_goods_audit

# --- CONFIGURATION ---
logger = logging.getLogger(__name__)
api_key = os.environ.get("GEMINI_API_KEY")

if api_key:
    client = Client(api_key=api_key)
else:
    client = None

def generate_all_summaries(results: dict) -> dict:
    """
    Final optimized AI summary generator for 2026.
    Uses google-genai Client instead of deprecated configure().
    """

    if not client:
        return {
            "employee_summary": "AI summary unavailable (missing API key).",
            "department_summary": "AI summary unavailable (missing API key).",
            "goods_summary": "AI summary unavailable (missing API key)."
        }

    models_to_try = [
        "models/gemini-2.5-flash", 
        "models/gemini-2.5-pro-latest"
    ]

    emp_count = len(results.get('employee', []))
    dept_count = len(results.get('department', []))
    goods_count = len(results.get('goods', []))

    prompt = (
        "You are a Senior Auditor. Analyze these anomaly counts and provide "
        "a professional, informative narrative summary (3-4 sentences each) regarding "
        "potential financial risk and recommended audit actions.\n\n"
        f"Employee Anomalies: {emp_count}\n"
        f"Department Anomalies: {dept_count}\n"
        f"Goods Anomalies: {goods_count}\n\n"
        "Return ONLY a valid JSON object with these exact keys: "
        "'employee_summary', 'department_summary', 'goods_summary'."
    )

    for model_name in models_to_try:
        try:
            response = client.models.generate_content(
                model=model_name,
                contents=prompt,
                config={"response_mime_type": "application/json"}  # ✅ updated config
            )

            if response and response.text:
                clean_json = response.text.strip().removeprefix("```json").removesuffix("```").strip()
                parsed = json.loads(clean_json)

                return {
                    "employee_summary": parsed.get("employee_summary", "Detailed report pending."),
                    "department_summary": parsed.get("department_summary", "Detailed report pending."),
                    "goods_summary": parsed.get("goods_summary", "Detailed report pending.")
                }

        except Exception as e:
            logger.warning(f"Model {model_name} failed or returned invalid JSON: {e}")

    # Fallback summaries
    return {
        "employee_summary": (
            f"Employee Alert: {emp_count} employee-level anomalies detected. "
            "This volume suggests a risk of internal control circumvention. "
            "Immediate cross-referencing of high-risk user IDs is advised."
        ),
        "department_summary": (
            f"Departmental Review: {dept_count} irregularities identified. "
            "Patterns suggest potential budget manipulation. "
            "Next step: Conduct a deep-dive audit into outlier departments."
        ),
        "goods_summary": (
            f"Inventory Risk: {goods_count} goods anomalies flagged. "
            "Discrepancies point toward inventory shrinkage or billing errors. "
            "Recommendation: Reconcile physical inventory counts with digital ledgers."
        )
    }

def dashboard(request):
    """Simple view to render the main dashboard page."""
    return render(request, "dashboard.html")



def dashboard(request):
    anomalies = Anomaly.objects.all()
    emp_list = anomalies.filter(category="employee")

    stats = {
        "total_transactions": anomalies.count(),
        "flagged_count": emp_list.filter(score__lt=0).count(),
        "critical_count": emp_list.filter(score__lt=-0.1).count(),
        "avg_risk_score": round(emp_list.aggregate(Avg("score"))["score__avg"] or 0, 2),
        "low": emp_list.filter(score__gte=0).count(),
        "medium": emp_list.filter(score__gte=-0.05, score__lt=0).count(),
        "high": emp_list.filter(score__gte=-0.1, score__lt=-0.05).count(),
        "critical": emp_list.filter(score__lt=-0.1).count(),
    }

    context = {
        **stats,
        "vendor_list": anomalies.filter(category="department")[:5],
        "feed_list": anomalies.filter(category="goods").order_by("-created_at")[:10],
        # Chart.js arrays
        "trend_labels": mark_safe(json.dumps([a.created_at.strftime("%d-%b") for a in emp_list])),
        "trend_scores": mark_safe(json.dumps([a.score for a in emp_list])),
    }
    return render(request, "dashboard.html", context)


def upload_zip(request):
    if request.method == "POST" and request.FILES.getlist("files"):
        uploaded_files = request.FILES.getlist("files")
        
        for file in uploaded_files:
            try:
                with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as temp:
                    for chunk in file.chunks():
                        temp.write(chunk)
                    temp_path = temp.name

                file_name = file.name.lower()
                if "employee" in file_name:
                    res = process_employee_audit(temp_path, pd.DataFrame())
                    for r in res.to_dict("records"):
                        Anomaly.objects.create(
                            category="employee",
                            label=r.get("emp_id_original"),
                            score=float(r.get("risk_score", 0))
                        )
                elif "department" in file_name:
                    res = process_department_audit(temp_path)
                    for r in res.to_dict("records"):
                        Anomaly.objects.create(
                            category="department",
                            label=r.get("department_original"),
                            score=float(r.get("anomaly_score", 0))
                        )
                elif "goods" in file_name:
                    res = process_goods_audit(temp_path)
                    for r in res.to_dict("records"):
                        Anomaly.objects.create(
                            category="goods",
                            label=r.get("product_name"),
                            score=float(r.get("raw_score", 0))
                        )

                os.unlink(temp_path)
            except Exception as e:
                logger.error(f"File processing error: {file.name} - {e}")

        return redirect("dashboard")

    return render(request, "upload.html")


def anomalies(request):
    """Dashboard view providing both tables and Chart.js graphs."""
    results = request.session.get("results", {"employee": [], "department": [], "goods": []})

    emp_list = results.get("employee", [])
    dept_list = results.get("department", [])
    goods_list = results.get("goods", [])

    stats = {
        "total": len(emp_list) + len(dept_list) + len(goods_list),
        "critical": sum(1 for r in emp_list if float(r.get("risk_score", 0)) < -0.1),
        "high": sum(1 for r in emp_list if -0.1 <= float(r.get("risk_score", 0)) < -0.05),
        "medium": sum(1 for r in emp_list if -0.05 <= float(r.get("risk_score", 0)) < 0),
        "low": sum(1 for r in emp_list if float(r.get("risk_score", 0)) >= 0),
    }

    context = {
        **stats,
        "results": results,
        "employee_labels": mark_safe(json.dumps([r.get("emp_id_original") for r in emp_list])),
        "employee_scores": mark_safe(json.dumps([float(r.get("risk_score", 0)) for r in emp_list])),
        "department_labels": mark_safe(json.dumps([r.get("department_original") for r in dept_list])),
        "department_scores": mark_safe(json.dumps([float(r.get("anomaly_score", 0)) for r in dept_list])),
        "goods_labels": mark_safe(json.dumps([r.get("product_name") for r in goods_list])),
        "goods_scores": mark_safe(json.dumps([float(r.get("raw_score", 0)) for r in goods_list])),
    }

    # ✅ Instead of staying on anomalies, redirect back to dashboard after showing once
    if request.GET.get("redirect") == "true":
        return redirect("dashboard")

    return render(request, "anomalies.html", context)


def api_get_uploads(request):
    """Fetches a list of previously uploaded files from the media directory."""
    try:
        files = []
        # Adjust this path if your uploads are stored elsewhere
        uploads_dir = Path("media/uploads") 
        if uploads_dir.exists():
            for file_path in sorted(uploads_dir.glob("*"), key=lambda p: p.stat().st_mtime, reverse=True):
                if not file_path.is_file(): continue
                stat = file_path.stat()
                files.append({
                    "name": file_path.name,
                    "size": stat.st_size,
                    "uploaded_at": pd.Timestamp.fromtimestamp(stat.st_mtime).isoformat(),
                })
        return JsonResponse({"files": files})
    except Exception as e:
        logger.error(f"API Uploads Error: {e}")
        return JsonResponse({"error": str(e)}, status=500)

@api_view(["GET"])
def dashboard_summary(request):
    """Provides summary stats for the dashboard cards and trend charts."""
    # In a real scenario, you'd calculate these from your DB or session
    data = {
        "total_transactions": 1000,
        "flagged_count": 120,
        "critical_count": 10,
        "avg_risk_score": 35,
    }
    return Response(data)

def settings_view(request):
    return render(request, 'settings.html')

# --- HELPER: GENERATE MATPLOTLIB GRAPHS ---
def generate_category_graph(data, labels, title, chart_type="bar"):
    import matplotlib.pyplot as plt
    import matplotlib 
    matplotlib.use('Agg')  # Required for Django/Server environments

    # Clear any previous plots to prevent data bleeding between requests
    plt.clf() 
    
    plt.figure(figsize=(6, 3))
    plt.title(title, fontsize=10, fontweight='bold', color='#1A237E')
    
    # Ensure data isn't empty to avoid plotting errors
    if not data:
        plt.text(0.5, 0.5, "No Data Available", ha='center')
    else:
        if chart_type == "bar":
            # Normalize scores for the colormap (0-100)
            norm_data = [x / 100.0 for x in data]
            colors_list = plt.cm.get_cmap('RdYlGn_r')(norm_data) 
            plt.barh(labels[:10], data[:10], color=colors_list)
            plt.xlabel("Risk Score")
            
        elif chart_type == "pie":
            plt.pie(data, labels=labels, autopct='%1.1f%%', startangle=140)
            
        elif chart_type == "line":
            plt.plot(labels, data, marker='o', linestyle='-', color='#FF6D00')
            plt.fill_between(labels, data, color='#FFE0B2', alpha=0.3)
            plt.xticks(rotation=45)

    plt.tight_layout()
    
    img_buffer = io.BytesIO()
    plt.savefig(img_buffer, format='png', dpi=150)
    img_buffer.seek(0)
    plt.close('all') # Essential: Free up memory
    return img_buffer

def draw_page_border(canvas, doc):
    canvas.saveState()
    canvas.setStrokeColor(colors.HexColor("#1A237E"))
    canvas.setLineWidth(2)
    canvas.rect(20, 20, A4[0]-40, A4[1]-40)
    canvas.restoreState()

# --- MAIN PDF GENERATOR (Updated with correct Goods keys) ---
def generate_pdf_report(results, summaries):
    import matplotlib.pyplot as plt
    import matplotlib 
    matplotlib.use('Agg') 
    os.makedirs("media", exist_ok=True)
    pdf_path = "media/audit_report.pdf"
    doc = SimpleDocTemplate(pdf_path, pagesize=A4, rightMargin=45, leftMargin=45, topMargin=55, bottomMargin=45)
    styles = getSampleStyleSheet()

    # Custom Styles
    typewriter = ParagraphStyle('Type', fontName='Courier', fontSize=9, leading=12)
    title_style = ParagraphStyle('Title', fontName='Helvetica-Bold', fontSize=24, textColor=colors.HexColor("#1A237E"), alignment=TA_CENTER)
    section_style = ParagraphStyle('Section', fontName='Helvetica-Bold', fontSize=14, textColor=colors.HexColor("#1A237E"), spaceAfter=10)
    alert_box = ParagraphStyle('Alert', fontName='Courier-Bold', fontSize=11, textColor=colors.white, backColor=colors.HexColor("#1A237E"), borderPadding=6, alignment=TA_CENTER)

    elements = []

    # --- COVER PAGE ---
    elements.append(Spacer(1, 30))
    elements.append(Paragraph("AuditAI SYSTEM REPORT", title_style))
    elements.append(Spacer(1, 20))
    
    meta_data = [
        ["REPORT DATE", datetime.now().strftime("%d-%b-%Y")],
        ["VERSION", "v2.1.0-STABLE"],
        ["ANOMALIES DETECTED", str(sum(len(v) for v in results.values()))],
        ["SECURITY", "ENCRYPTED_INTERNAL"]
    ]
    t = Table(meta_data, colWidths=[150, 250])
    t.setStyle(TableStyle([('FONTNAME', (0,0), (-1,-1), 'Courier-Bold'), ('GRID', (0,0), (-1,-1), 0.5, colors.grey)]))
    elements.append(t)
    elements.append(Spacer(1, 30))
    elements.append(Paragraph("EXECUTIVE SUMMARY", alert_box))
    elements.append(Spacer(1, 10))
    elements.append(Paragraph(summaries.get('global', 'Analysis complete.'), typewriter))

    qr = qrcode.make(f"Audit-Verify-{datetime.now().timestamp()}")
    qr_buf = BytesIO(); qr.save(qr_buf, format="PNG"); qr_buf.seek(0)
    elements.append(Spacer(1, 20))
    elements.append(Image(qr_buf, width=100, height=100, hAlign='CENTER'))
    elements.append(PageBreak())

    # --- DYNAMIC CATEGORY LOGIC ---
    categories = [
        ('employee', 'Employee Risk Analysis', 'bar', ['ID', 'Risk Score']),
        ('department', 'Departmental Distribution', 'pie', ['Dept Name', 'Anomaly Count']),
        ('goods', 'Goods & Procurement Audit', 'line', ['Product', 'Deviation']) # Updated Header
    ]

    for key, title, chart_type, table_headers in categories:
        data_list = results.get(key, [])
        if not data_list: continue

        elements.append(Paragraph(title.upper(), section_style))
        
        table_data = [table_headers]
        graph_labels = []
        graph_values = []

        for item in data_list[:8]: 
            if key == 'employee':
                label = str(item.get('emp_id_original'))
                val = item.get('risk_score', 0)
                row = [label, f"{val}%"]
            elif key == 'department':
                label = str(item.get('department_original'))
                val = 1 
                row = [label, "Detected"]
            else:
                # UPDATED: Using product_name and raw_score for Goods
                label = str(item.get('product_name', 'N/A'))
                val = item.get('raw_score', 0)
                row = [label, str(val)]
            
            graph_labels.append(label)
            graph_values.append(float(val))
            table_data.append(row)

        # Generate Graph
        chart_buf = generate_category_graph(graph_values, graph_labels, f"{title} Visualization", chart_type)
        elements.append(Image(chart_buf, width=400, height=200))
        elements.append(Spacer(1, 15))

        # Add Data Table
        t = Table(table_data, colWidths=[200, 200])
        t.setStyle(TableStyle([
            ('BACKGROUND', (0,0), (-1,0), colors.HexColor("#EEEEEE")),
            ('GRID', (0,0), (-1,-1), 0.25, colors.grey),
            ('FONTNAME', (0,0), (-1,-1), 'Courier'),
        ]))
        elements.append(t)
        elements.append(Spacer(1, 15))
        elements.append(Paragraph(f"<b>Summary:</b> {summaries.get(f'{key}_summary', 'N/A')}", typewriter))
        elements.append(PageBreak())

    doc.build(elements, onFirstPage=draw_page_border, onLaterPages=draw_page_border)
    return pdf_path

# --- VIEW TO SHOW REPORT (Updated with Goods JSON for HTML) ---
def show_report(request):
    results = request.session.get("results", {})
    summaries = generate_all_summaries(results)

    # Re-generate the PDF file so it's fresh for download
    generate_pdf_report(results, summaries)

    context = {
        "results": results,
        "employee_summary": summaries.get("employee_summary"),
        "department_summary": summaries.get("department_summary"),
        "goods_summary": summaries.get("goods_summary"),
        
        # JSON for Employee Chart
        "employee_json": mark_safe(json.dumps([r.get("emp_id_original") for r in results.get("employee", [])])),
        "emp_scores_json": mark_safe(json.dumps([float(r.get("risk_score", 0)) for r in results.get("employee", [])])),
        
        # JSON for Department Chart
        "dept_json": mark_safe(json.dumps([r.get("department_original") for r in results.get("department", [])])),
        "dept_scores_json": mark_safe(json.dumps([float(r.get("anomaly_score", 0)) for r in results.get("department", [])])),
        
        # UPDATED: JSON for Goods Chart (Matches your HTML)
        "goods_json": mark_safe(json.dumps([r.get("product_name") for r in results.get("goods", [])])),
        "goods_scores_json": mark_safe(json.dumps([float(r.get("raw_score", 0)) for r in results.get("goods", [])])),
    }
    return render(request, "audit_report.html", context)


# --- DOWNLOAD REPORT ---
def download_report(request):
    path = "media/audit_report.pdf"
    if os.path.exists(path):
        return FileResponse(open(path, 'rb'), as_attachment=True, filename='Audit.pdf')
    return JsonResponse({"error": "Report not found. Please run analysis first."}, status=404)


def pro_dashboard(request):
    return render(request, "pro_dashboard.html")

def reimbursement_audit(request):
    return render(request, "reimbursement.html")

def approval_system(request):
     return render(request, "dashboard.html", context)


"""
Project Financial Audit — Advanced AI-Powered View Functions
============================================================
Drop-in replacements for project_audit() and download_full_project_audit_pdf().

Dependencies (add to requirements.txt):
    pandas, numpy, scikit-learn, reportlab, matplotlib, google-generativeai

Environment variables:
    GEMINI_API_KEY  — required only for the PDF narrative generation
"""

import io
import json
import logging
import traceback
from datetime import datetime

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from django.http import FileResponse
from django.shortcuts import render

# ReportLab
from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import inch, mm
from reportlab.platypus import (
    HRFlowable,
    Image,
    PageBreak,
    Paragraph,
    SimpleDocTemplate,
    Spacer,
    Table,
    TableStyle,
)

logger = logging.getLogger(__name__)

# ── Colour palette (hex) ──────────────────────────────────────────────────────
RISK_COLORS = {
    "HIGH RISK":   "#E24B4A",
    "MEDIUM RISK": "#EF9F27",
    "LOW RISK":    "#1D9E75",
}

FLEX_MAP = {"Strict": 0.05, "Moderate": 0.10, "Flexible": 0.20}

def _safe_num(series: pd.Series, fill=0) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").fillna(fill)

# ═════════════════════════════════════════════════════════════════════════════
#  HELPER – build chart PNG bytes (matplotlib)
# ═════════════════════════════════════════════════════════════════════════════
def _chart_png(fig) -> io.BytesIO:
    buf = io.BytesIO()
    fig.savefig(buf, format="PNG", dpi=150, bbox_inches="tight",
                facecolor="#FAFAF8", edgecolor="none")
    plt.close(fig)
    buf.seek(0)
    return buf

# ═════════════════════════════════════════════════════════════════════════════
#  HELPER – risk scoring (extracted so it is testable independently)
# ═════════════════════════════════════════════════════════════════════════════
def _compute_risk_score(
    row: pd.Series,
    *,
    dynamic_limit: float,
    peer_avg: pd.Series,
    total_planned: float,
    budget_gap_q90: float,
    has_ai: bool,
) -> str:
    score = 0

    # Global budget breach (whole portfolio over limit)
    if row.get("global_budget_breach", False):
        score += 4

    # Row-level overrun vs adaptive 75th-percentile threshold
    if row["overrun_ratio"] > dynamic_limit:
        score += 3

    # Peer-group anomaly (department × service_type cohort)
    peer = peer_avg.get(row.name, 0)
    if row["overrun_ratio"] > peer * 1.30:
        score += 2

    # Extreme overspend (>2× budget)
    if row["actual_spend"] > row["planned_budget"] * 2:
        score += 3

    # Concentration risk (single row > 25 % of whole portfolio)
    if total_planned > 0 and row["actual_spend"] > total_planned * 0.25:
        score += 3

    # Temporal spike (1.5× monthly average)
    if row.get("temporal_spike", False):
        score += 2

    # AI isolation-forest signal
    if has_ai and row.get("is_anomaly_raw") == -1:
        score += 2

    # High absolute impact
    if row.get("impact_score", 0) > 2:
        score += 2

    # Severe budget gap (top-decile gap)
    if row["budget_gap"] > budget_gap_q90:
        score += 2

    if score >= 10:
        return "HIGH RISK"
    elif score >= 5:
        return "MEDIUM RISK"
    return "LOW RISK"

# ═════════════════════════════════════════════════════════════════════════════
#  MAIN VIEW
# ═════════════════════════════════════════════════════════════════════════════
# Load model once at startup
import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

MODEL_PATH = os.path.join(
    BASE_DIR,
    'audit',
    'ml_assets',
    'project_Models',
    'advanced_audit_pipeline.pkl'
)
isolation_model = joblib.load(MODEL_PATH)


def project_audit(request):
    import matplotlib.pyplot as plt
    import matplotlib 
    matplotlib.use('Agg') 
    """
    POST  – accepts ProjectAuditForm + CSV upload → runs AI anomaly detection
            and risk scoring → stores summary in session → renders results page.
    GET   – renders the upload / configuration form.
    """
    from .forms import ProjectAuditForm

    if request.method != "POST":
        return render(request, "project_upload.html", {"form": ProjectAuditForm()})

    form = ProjectAuditForm(request.POST, request.FILES)
    if not form.is_valid():
        return render(request, "project_upload.html", {"form": form})

    # ── 1. LOAD CSV ───────────────────────────────────────────────
    user_input = {k: v for k, v in form.cleaned_data.items() if k != "file"}  # remove file
    try:
        df = pd.read_csv(request.FILES["file"])
    except Exception as exc:
        form.add_error("file", f"Could not parse CSV: {exc}")
        return render(request, "project_upload.html", {"form": form})

    df.columns = df.columns.str.lower().str.strip()

    # ── 2. DATE ─────────────────────────────────────────────────
    df["transaction_date"] = pd.to_datetime(df.get("transaction_date", pd.Series(dtype=str)), dayfirst=True, errors="coerce")
    df["month"] = df["transaction_date"].dt.month.fillna(0).astype(int)
    df["year"]  = df["transaction_date"].dt.year.fillna(0).astype(int)
    df["week"]  = df["transaction_date"].dt.isocalendar().week.fillna(0).astype(int)

    # ── 3. NUMERIC ──────────────────────────────────────────────
    df["actual_spend"]   = _safe_num(df.get("actual_spend", pd.Series(dtype=str)))
    df["planned_budget"] = _safe_num(df.get("planned_budget", pd.Series(dtype=str)), fill=np.nan)

    # ── 4. USER BUDGET CONTROL ─────────────────────────────────
    total_planned  = float(user_input.get("total_planned_budget", df["planned_budget"].sum()))
    flexibility    = FLEX_MAP.get(user_input.get("budget_flexibility", "Moderate"), 0.10)
    total_actual   = df["actual_spend"].sum()
    allowed_limit  = total_planned * (1 + flexibility)
    df["global_budget_breach"] = total_actual > allowed_limit

    # Fill missing planned_budget
    if df["planned_budget"].isna().all():
        df["planned_budget"] = total_planned / max(len(df), 1)
    df["planned_budget"] = df["planned_budget"].fillna(total_planned / max(len(df), 1))
    pb_sum = df["planned_budget"].sum()
    if pb_sum > 0:
        df["planned_budget"] *= total_planned / pb_sum

    # ── 5. FEATURE ENGINEERING ─────────────────────────────────
    df["overrun_ratio"] = df["actual_spend"] / df["planned_budget"].clip(lower=1e-9)
    df["budget_gap"]    = df["actual_spend"] - df["planned_budget"]
    df["log_actual"]    = np.log1p(df["actual_spend"].clip(lower=0))

    weekly_spend = df.groupby("week")["actual_spend"].transform("sum")
    weekly_prev  = weekly_spend.shift(1).fillna(0)
    df["velocity"] = (weekly_spend - weekly_prev).clip(lower=0)

    # ── 6. CATEGORICAL DEFAULTS ───────────────────────────────
    CAT_COLS = ["project_type", "department", "service_type", "vendor"]
    for col in CAT_COLS:
        if col not in df.columns:
            df[col] = "Unknown"
        df[col] = df[col].astype(str).str.strip().replace("", "Unknown").fillna("Unknown")

    # ── 7. AI PREDICTION ───────────────────────────────────────
    MODEL_FEATURES = ["planned_budget", "actual_spend", "overrun_ratio", "budget_gap", "log_actual", "month", "velocity"]
    X = df[MODEL_FEATURES + CAT_COLS].copy().fillna(0).replace([np.inf, -np.inf], 0)

    try:
        df["is_anomaly_raw"] = isolation_model.predict(X)
        df["anomaly_score"]  = -isolation_model.decision_function(X)
        has_ai = True
    except Exception:
        logger.warning("IsolationForest prediction failed:\n%s", traceback.format_exc())
        df["is_anomaly_raw"] = 1
        df["anomaly_score"]  = 0.0
        has_ai = False

    # Normalise anomaly score 0-100
    a_min, a_max = df["anomaly_score"].min(), df["anomaly_score"].max()
    df["anomaly_score_pct"] = ((df["anomaly_score"] - a_min) / (a_max - a_min) * 100).round(1) if a_max > a_min else 0.0

    # ── 8. HIGH-RISK LOGIC (Training Rules) ───────────────────
    df["extreme_over_budget"] = df["actual_spend"] > (df["planned_budget"] * 1.5)
    df["top_5_percent_spend"] = df["actual_spend"] > df["actual_spend"].quantile(0.95)
    df["final_flag"] = np.where(
        (df["is_anomaly_raw"] == -1) & (df["extreme_over_budget"] | df["top_5_percent_spend"]),
        "High Risk",
        "Normal"
    )

    # ── 9. RISK ENGINE ───────────────────────────────────────
    dynamic_limit = float(df["overrun_ratio"].quantile(0.75))
    peer_avg      = df.groupby(["department", "service_type"])["overrun_ratio"].transform("mean").fillna(0)
    monthly_avg   = df.groupby("month")["actual_spend"].transform("mean").fillna(0)
    df["temporal_spike"] = df["actual_spend"] > (monthly_avg * 1.5)
    df["impact_score"]   = df["actual_spend"] / max(df["actual_spend"].mean(), 1e-9)
    budget_gap_q90       = float(df["budget_gap"].quantile(0.90))

    df["risk_level"] = df.apply(
        _compute_risk_score,
        axis=1,
        dynamic_limit=dynamic_limit,
        peer_avg=peer_avg,
        total_planned=total_planned,
        budget_gap_q90=budget_gap_q90,
        has_ai=has_ai,
    )

    df["risk_confidence"] = (
        (df["overrun_ratio"].clip(0, 5) / 5 * 40) +
        (df["anomaly_score_pct"] * 0.30) +
        (df["temporal_spike"].astype(int) * 15) +
        (df["global_budget_breach"].astype(int) * 15)
    ).clip(0, 100).round(1)

    df.replace([np.inf, -np.inf], 0, inplace=True)
    df.fillna(0, inplace=True)

    hr_df = df[df["risk_level"] == "HIGH RISK"]
    mr_df = df[df["risk_level"] == "MEDIUM RISK"]

    # ── 10. GRAPH DATA ────────────────────────────────────────
    g1 = df.sort_values("transaction_date")
    g1_labels = g1["transaction_date"].dt.strftime("%d-%b").fillna("").tolist()
    g1_data   = g1["actual_spend"].round(2).tolist()
    g1_colors = [RISK_COLORS.get(r, "#7c3aed") for r in g1["risk_level"]]

    g2 = df[df["risk_level"] != "LOW RISK"].groupby("vendor")["actual_spend"].sum().sort_values(ascending=False).head(5)
    g2_labels = g2.index.tolist()
    g2_data   = g2.round(2).values.tolist()

    g3 = df.groupby("service_type")["actual_spend"].std().fillna(0).sort_values(ascending=False)
    if g3.empty: g3 = pd.Series([0], index=["No Data"])

    g4 = df.groupby("approval_status")["overrun_ratio"].mean().fillna(0)
    if g4.empty: g4 = pd.Series([0], index=["No Data"])

    g5 = df.groupby("month")["actual_spend"].sum().fillna(0)
    if g5.empty: g5 = pd.Series([0], index=["0"])

    g6 = df[["actual_spend", "anomaly_score_pct", "risk_level"]].round(2).to_dict(orient="records")

    g7 = df.groupby("department")["actual_spend"].sum().sort_values(ascending=False)
    g7_labels = g7.index.tolist()
    g7_data   = g7.round(2).values.tolist()

    risk_counts = df["risk_level"].value_counts()
    g8_labels = risk_counts.index.tolist()
    g8_data   = risk_counts.values.tolist()
    g8_colors = [RISK_COLORS.get(l, "#888") for l in g8_labels]

    # ── 11. SUMMARY ─────────────────────────────────────────
    leakage      = float(hr_df["actual_spend"].sum())
    medium_spend = float(mr_df["actual_spend"].sum())

    hr_records = hr_df.sort_values("actual_spend", ascending=False).head(200).to_dict(orient="records")

    def _clean(obj):
        if isinstance(obj, dict): return {k: _clean(v) for k, v in obj.items()}
        if isinstance(obj, list): return [_clean(i) for i in obj]
        if isinstance(obj, (np.integer,)): return int(obj)
        if isinstance(obj, (np.floating,)): return float(obj)
        if isinstance(obj, pd.Timestamp): return str(obj)
        return obj

    summary = _clean({
        "meta": user_input,
        "generated_at": datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC"),
        "total_records": len(df),
        "high_risk_count": len(hr_df),
        "medium_risk_count": len(mr_df),
        "low_risk_count": int(df["risk_level"].eq("LOW RISK").sum()),
        "total_planned": total_planned,
        "total_spent": float(total_actual),
        "budget_limit": float(allowed_limit),
        "budget_breach": bool(total_actual > allowed_limit),
        "over_budget_ratio": round(total_actual / total_planned, 4) if total_planned > 0 else 0,
        "remaining_budget": float(max(allowed_limit - total_actual, 0)),
        "leakage": leakage,
        "medium_spend": medium_spend,
        "avg_anomaly_score": float(df["anomaly_score_pct"].mean().round(1)),
        "has_ai": has_ai,
        "confidence": round(100 - (len(hr_df) / max(len(df), 1) * 100), 2),
        "all_high_risks": hr_records,
        "g1_burn_labels": json.dumps(g1_labels),
        "g1_burn_data": json.dumps(g1_data),
        "g1_colors": json.dumps(g1_colors),
        "g2_vendor_labels": json.dumps(g2_labels),
        "g2_vendor_data": json.dumps(g2_data),
        "g3_cat_labels": json.dumps(g3.index.tolist()),
        "g3_cat_data": json.dumps(g3.round(2).values.tolist()),
        "g4_app_labels": json.dumps(g4.index.tolist()),
        "g4_app_risk": json.dumps(g4.round(4).values.tolist()),
        "g5_temp_labels": json.dumps(g5.index.astype(str).tolist()),
        "g5_temp_data": json.dumps(g5.round(2).values.tolist()),
        "g6_scatter": json.dumps(g6),
        "g7_dept_labels": json.dumps(g7_labels),
        "g7_dept_data": json.dumps(g7_data),
        "g8_risk_labels": json.dumps(g8_labels),
        "g8_risk_data": json.dumps(g8_data),
        "g8_risk_colors": json.dumps(g8_colors),
        "df": json.loads(df.to_json(orient="records", date_format="iso")),
    })

    # Store session safely
    request.session["summary"] = summary
    request.session.modified = True

    return render(request, "project_results.html", {"summary": summary})
def download_full_project_audit_pdf(request):
    import matplotlib.pyplot as plt
    import matplotlib 
    matplotlib.use('Agg') 
    """
    Streams a comprehensive, multi-section audit PDF using data stored in the
    session by project_audit().  Falls back gracefully if session data is absent.

    UPDATED:
    - Typewriter / monospace font (DejaVu Sans Mono) for body text & data
    - Floating QR code on cover and every section header page
    - Richer cover page with gradient-like decorative bars
    - Section header banners with accent stripe
    - Improved metric cards, callout boxes, and finding cards
    - Footer with page number, report ID, and classification badge on every page
    """
    import io
    import json
    import logging
    import hashlib
    import traceback
    from datetime import datetime

    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import numpy as np
    import pandas as pd

    from django.http import FileResponse

    from reportlab.lib import colors
    from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT
    from reportlab.lib.pagesizes import A4
    from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
    from reportlab.lib.units import inch, mm
    from reportlab.platypus import (
        HRFlowable, Image, PageBreak, Paragraph,
        SimpleDocTemplate, Spacer, Table, TableStyle, KeepTogether,
    )
    from reportlab.pdfbase import pdfmetrics
    from reportlab.pdfbase.ttfonts import TTFont
    from reportlab.platypus.flowables import Flowable

    logger = logging.getLogger(__name__)

    # ── Fonts ────────────────────────────────────────────────────────────────
    # Use built-in fonts on Windows
    BODY_FONT      = "Courier"
    BODY_FONT_BOLD = "Courier-Bold"
    HEAD_FONT      = "Times-Bold"
    HEAD_FONT_REG  = "Times-Bold"

    # ── Constants ─────────────────────────────────────────────────────────────
    PAGE_W, PAGE_H = A4

    RISK_COLORS = {
        "HIGH RISK":   "#E24B4A",
        "MEDIUM RISK": "#EF9F27",
        "LOW RISK":    "#1D9E75",
    }

    PDF_PALETTE = [
        "#185FA5", "#1D9E75", "#E24B4A", "#EF9F27",
        "#7F77DD", "#D85A30", "#3B8BD4", "#63991A", "#A32D2D",
    ]

    FLEX_MAP = {"Strict": 0.05, "Moderate": 0.10, "Flexible": 0.20}

    # ── Colours ───────────────────────────────────────────────────────────────
    NAVY       = colors.HexColor("#0C2D5A")
    TEAL       = colors.HexColor("#085041")
    RED        = colors.HexColor("#A32D2D")
    AMBER      = colors.HexColor("#854F0B")
    LIGHT_BG   = colors.HexColor("#F4F2EC")
    MID_GREY   = colors.HexColor("#7A7872")
    DARK_GREY  = colors.HexColor("#1E1E1C")
    ACCENT     = colors.HexColor("#1A6FAE")   # section stripe accent
    CREAM      = colors.HexColor("#FDFCF8")
    STRIPE1    = colors.HexColor("#0C2D5A")
    STRIPE2    = colors.HexColor("#1A6FAE")
    STRIPE3    = colors.HexColor("#3BA8D4")

    # ── Helper: matplotlib figure → BytesIO PNG ───────────────────────────────
    def chart_png(fig, bg="#FDFCF8"):
        buf = io.BytesIO()
        fig.savefig(buf, format="PNG", dpi=150, bbox_inches="tight",
                    facecolor=bg, edgecolor="none")
        plt.close(fig)
        buf.seek(0)
        return buf

    # ── Helper: QR-code-like matrix from report ID ────────────────────────────
    def make_qr_image(data: str, px: int = 80) -> io.BytesIO:
        """
        Renders a deterministic QR-style matrix image from `data`.
        Uses matplotlib so no qrcode package is needed.
        """
        SIZE = 21
        h = hashlib.md5(data.encode()).digest()
        rng = np.random.default_rng(int.from_bytes(h[:4], "big") % (2**31))
        mat = rng.integers(0, 2, (SIZE, SIZE))

        # Stamp three finder-pattern corners
        for (r, c) in [(0, 0), (0, SIZE - 7), (SIZE - 7, 0)]:
            mat[r:r+7, c:c+7] = 0
            mat[r+1:r+6, c+1:c+6] = 1
            mat[r+2:r+5, c+2:c+5] = 0
            mat[r+3:r+4, c+3:c+4] = 1

        # Timing patterns
        for i in range(8, SIZE - 8):
            mat[6, i] = i % 2
            mat[i, 6] = i % 2

        fig, ax = plt.subplots(figsize=(1.5, 1.5), facecolor="white")
        ax.imshow(mat, cmap="binary_r", interpolation="nearest", vmin=0, vmax=1)
        ax.axis("off")
        fig.subplots_adjust(left=0, right=1, top=1, bottom=0)
        buf = io.BytesIO()
        fig.savefig(buf, format="PNG", dpi=120, bbox_inches="tight",
                    facecolor="white", edgecolor="none")
        plt.close(fig)
        buf.seek(0)
        return buf

    # ── Custom Flowable: decorative section banner ────────────────────────────
    class SectionBanner(Flowable):
        """Coloured accent stripe + section title block."""
        def __init__(self, title, subtitle="", stripe_color=None):
            super().__init__()
            self.title    = title
            self.subtitle = subtitle
            self.stripe   = stripe_color or ACCENT
            self.width    = PAGE_W - 90
            self.height   = 38

        def draw(self):
            c = self.canv
            w, h = self.width, self.height
            # Background rect
            c.setFillColor(NAVY)
            c.rect(0, 0, w, h, fill=1, stroke=0)
            # Left accent stripe
            c.setFillColor(STRIPE3)
            c.rect(0, 0, 6, h, fill=1, stroke=0)
            # Title
            c.setFillColor(colors.white)
            c.setFont(HEAD_FONT, 14)
            c.drawString(14, h - 22, self.title)
            if self.subtitle:
                c.setFont(BODY_FONT, 8)
                c.setFillColor(colors.HexColor("#AACCEE"))
                c.drawString(14, 6, self.subtitle)

    # ── Custom Flowable: floating QR panel (right-aligned) ───────────────────
    class QRPanel(Flowable):
        """Small QR image + label panel, floats right."""
        def __init__(self, qr_buf, label="Verify Report", size=60):
            super().__init__()
            self.qr_buf = qr_buf
            self.label  = label
            self.size   = size
            self.width  = size + 4
            self.height = size + 16

        def draw(self):
            from reportlab.lib.utils import ImageReader
            c = self.canv
            sz = self.size
            # Border
            c.setStrokeColor(MID_GREY)
            c.setLineWidth(0.4)
            c.rect(0, 14, sz + 4, sz + 2, fill=0)
            # QR image
            self.qr_buf.seek(0)
            c.drawImage(ImageReader(self.qr_buf), 2, 16, sz, sz)
            # Label
            c.setFont(BODY_FONT, 6)
            c.setFillColor(MID_GREY)
            c.drawCentredString((sz + 4) / 2, 4, self.label)

    # ── Custom Flowable: page footer (called via canvas callbacks) ────────────
    # We attach a footer via the onPage callback on SimpleDocTemplate.

    # ── Session guard ─────────────────────────────────────────────────────────
    summary = request.session.get("summary")
    if not summary:
        buf = io.BytesIO(b"No audit data in session. Please run the audit first.")
        return FileResponse(buf, as_attachment=True, filename="NoData.txt")

    # ── Rebuild DataFrame ─────────────────────────────────────────────────────
    df_data = summary.get("df", [])
    df = pd.DataFrame(df_data) if df_data else pd.DataFrame()

    for col in ["actual_spend", "planned_budget", "overrun_ratio",
                "budget_gap", "anomaly_score_pct", "risk_confidence"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    # ── Document metadata ─────────────────────────────────────────────────────
    company      = summary.get("meta", {}).get("company_name", "Client Organisation")
    period       = summary.get("meta", {}).get("audit_period",  "FY 2025-26")
    report_id    = f"AUD-{datetime.utcnow().strftime('%Y%m%d')}-{np.random.randint(1000, 9999)}"
    generated_at = summary.get("generated_at", datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC"))

    # Pre-generate QR image (reused across pages)
    qr_buf = make_qr_image(report_id)

    # ── Build the PDF buffer ──────────────────────────────────────────────────
    buffer = io.BytesIO()

    # Footer callback ─────────────────────────────────────────────────────────
    def _footer(canvas, doc):
        canvas.saveState()
        # Bottom bar
        canvas.setFillColor(NAVY)
        canvas.rect(0, 0, PAGE_W, 22, fill=1, stroke=0)
        # Left: classification
        canvas.setFont(BODY_FONT_BOLD, 7)
        canvas.setFillColor(colors.HexColor("#AACCEE"))
        canvas.drawString(45, 7, "CONFIDENTIAL")
        # Centre: report ID
        canvas.setFont(BODY_FONT, 7)
        canvas.setFillColor(colors.white)
        canvas.drawCentredString(PAGE_W / 2, 7, f"Report ID: {report_id}  |  {company}")
        # Right: page number
        canvas.setFont(BODY_FONT, 7)
        canvas.drawRightString(PAGE_W - 45, 7, f"Page {doc.page}")
        canvas.restoreState()

    doc = SimpleDocTemplate(
        buffer,
        pagesize=A4,
        rightMargin=45, leftMargin=45,
        topMargin=50,   bottomMargin=38,   # extra bottom for footer bar
        title=f"AI Financial Audit — {company}",
        author="AI Audit Engine v3",
        onFirstPage=_footer,
        onLaterPages=_footer,
    )

    elements = []
    styles   = getSampleStyleSheet()

    # ── Style factory (typewriter-first) ──────────────────────────────────────
    def sty(name, **kw):
        return ParagraphStyle(name, parent=styles["Normal"], **kw)

    S = {
        # Cover
        "cover_eyebrow": sty("CoverEye", fontSize=9, leading=12,
                              alignment=TA_CENTER, textColor=colors.HexColor("#AACCEE"),
                              fontName=BODY_FONT, spaceAfter=4),
        "cover_title":   sty("CoverTitle", fontSize=26, leading=32,
                              alignment=TA_CENTER, textColor=colors.white,
                              fontName=HEAD_FONT, spaceAfter=6),
        "cover_sub":     sty("CoverSub", fontSize=12, alignment=TA_CENTER,
                              textColor=colors.HexColor("#AACCEE"),
                              fontName=BODY_FONT, spaceAfter=4),
        "cover_meta":    sty("CoverMeta", fontSize=10, leading=16,
                              textColor=DARK_GREY, fontName=BODY_FONT, spaceAfter=3),
        # Body
        "h1":            sty("H1", fontSize=15, fontName=HEAD_FONT,
                              textColor=colors.white, spaceBefore=2, spaceAfter=4),
        "h2":            sty("H2", fontSize=12, fontName=HEAD_FONT,
                              textColor=NAVY, spaceBefore=12, spaceAfter=5),
        "h3":            sty("H3", fontSize=10, fontName=BODY_FONT_BOLD,
                              textColor=DARK_GREY, spaceBefore=7, spaceAfter=3),
        "body":          sty("Body", fontSize=9, leading=14, textColor=DARK_GREY,
                              fontName=BODY_FONT, spaceAfter=5),
        "body_bold":     sty("BodyBold", fontSize=9, leading=14, textColor=DARK_GREY,
                              fontName=BODY_FONT_BOLD, spaceAfter=5),
        "small":         sty("Small", fontSize=8, leading=11, textColor=MID_GREY,
                              fontName=BODY_FONT),
        "mono_data":     sty("MonoData", fontSize=8, leading=11, textColor=DARK_GREY,
                              fontName=BODY_FONT),
        "callout":       sty("Callout", fontSize=9, leading=14,
                              textColor=DARK_GREY, fontName=BODY_FONT,
                              backColor=LIGHT_BG, borderPad=8,
                              leftIndent=14, rightIndent=14,
                              spaceBefore=8, spaceAfter=8),
        "red_flag":      sty("RedFlag", fontSize=9, fontName=BODY_FONT_BOLD,
                              textColor=RED),
        "toc_entry":     sty("TOC", fontSize=10, leading=18, textColor=DARK_GREY,
                              fontName=BODY_FONT),
        "toc_num":       sty("TOCNum", fontSize=10, leading=18,
                              textColor=ACCENT, fontName=BODY_FONT_BOLD),
        "footer":        sty("Footer", fontSize=7, alignment=TA_CENTER,
                              textColor=MID_GREY, fontName=BODY_FONT),
    }

    # ── Helpers ───────────────────────────────────────────────────────────────
    def divider(color=MID_GREY, width=0.4):
        return HRFlowable(width="100%", thickness=width, color=color,
                          spaceAfter=6, spaceBefore=2)

    def section_header(num, title, subtitle=""):
        """Returns a SectionBanner + small spacer."""
        return [
            Spacer(1, 8),
            SectionBanner(f"Section {num}  —  {title}", subtitle),
            Spacer(1, 10),
        ]

    def metric_table(pairs, color=NAVY):
        """Metric card strip — monospace values, small labels."""
        col_w = (PAGE_W - 90) / max(len(pairs), 1)

        row_vals = []
        row_lbls = []
        for i, (lbl, val) in enumerate(pairs):
            row_vals.append(Paragraph(
                f'<b>{val}</b>',
                sty(f"mv_{i}", fontSize=14, fontName=BODY_FONT_BOLD,
                    textColor=color, alignment=TA_CENTER),
            ))
            row_lbls.append(Paragraph(
                lbl,
                sty(f"ml_{i}", fontSize=7, fontName=BODY_FONT,
                    textColor=MID_GREY, alignment=TA_CENTER),
            ))

        t = Table([row_vals, row_lbls], colWidths=[col_w] * len(pairs))
        t.setStyle(TableStyle([
            ("BACKGROUND",    (0, 0), (-1, -1), LIGHT_BG),
            ("TOPPADDING",    (0, 0), (-1, -1), 8),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 8),
            ("LEFTPADDING",   (0, 0), (-1, -1), 4),
            ("RIGHTPADDING",  (0, 0), (-1, -1), 4),
            ("LINEBELOW",     (0, 0), (-1, 0),  0.3, MID_GREY),
            ("GRID",          (0, 0), (-1, -1), 0.25, MID_GREY),
        ]))
        return t

    def risk_badge(level):
        hex_map = {"HIGH RISK": "A32D2D", "MEDIUM RISK": "854F0B", "LOW RISK": "0F6E56"}
        bg_map  = {"HIGH RISK": "FCEBEB", "MEDIUM RISK": "FAEEDA", "LOW RISK": "E1F5EE"}
        h = hex_map.get(str(level), "444441")
        b = bg_map.get(str(level),  "F1EFE8")
        return Paragraph(
            f'<font color="#{h}"><b>{level}</b></font>',
            sty(f"rb_{h}", fontSize=7, fontName=BODY_FONT_BOLD,
                backColor=colors.HexColor(f"#{b}"),
                borderPad=3, alignment=TA_CENTER),
        )

    def rl_image(fig, width=480, height=220):
        return Image(chart_png(fig), width=width, height=height)

    def qr_image_flowable(size=64, label="Scan to verify"):
        qr_buf.seek(0)
        buf_copy = io.BytesIO(qr_buf.read())
        return QRPanel(buf_copy, label=label, size=size)

    def dept_table(dept_df, cols_show):
        preview = dept_df.sort_values("actual_spend", ascending=False).head(20)
        col_w   = (PAGE_W - 90) / max(len(cols_show), 1)

        tdata = [[Paragraph(f"<b>{c.replace('_',' ').title()}</b>", S["small"])
                  for c in cols_show]]
        for _, row in preview.iterrows():
            trow = []
            for c in cols_show:
                v = row.get(c, "")
                if c in ("actual_spend", "planned_budget"):
                    cell = Paragraph(f"Rs.{float(v or 0):,.0f}", S["mono_data"])
                elif c == "risk_level":
                    cell = risk_badge(str(v))
                elif c == "transaction_date":
                    cell = Paragraph(str(v)[:10], S["mono_data"])
                else:
                    cell = Paragraph(str(v)[:28], S["mono_data"])
                trow.append(cell)
            tdata.append(trow)

        t = Table(tdata, colWidths=[col_w] * len(cols_show), repeatRows=1)
        t.setStyle(TableStyle([
            ("BACKGROUND",    (0, 0), (-1, 0),  NAVY),
            ("TEXTCOLOR",     (0, 0), (-1, 0),  colors.white),
            ("ROWBACKGROUNDS",(0, 1), (-1, -1), [colors.white, LIGHT_BG]),
            ("GRID",          (0, 0), (-1, -1), 0.25, MID_GREY),
            ("TOPPADDING",    (0, 0), (-1, -1), 3),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
            ("FONTSIZE",      (0, 0), (-1, -1), 7),
        ]))
        return t

    # ── Finding card ─────────────────────────────────────────────────────────
    def finding_card(code, title, body_text, active):
        accent  = RED if active else TEAL
        bg      = colors.HexColor("#FDF4F4") if active else colors.HexColor("#F0FAF5")
        badge   = "ACTION REQUIRED" if active else "MONITOR"
        badge_c = RED if active else TEAL

        inner = [
            [
                Paragraph(f'<b>[{code}]</b>', sty(f"fc_code_{code}", fontSize=8,
                           fontName=BODY_FONT_BOLD, textColor=accent)),
                Paragraph(title, sty(f"fc_t_{code}", fontSize=9,
                           fontName=HEAD_FONT, textColor=DARK_GREY)),
                Paragraph(f'<b>{badge}</b>', sty(f"fc_b_{code}", fontSize=7,
                           fontName=BODY_FONT_BOLD, textColor=badge_c,
                           alignment=TA_RIGHT)),
            ],
            [
                Paragraph("", S["small"]),
                Paragraph(body_text, sty(f"fc_body_{code}", fontSize=8,
                           fontName=BODY_FONT, leading=12,
                           textColor=DARK_GREY), ),
                Paragraph("", S["small"]),
            ],
        ]
        t = Table(inner, colWidths=[38, 360, 90])
        t.setStyle(TableStyle([
            ("BACKGROUND",    (0, 0), (-1, -1), bg),
            ("LINEAFTER",     (0, 0), (0, -1),  1.5, accent),
            ("TOPPADDING",    (0, 0), (-1, -1), 5),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
            ("LEFTPADDING",   (0, 0), (-1, -1), 6),
            ("RIGHTPADDING",  (0, 0), (-1, -1), 6),
            ("SPAN",          (1, 1), (2, 1)),
            ("VALIGN",        (0, 0), (-1, -1), "MIDDLE"),
        ]))
        return KeepTogether([t, Spacer(1, 6)])

    # =========================================================================
    #  PAGE 1 – COVER  (full-page coloured background via canvas)
    # =========================================================================
    # We fake a "dark cover" by drawing a large coloured table.
    cover_bg = Table(
        [[Paragraph(
            "INDEPENDENT AI FINANCIAL AUDIT<br/>"
            "<font size='10'>COMPREHENSIVE RISK &amp; ANOMALY REPORT</font>",
            sty("CovH", fontSize=22, fontName=HEAD_FONT, textColor=colors.white,
                leading=30, alignment=TA_CENTER),
        )]],
        colWidths=[PAGE_W - 90],
        rowHeights=[110],
    )
    cover_bg.setStyle(TableStyle([
        ("BACKGROUND",    (0, 0), (-1, -1), NAVY),
        ("TOPPADDING",    (0, 0), (-1, -1), 28),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 28),
        ("LEFTPADDING",   (0, 0), (-1, -1), 20),
        ("RIGHTPADDING",  (0, 0), (-1, -1), 20),
        ("LINEBELOW",     (0, 0), (-1, -1), 4, STRIPE3),
    ]))

    # Three accent stripes
    accent_bar = Table(
        [["", "", ""]],
        colWidths=[(PAGE_W - 90) * 0.5, (PAGE_W - 90) * 0.3, (PAGE_W - 90) * 0.2],
        rowHeights=[6],
    )
    accent_bar.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (0, 0), STRIPE1),
        ("BACKGROUND", (1, 0), (1, 0), STRIPE2),
        ("BACKGROUND", (2, 0), (2, 0), STRIPE3),
        ("TOPPADDING", (0, 0), (-1, -1), 0),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 0),
    ]))

    # Meta info table next to QR
    qr_cover = qr_image_flowable(size=72, label="Scan to verify report")
    meta_rows = [
        ["Client",         company],
        ["Audit Period",   period],
        ["Report ID",      report_id],
        ["Generated",      generated_at],
        ["Classification", "CONFIDENTIAL"],
        ["Engine",         "AI Audit Engine v3  |  Isolation Forest"],
    ]
    meta_tbl = Table(
        [[Paragraph(f"<b>{k}</b>", S["mono_data"]),
          Paragraph(v, S["mono_data"])] for k, v in meta_rows],
        colWidths=[110, 260],
    )
    meta_tbl.setStyle(TableStyle([
        ("ROWBACKGROUNDS",  (0, 0), (-1, -1), [colors.white, LIGHT_BG]),
        ("GRID",            (0, 0), (-1, -1), 0.25, MID_GREY),
        ("TOPPADDING",      (0, 0), (-1, -1), 4),
        ("BOTTOMPADDING",   (0, 0), (-1, -1), 4),
        ("LEFTPADDING",     (0, 0), (-1, -1), 6),
        ("RIGHTPADDING",    (0, 0), (-1, -1), 6),
        ("FONTNAME",        (0, 0), (0, -1),  BODY_FONT_BOLD),
        ("TEXTCOLOR",       (0, 0), (0, -1),  NAVY),
        ("TEXTCOLOR",       (4, 1), (4, 1),   RED),
    ]))

    # Side-by-side: meta table | QR
    cover_info = Table(
        [[meta_tbl, qr_cover]],
        colWidths=[375, 84],
        hAlign="LEFT",
    )
    cover_info.setStyle(TableStyle([
        ("VALIGN",  (0, 0), (-1, -1), "TOP"),
        ("LEFTPADDING",  (0, 0), (-1, -1), 0),
        ("RIGHTPADDING", (0, 0), (-1, -1), 0),
        ("TOPPADDING",   (0, 0), (-1, -1), 0),
        ("BOTTOMPADDING",(0, 0), (-1, -1), 0),
    ]))

    elements += [
        Spacer(1, 0.5 * inch),
        cover_bg,
        accent_bar,
        Spacer(1, 0.35 * inch),
        cover_info,
        Spacer(1, 0.3 * inch),
        divider(MID_GREY),
        Paragraph(
            "This report is produced by an AI-assisted audit engine using Isolation "
            "Forest anomaly detection, statistical risk scoring, and peer-cohort "
            "benchmarking. It must be reviewed alongside manual human oversight "
            "before enforcement action is taken.",
            S["small"],
        ),
        PageBreak(),
    ]

    # =========================================================================
    #  PAGE 2 – TABLE OF CONTENTS
    # =========================================================================
    toc_sections = [
        ("1", "Executive Summary",              "Key metrics, AI narrative, portfolio health"),
        ("2", "Audit Scope and Methodology",     "Data sources, detection techniques"),
        ("3", "Portfolio-Level Budget Analysis", "Planned vs actual, breach status"),
        ("4", "AI Anomaly Detection Results",    "Isolation forest output, burn rate chart"),
        ("5", "Departmental Deep-Dive",          "Per-department spend, service breakdown"),
        ("6", "High-Risk Transaction Logs",      "Paginated HIGH RISK transaction records"),
        ("7", "Trend and Visualisation Analysis","Vendor, monthly, and department charts"),
        ("8", "Findings and Recommendations",    "Actionable findings, remediation steps"),
        ("9", "Auditor Conclusion",              "Overall risk classification, sign-off"),
        ("A", "Appendix and Legal Disclaimer",   "Glossary, disclaimers"),
    ]

    elements += section_header("", "Table of Contents")

    toc_data = []
    for num, title, desc in toc_sections:
        toc_data.append([
            Paragraph(f"<b>{num}</b>",  S["toc_num"]),
            Paragraph(f"<b>{title}</b><br/>"
                      f'<font color="#7A7872" size="8">{desc}</font>', S["toc_entry"]),
            Paragraph("· · ·", sty("dots", fontSize=9, alignment=TA_RIGHT,
                                   textColor=MID_GREY, fontName=BODY_FONT)),
        ])

    toc_tbl = Table(toc_data, colWidths=[24, 370, 60])
    toc_tbl.setStyle(TableStyle([
        ("ROWBACKGROUNDS", (0, 0), (-1, -1), [colors.white, LIGHT_BG]),
        ("TOPPADDING",     (0, 0), (-1, -1), 6),
        ("BOTTOMPADDING",  (0, 0), (-1, -1), 6),
        ("LEFTPADDING",    (0, 0), (-1, -1), 6),
        ("RIGHTPADDING",   (0, 0), (-1, -1), 6),
        ("LINEBELOW",      (0, -1), (-1, -1), 0.5, MID_GREY),
        ("VALIGN",         (0, 0), (-1, -1), "MIDDLE"),
    ]))
    elements.append(toc_tbl)
    elements.append(PageBreak())

    # =========================================================================
    #  SECTION 1 – EXECUTIVE SUMMARY
    # =========================================================================
    total_spent   = float(summary.get("total_spent",   0) or 0)
    total_planned = float(summary.get("total_planned", 0) or 0)
    leakage       = float(summary.get("leakage",       0) or 0)
    hr_count      = int(summary.get("high_risk_count", 0) or 0)
    mr_count      = int(summary.get("medium_risk_count", 0) or 0)
    breach        = bool(summary.get("budget_breach",  False))
    ratio         = float(summary.get("over_budget_ratio", 0) or 0)
    confidence    = float(summary.get("confidence",    0) or 0)
    avg_anomaly   = float(summary.get("avg_anomaly_score", 0) or 0)
    budget_limit  = float(summary.get("budget_limit",  0) or 0)
    remaining     = float(summary.get("remaining_budget", 0) or 0)
    medium_spend  = float(summary.get("medium_spend",  0) or 0)

    elements += section_header("1", "Executive Summary",
                               f"{company}  |  {period}")

    elements.append(metric_table([
        ("Total Transactions", str(summary.get("total_records", 0))),
        ("High-Risk Flags",    str(hr_count)),
        ("Medium-Risk Flags",  str(mr_count)),
        ("Audit Confidence",   f"{confidence:.1f}%"),
    ]))
    elements.append(Spacer(1, 8))
    elements.append(metric_table([
        ("Total Planned",     f"Rs.{total_planned:,.0f}"),
        ("Total Actual",      f"Rs.{total_spent:,.0f}"),
        ("Flagged Leakage",   f"Rs.{leakage:,.0f}"),
        ("Over-Budget Ratio", f"{ratio:.2f}x"),
    ], color=RED if breach else TEAL))
    elements.append(Spacer(1, 10))

    # Breach callout
    if breach:
        elements.append(Paragraph(
            f"  *** BUDGET BREACH DETECTED — Actual spend exceeds approved "
            f"limit by a factor of {ratio:.2f}x. Immediate action required. ***",
            sty("BreachWarn", fontSize=9, fontName=BODY_FONT_BOLD,
                textColor=RED, backColor=colors.HexColor("#FCEBEB"),
                borderPad=8, leftIndent=10, spaceBefore=4, spaceAfter=8),
        ))

    # AI narrative
    try:
        import os
        import google.generativeai as genai
        genai.configure(api_key=os.environ["GEMINI_API_KEY"])
        gem = genai.GenerativeModel("gemini-2.0-flash")
        prompt = (
            f"You are a senior financial auditor. Write a 400-word professional "
            f"executive summary for {company} covering these metrics:\n"
            f"- Audit period: {period}\n"
            f"- Total planned budget: Rs.{total_planned:,.0f}\n"
            f"- Total actual spend: Rs.{total_spent:,.0f}\n"
            f"- High-risk transactions: {hr_count} (leakage Rs.{leakage:,.0f})\n"
            f"- Medium-risk transactions: {mr_count}\n"
            f"- Budget breach: {'YES' if breach else 'NO'}\n"
            f"- Overrun ratio: {ratio:.2f}x\n\n"
            "Use a formal auditor tone. Highlight key risks and give 3 prioritised "
            "remediation recommendations. Do NOT use markdown or bullet symbols."
        )
        ai_text = gem.generate_content(prompt).text.strip()
    except Exception:
        ai_text = (
            f"This audit examined {summary.get('total_records', 0)} transactions "
            f"for {company} over {period}. The AI risk engine identified {hr_count} "
            f"high-risk records representing an estimated leakage exposure of "
            f"Rs.{leakage:,.0f}. "
            + ("The portfolio has breached the approved budget ceiling. "
               if breach else
               "The portfolio remains within the approved budget ceiling. ")
            + "Detailed findings are presented in the sections that follow. "
              "Management attention is directed to the high-risk transactions "
              "flagged in Section 6 and to the departmental variance analysis "
              "in Section 5. Immediate remedial action is recommended for all "
              "HIGH RISK transactions."
        )
    elements.append(Paragraph(ai_text, S["body"]))
    elements.append(PageBreak())

    # =========================================================================
    #  SECTION 2 – METHODOLOGY
    # =========================================================================
    flex_key = summary.get("meta", {}).get("budget_flexibility", "Moderate")
    flex_pct = int(FLEX_MAP.get(flex_key, 0.10) * 100)

    elements += section_header("2", "Audit Scope and Methodology",
                               "Data sources, detection techniques, scoring model")
    elements.append(Paragraph("Data Sources and Scope", S["h2"]))
    elements.append(Paragraph(
        f"The audit ingested {summary.get('total_records', 0)} transaction records "
        f"from the client-provided CSV dataset. Records span departments, vendors, "
        f"service types, and approval statuses. All monetary values are in Indian "
        f"Rupees (Rs.). The budget flexibility threshold applied was [{flex_key}] "
        f"({flex_pct}% above total planned budget).",
        S["body"],
    ))
    elements.append(Paragraph("Detection Techniques", S["h2"]))

    methods = [
        ("ISOLATION FOREST (scikit-learn)",
         "Unsupervised ML model trained on 7 engineered features to detect "
         "statistically anomalous transactions in high-dimensional space."),
        ("ADAPTIVE THRESHOLD SCORING",
         "Overrun ratios compared against the 75th-percentile dynamic limit, "
         "preventing rigid fixed-threshold false positives."),
        ("PEER-COHORT BENCHMARKING",
         "Each transaction is compared with its department x service-type peer "
         "group, surfacing within-category anomalies invisible to global thresholds."),
        ("TEMPORAL SPIKE DETECTION",
         "Monthly spend aggregated; rows exceeding 1.5x the monthly average "
         "are flagged as temporal spikes."),
        ("CONCENTRATION RISK",
         "Transactions exceeding 25% of the total planned budget receive additional "
         "risk weight, regardless of other signals."),
        ("VELOCITY ANALYSIS",
         "Week-over-week spend acceleration computed per row to identify "
         "sudden ramp-ups that precede month-end budget dumps."),
    ]

    meth_data = []
    for i, (tech, desc) in enumerate(methods):
        meth_data.append([
            Paragraph(f"{i+1:02d}", sty(f"mnum_{i}", fontSize=14,
                       fontName=BODY_FONT_BOLD, textColor=ACCENT, alignment=TA_CENTER)),
            Paragraph(f"<b>{tech}</b><br/>{desc}",
                      sty(f"mdesc_{i}", fontSize=8, fontName=BODY_FONT,
                          leading=12, textColor=DARK_GREY)),
        ])

    meth_tbl = Table(meth_data, colWidths=[32, 430])
    meth_tbl.setStyle(TableStyle([
        ("ROWBACKGROUNDS", (0, 0), (-1, -1), [colors.white, LIGHT_BG]),
        ("GRID",           (0, 0), (-1, -1), 0.2, MID_GREY),
        ("TOPPADDING",     (0, 0), (-1, -1), 7),
        ("BOTTOMPADDING",  (0, 0), (-1, -1), 7),
        ("LEFTPADDING",    (0, 0), (-1, -1), 8),
        ("RIGHTPADDING",   (0, 0), (-1, -1), 8),
        ("VALIGN",         (0, 0), (-1, -1), "MIDDLE"),
    ]))
    elements.append(meth_tbl)
    elements.append(PageBreak())

    # =========================================================================
    #  SECTION 3 – PORTFOLIO BUDGET ANALYSIS
    # =========================================================================
    elements += section_header("3", "Portfolio-Level Budget Analysis",
                               "Planned vs actual spend, breach indicators, headroom")

    status_ratio = ("HIGH" if ratio > 1.2 else ("ELEVATED" if ratio > 1 else "NORMAL"))

    budget_data = [
        ["Metric", "Value", "Status"],
        ["Total Planned Budget",               f"Rs.{total_planned:,.2f}", "—"],
        ["Approved Limit (with flexibility)",  f"Rs.{budget_limit:,.2f}",  "—"],
        ["Total Actual Spend",                 f"Rs.{total_spent:,.2f}",
         "BREACH" if breach else "OK"],
        ["Remaining Approved Headroom",        f"Rs.{remaining:,.2f}",     "—"],
        ["Over-Budget Multiplier",             f"{ratio:.4f}x",            status_ratio],
        ["Estimated High-Risk Leakage",        f"Rs.{leakage:,.2f}",       "FLAGGED"],
        ["Medium-Risk Exposure",               f"Rs.{medium_spend:,.2f}", "MONITOR"],
        ["Average Anomaly Score",              f"{avg_anomaly:.1f}/100",   "—"],
    ]

    bt_col_w = [(PAGE_W - 90) * 0.52, (PAGE_W - 90) * 0.31, (PAGE_W - 90) * 0.17]
    budget_table = Table(budget_data, colWidths=bt_col_w)
    budget_table.setStyle(TableStyle([
        ("BACKGROUND",    (0, 0), (-1, 0),  NAVY),
        ("TEXTCOLOR",     (0, 0), (-1, 0),  colors.white),
        ("FONTNAME",      (0, 0), (-1, 0),  BODY_FONT_BOLD),
        ("FONTNAME",      (0, 1), (-1, -1), BODY_FONT),
        ("FONTSIZE",      (0, 0), (-1, -1), 9),
        ("ROWBACKGROUNDS",(0, 1), (-1, -1), [colors.white, LIGHT_BG]),
        ("GRID",          (0, 0), (-1, -1), 0.3, MID_GREY),
        ("ALIGN",         (1, 0), (-1, -1), "RIGHT"),
        ("TOPPADDING",    (0, 0), (-1, -1), 5),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
        ("TEXTCOLOR",     (2, 3), (2, 3),   RED if breach else TEAL),
        ("TEXTCOLOR",     (2, 6), (2, 6),   RED),
        ("TEXTCOLOR",     (2, 7), (2, 7),   AMBER),
        ("FONTNAME",      (2, 3), (2, 3),   BODY_FONT_BOLD),
        ("FONTNAME",      (2, 6), (2, 6),   BODY_FONT_BOLD),
    ]))
    elements.append(budget_table)
    elements.append(PageBreak())

    # =========================================================================
    #  SECTION 4 – AI ANOMALY DETECTION (charts)
    # =========================================================================
    elements += section_header("4", "AI Anomaly Detection Results",
                               "Isolation Forest predictions, risk distribution, burn rate")

    has_ai = bool(summary.get("has_ai", False))
    engine_status = ("ACTIVE — Isolation Forest predictions applied"
                     if has_ai else "FALLBACK — rule-based scoring only")
    elements.append(Paragraph(
        f"Engine status: [{engine_status}]",
        sty("EngStat", fontSize=9, fontName=BODY_FONT_BOLD,
            textColor=TEAL if has_ai else AMBER),
    ))
    elements.append(Spacer(1, 8))

    # Chart A – Risk distribution pie
    if not df.empty and "risk_level" in df.columns:
        rc     = df["risk_level"].value_counts()
        labels = rc.index.tolist()
        vals   = rc.values.tolist()
        clrs   = [RISK_COLORS.get(l, "#888") for l in labels]

        fig, ax = plt.subplots(figsize=(5, 3.2), facecolor="#FDFCF8")
        wedges, texts, autotexts = ax.pie(
            vals, labels=labels, colors=clrs, autopct="%1.1f%%",
            startangle=140, textprops={"fontsize": 8, "fontfamily": "DejaVu Sans Mono"},
            pctdistance=0.78,
        )
        for at in autotexts:
            at.set_fontweight("bold")
        ax.set_title("Risk Level Distribution", fontsize=10,
                     fontfamily="DejaVu Serif", fontweight="bold", pad=10)
        fig.tight_layout()
        elements.append(rl_image(fig, width=320, height=230))

    elements.append(Spacer(1, 8))

    # Chart B – Burn rate
    g1_labels_parsed = json.loads(summary.get("g1_burn_labels", "[]"))
    g1_data_parsed   = json.loads(summary.get("g1_burn_data",   "[]"))
    if g1_labels_parsed and g1_data_parsed:
        n    = len(g1_labels_parsed)
        step = max(1, n // 30)
        xs   = list(range(0, n, step))

        fig, ax = plt.subplots(figsize=(7, 2.8), facecolor="#FDFCF8")
        ax.plot(
            [g1_labels_parsed[i] for i in xs],
            [g1_data_parsed[i]   for i in xs],
            color="#185FA5", linewidth=1.3, marker="o", markersize=3.5,
        )
        ax.fill_between(
            [g1_labels_parsed[i] for i in xs],
            [g1_data_parsed[i]   for i in xs],
            alpha=0.1, color="#185FA5",
        )
        ax.set_title("Spend Burn-Rate Over Time", fontsize=10,
                     fontfamily="DejaVu Serif", fontweight="bold")
        ax.set_xlabel("Date", fontsize=8, fontfamily="DejaVu Sans Mono")
        ax.set_ylabel("Rs. Actual Spend", fontsize=8, fontfamily="DejaVu Sans Mono")
        ax.tick_params(labelsize=7)
        plt.xticks(rotation=45, ha="right")
        fig.tight_layout()
        elements.append(rl_image(fig, width=480, height=200))

    elements.append(PageBreak())

    # =========================================================================
    #  SECTION 5 – DEPARTMENTAL DEEP-DIVE
    # =========================================================================
    elements += section_header("5", "Departmental Deep-Dive",
                               "Per-department spend, service type breakdown, risk flags")

    if not df.empty and "department" in df.columns:
        departments = df["department"].unique()
        for dept in list(departments)[:10]:
            dept_df    = df[df["department"] == dept]
            dept_total = float(dept_df["actual_spend"].sum())
            dept_hr    = (dept_df[dept_df["risk_level"] == "HIGH RISK"]
                          if "risk_level" in dept_df.columns
                          else dept_df.iloc[0:0])

            # Dept header banner (mini)
            dept_banner = Table(
                [[Paragraph(f"DEPT: {dept.upper()}", sty("dh", fontSize=10,
                             fontName=BODY_FONT_BOLD, textColor=colors.white))]],
                colWidths=[PAGE_W - 90],
            )
            dept_banner.setStyle(TableStyle([
                ("BACKGROUND",    (0, 0), (-1, -1), ACCENT),
                ("TOPPADDING",    (0, 0), (-1, -1), 5),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
                ("LEFTPADDING",   (0, 0), (-1, -1), 10),
            ]))
            elements.append(dept_banner)

            elements.append(Paragraph(
                f"Total Spend: Rs.{dept_total:,.2f}    |    "
                f"Transactions: {len(dept_df)}    |    "
                f"High-Risk Flags: {len(dept_hr)}",
                sty("dinfo", fontSize=8, fontName=BODY_FONT,
                    textColor=DARK_GREY, spaceBefore=4, spaceAfter=4),
            ))

            if "service_type" in dept_df.columns:
                st_spend = (
                    dept_df.groupby("service_type")["actual_spend"]
                    .sum().sort_values(ascending=False).head(8)
                )
                if not st_spend.empty:
                    fig, ax = plt.subplots(figsize=(6, 2.3), facecolor="#FDFCF8")
                    ax.bar(range(len(st_spend)), st_spend.values,
                           color=PDF_PALETTE[:len(st_spend)])
                    ax.set_xticks(range(len(st_spend)))
                    ax.set_xticklabels(
                        [s[:20] for s in st_spend.index],
                        rotation=28, ha="right", fontsize=6,
                        fontfamily="DejaVu Sans Mono",
                    )
                    ax.set_title(f"{dept} — Spend by Service Type",
                                 fontsize=8, fontfamily="DejaVu Serif",
                                 fontweight="bold")
                    ax.tick_params(labelsize=6)
                    fig.tight_layout()
                    elements.append(rl_image(fig, width=390, height=175))

            cols_show = [c for c in ["transaction_date", "vendor", "service_type",
                                     "actual_spend", "planned_budget", "risk_level"]
                         if c in dept_df.columns]
            if cols_show:
                elements.append(Spacer(1, 5))
                elements.append(dept_table(dept_df, cols_show))

            elements.append(Spacer(1, 8))
            elements.append(divider(MID_GREY, 0.25))

    elements.append(PageBreak())

    # =========================================================================
    #  SECTION 6 – HIGH-RISK TRANSACTION LOGS
    # =========================================================================
    elements += section_header("6", "High-Risk Transaction Logs",
                               f"{hr_count} transactions flagged  |  "
                               f"Est. leakage Rs.{leakage:,.0f}")

    elements.append(Paragraph(
        f"The following {hr_count} transaction(s) were classified as HIGH RISK "
        f"by the AI engine. Estimated total leakage exposure: Rs.{leakage:,.2f}.",
        S["body"],
    ))
    elements.append(Spacer(1, 8))

    all_hr   = summary.get("all_high_risks", [])
    LOG_COLS = ["department", "vendor", "service_type",
                "actual_spend", "planned_budget", "overrun_ratio",
                "anomaly_score_pct", "risk_confidence", "risk_level"]
    CHUNK    = 30

    for chunk_start in range(0, max(len(all_hr), 1), CHUNK):
        chunk      = all_hr[chunk_start:chunk_start + CHUNK]
        avail_cols = [c for c in LOG_COLS if chunk and c in chunk[0]]

        if not avail_cols:
            break

        col_w = (PAGE_W - 90) / len(avail_cols)
        tdata = [[Paragraph(f"<b>{c.replace('_',' ').title()}</b>", S["small"])
                  for c in avail_cols]]

        for r in chunk:
            trow = []
            for c in avail_cols:
                v = r.get(c, "")
                if c in ("actual_spend", "planned_budget"):
                    cell = Paragraph(f"Rs.{float(v or 0):,.0f}", S["mono_data"])
                elif c == "overrun_ratio":
                    cell = Paragraph(f"{float(v or 0):.2f}x", S["mono_data"])
                elif c in ("anomaly_score_pct", "risk_confidence"):
                    cell = Paragraph(f"{float(v or 0):.1f}", S["mono_data"])
                elif c == "risk_level":
                    cell = risk_badge(str(v))
                else:
                    cell = Paragraph(str(v)[:28], S["mono_data"])
                trow.append(cell)
            tdata.append(trow)

        lt = Table(tdata, colWidths=[col_w] * len(avail_cols), repeatRows=1)
        lt.setStyle(TableStyle([
            ("BACKGROUND",    (0, 0), (-1, 0),  colors.HexColor("#7A1A1A")),
            ("TEXTCOLOR",     (0, 0), (-1, 0),  colors.white),
            ("FONTNAME",      (0, 0), (-1, 0),  BODY_FONT_BOLD),
            ("FONTNAME",      (0, 1), (-1, -1), BODY_FONT),
            ("ROWBACKGROUNDS",(0, 1), (-1, -1), [colors.white, colors.HexColor("#FCEBEB")]),
            ("GRID",          (0, 0), (-1, -1), 0.25, colors.HexColor("#F09595")),
            ("TOPPADDING",    (0, 0), (-1, -1), 3),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
            ("FONTSIZE",      (0, 0), (-1, -1), 7),
        ]))
        elements.append(lt)
        elements.append(Spacer(1, 8))
        if chunk_start + CHUNK < len(all_hr):
            elements.append(PageBreak())

    elements.append(PageBreak())

    # =========================================================================
    #  SECTION 7 – TREND VISUALISATIONS
    # =========================================================================
    elements += section_header("7", "Trend and Visualisation Analysis",
                               "Vendor spend, monthly trend, department breakdown")

    # 7A – Top vendors
    g2_labels = json.loads(summary.get("g2_vendor_labels", "[]"))
    g2_data   = json.loads(summary.get("g2_vendor_data",   "[]"))
    if g2_labels and g2_data:
        fig, ax = plt.subplots(figsize=(6.5, 3), facecolor="#FDFCF8")
        bars = ax.barh(g2_labels[::-1], g2_data[::-1],
                       color=PDF_PALETTE[:len(g2_labels)])
        ax.set_title("Top Vendors by Flagged Spend", fontsize=10,
                     fontfamily="DejaVu Serif", fontweight="bold")
        ax.set_xlabel("Rs. Actual Spend", fontsize=8,
                      fontfamily="DejaVu Sans Mono")
        ax.tick_params(labelsize=7)
        for bar in bars:
            w = bar.get_width()
            ax.text(w * 1.01, bar.get_y() + bar.get_height() / 2,
                    f"Rs.{w:,.0f}", va="center", fontsize=6,
                    fontfamily="DejaVu Sans Mono")
        fig.tight_layout()
        elements.append(Paragraph("Top Vendors by Flagged Spend", S["h2"]))
        elements.append(rl_image(fig, width=470, height=220))
        elements.append(Spacer(1, 10))

    # 7B – Monthly spend
    g5_labels = json.loads(summary.get("g5_temp_labels", "[]"))
    g5_data   = json.loads(summary.get("g5_temp_data",   "[]"))
    if g5_labels and g5_data:
        fig, ax = plt.subplots(figsize=(6.5, 2.8), facecolor="#FDFCF8")
        ax.bar(g5_labels, g5_data, color="#1D9E75", alpha=0.85)
        ax.set_title("Monthly Actual Spend", fontsize=10,
                     fontfamily="DejaVu Serif", fontweight="bold")
        ax.set_xlabel("Month", fontsize=8, fontfamily="DejaVu Sans Mono")
        ax.set_ylabel("Rs.", fontsize=8, fontfamily="DejaVu Sans Mono")
        ax.tick_params(labelsize=7)
        fig.tight_layout()
        elements.append(Paragraph("Monthly Spend Trend", S["h2"]))
        elements.append(rl_image(fig, width=470, height=200))
        elements.append(Spacer(1, 10))

    # 7C – Dept spend
    g7_labels = json.loads(summary.get("g7_dept_labels", "[]"))
    g7_data   = json.loads(summary.get("g7_dept_data",   "[]"))
    if g7_labels and g7_data:
        fig, ax = plt.subplots(figsize=(6.5, 3.2), facecolor="#FDFCF8")
        ax.barh(g7_labels[::-1][:12], g7_data[::-1][:12],
                color="#7F77DD", alpha=0.85)
        ax.set_title("Spend by Department", fontsize=10,
                     fontfamily="DejaVu Serif", fontweight="bold")
        ax.set_xlabel("Rs. Actual Spend", fontsize=8,
                      fontfamily="DejaVu Sans Mono")
        ax.tick_params(labelsize=7)
        fig.tight_layout()
        elements.append(Paragraph("Spend by Department", S["h2"]))
        elements.append(rl_image(fig, width=470, height=230))

    elements.append(PageBreak())

    # =========================================================================
    #  SECTION 8 – FINDINGS AND RECOMMENDATIONS
    # =========================================================================
    elements += section_header("8", "Findings and Recommendations",
                               "Actionable findings with severity and remediation steps")

    findings = [
        ("F-01", "Budget Discipline",
         f"The portfolio {'has exceeded' if breach else 'is approaching'} the approved "
         f"spending limit. Over-budget ratio: {ratio:.2f}x. "
         "Recommended action: Immediate spend freeze on all non-critical services "
         "pending CFO sign-off.",
         breach),
        ("F-02", "Anomalous Vendor Spend",
         f"{len(g2_labels)} vendor(s) account for a disproportionate share of flagged "
         "transactions. Recommended action: Commission independent vendor audits and "
         "renegotiate contract terms within 30 days.",
         len(g2_labels) > 0),
        ("F-03", "Temporal Spike Events",
         "One or more months exhibit spend spikes exceeding 1.5x the monthly average, "
         "suggesting batch processing of backdated invoices or split-purchase "
         "circumvention. Recommended action: Enforce real-time invoice matching.",
         True),
        ("F-04", "Unapproved Transactions",
         "Transactions flagged with non-standard approval statuses show elevated "
         "overrun ratios. Recommended action: Re-route all such items to a "
         "dual-approval workflow and freeze processing.",
         True),
        ("F-05", "Velocity Anomalies",
         "Week-over-week spend acceleration detected — indicates potential month-end "
         "budget dumping. Recommended action: Implement weekly spend caps "
         "at the department level and automate alerting.",
         True),
    ]

    for code, ftitle, ftext, active in findings:
        elements.append(finding_card(code, ftitle, ftext, active))

    elements.append(PageBreak())

    # =========================================================================
    #  SECTION 9 – CONCLUSION
    # =========================================================================
    elements += section_header("9", "Auditor Conclusion",
                               "Overall portfolio classification and sign-off")

    overall_risk = (
        "HIGH RISK"
        if (breach or hr_count > summary.get("total_records", 1) * 0.15)
        else "MODERATE RISK"
    )
    risk_color = RED if overall_risk == "HIGH RISK" else AMBER
    risk_bg    = colors.HexColor("#FCEBEB") if overall_risk == "HIGH RISK" else colors.HexColor("#FEF5E4")

    # Classification box
    class_box = Table(
        [[
            Paragraph(
                f"OVERALL PORTFOLIO CLASSIFICATION: {overall_risk}",
                sty("ClassBox", fontSize=13, fontName=BODY_FONT_BOLD,
                    textColor=risk_color, alignment=TA_CENTER),
            )
        ]],
        colWidths=[PAGE_W - 90],
    )
    class_box.setStyle(TableStyle([
        ("BACKGROUND",    (0, 0), (-1, -1), risk_bg),
        ("LINEABOVE",     (0, 0), (-1, 0),  2, risk_color),
        ("LINEBELOW",     (0, 0), (-1, -1), 2, risk_color),
        ("TOPPADDING",    (0, 0), (-1, -1), 14),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 14),
    ]))
    elements.append(class_box)
    elements.append(Spacer(1, 12))

    elements.append(Paragraph(
        f"Based on {summary.get('total_records', 0)} transactions analysed for "
        f"{company} during {period}, the AI audit engine has determined the "
        f"portfolio risk level to be {overall_risk}. A total of {hr_count} "
        f"transactions require immediate management intervention. Estimated "
        f"financial exposure from high-risk items is Rs.{leakage:,.2f}.",
        S["body"],
    ))

    # Sign-off block with QR
    qr_signoff = qr_image_flowable(size=58, label="Report ID QR")
    signoff_text = Table(
        [[
            Paragraph(
                f"<br/><br/>"
                f"_________________________<br/>"
                f"<b>AI Certified Lead Auditor</b><br/>"
                f"AI Audit Engine v3<br/>"
                f"Report ID: {report_id}<br/>"
                f"Issued: {generated_at}",
                sty("SignOff", fontSize=9, fontName=BODY_FONT,
                    leading=14, textColor=DARK_GREY),
            ),
            qr_signoff,
        ]],
        colWidths=[340, 80],
    )
    signoff_text.setStyle(TableStyle([
        ("VALIGN",       (0, 0), (-1, -1), "BOTTOM"),
        ("LEFTPADDING",  (0, 0), (-1, -1), 0),
        ("RIGHTPADDING", (0, 0), (-1, -1), 0),
        ("TOPPADDING",   (0, 0), (-1, -1), 0),
        ("BOTTOMPADDING",(0, 0), (-1, -1), 0),
        ("ALIGN",        (1, 0), (1, 0), "RIGHT"),
    ]))
    elements.append(Spacer(1, 0.5 * inch))
    elements.append(signoff_text)
    elements.append(PageBreak())

    # =========================================================================
    #  APPENDIX A – LEGAL DISCLAIMER
    # =========================================================================
    elements += section_header("A", "Appendix — Legal Disclaimer & Glossary")

    elements.append(Paragraph("A.1  Legal Disclaimer", S["h2"]))
    elements.append(Paragraph(
        "This report is generated by an automated AI financial audit system. "
        "While the system employs statistically rigorous methods, all findings "
        "must be validated by a qualified human auditor before being used as the "
        "basis for legal, regulatory, or disciplinary action. The anomaly scores "
        "and risk classifications are probabilistic in nature and do not constitute "
        "definitive evidence of fraud or malfeasance. The system operator accepts "
        "no liability for decisions made solely on the basis of this report.",
        S["body"],
    ))
    elements.append(Spacer(1, 10))

    elements.append(Paragraph("A.2  Feature Glossary", S["h2"]))

    glossary = [
        ["TERM",               "DEFINITION"],
        ["overrun_ratio",      "actual_spend / planned_budget. Values > 1 indicate overspend."],
        ["anomaly_score_pct",  "Normalised isolation-forest score (0-100). Higher = more anomalous."],
        ["temporal_spike",     "True when actual_spend > 1.5x the monthly cohort average."],
        ["impact_score",       "actual_spend / portfolio mean spend. Measures absolute impact."],
        ["risk_confidence",    "Composite score (0-100): overrun + anomaly + spike + breach."],
        ["velocity",           "Week-over-week spend acceleration, clipped at zero for new periods."],
        ["peer_avg",           "Mean overrun_ratio within same department x service_type cohort."],
        ["budget_gap",         "actual_spend - planned_budget. Positive = overspend; negative = savings."],
    ]
    gt = Table(glossary, colWidths=[130, 365])
    gt.setStyle(TableStyle([
        ("BACKGROUND",    (0, 0), (-1, 0),  NAVY),
        ("TEXTCOLOR",     (0, 0), (-1, 0),  colors.white),
        ("FONTNAME",      (0, 0), (-1, 0),  BODY_FONT_BOLD),
        ("FONTNAME",      (0, 1), (-1, -1), BODY_FONT),
        ("FONTNAME",      (0, 1), (0, -1),  BODY_FONT_BOLD),
        ("TEXTCOLOR",     (0, 1), (0, -1),  ACCENT),
        ("ROWBACKGROUNDS",(0, 1), (-1, -1), [colors.white, LIGHT_BG]),
        ("GRID",          (0, 0), (-1, -1), 0.3, MID_GREY),
        ("FONTSIZE",      (0, 0), (-1, -1), 8),
        ("TOPPADDING",    (0, 0), (-1, -1), 5),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
        ("LEFTPADDING",   (0, 0), (-1, -1), 7),
    ]))
    elements.append(gt)

    # =========================================================================
    #  BUILD PDF
    # =========================================================================
    doc.build(elements)
    buffer.seek(0)

    safe_name = "".join(
        c if c.isalnum() or c in (".", "_", "-", " ") else "_"
        for c in company
    )
    filename = f"Audit_{safe_name}_{datetime.utcnow().strftime('%Y%m%d')}.pdf"
    return FileResponse(buffer, as_attachment=True, filename=filename,
                        content_type="application/pdf")

def login_page(request):
    return render(request, 'prologin.html')  