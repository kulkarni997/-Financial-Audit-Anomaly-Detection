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
import matplotlib 
matplotlib.use('Agg')  # Required for Django/Server environments
import matplotlib.pyplot as plt

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
api_key = os.getenv("GENAI_API_KEY")
client = Client(api_key=api_key)   # ✅ new client object

def generate_all_summaries(results: dict) -> dict:
    """
    Final optimized AI summary generator for 2026.
    Uses google-genai Client instead of deprecated configure().
    """

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


# --- HELPER: GENERATE MATPLOTLIB GRAPHS ---
def generate_category_graph(data, labels, title, chart_type="bar"):
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
    return render(request, "approval.html")


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
MODEL_PATH = r"C:\Users\HP\OneDrive\Desktop\EDUNET\-Financial-Audit-Anomaly-Detection\audit\ml_assets\project_Models\advanced_audit_pipeline.pkl"
isolation_model = joblib.load(MODEL_PATH)


def project_audit(request):
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
    """
    Streams a comprehensive, multi-section audit PDF using data stored in the
    session by project_audit().  Falls back gracefully if session data is absent.
    """
    summary = request.session.get("summary")
    if not summary:
        buf = io.BytesIO(b"No audit data in session. Please run the audit first.")
        return FileResponse(buf, as_attachment=True, filename="NoData.txt")

    df_data = summary.get("df", [])
    df = pd.DataFrame(df_data) if df_data else pd.DataFrame()

    # Coerce numeric columns that may have become strings after JSON round-trip
    for col in ["actual_spend", "planned_budget", "overrun_ratio",
                "budget_gap", "anomaly_score_pct", "risk_confidence"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    # ── Document setup ────────────────────────────────────────────────────────
    company      = summary.get("meta", {}).get("company_name", "Client Organisation")
    period       = summary.get("meta", {}).get("audit_period",  "FY 2025-26")
    report_id    = f"AUD-{datetime.utcnow().strftime('%Y%m%d')}-{np.random.randint(1000, 9999)}"
    generated_at = summary.get("generated_at", datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC"))

    buffer = io.BytesIO()
    doc = SimpleDocTemplate(
        buffer,
        pagesize=A4,
        rightMargin=45, leftMargin=45,
        topMargin=50,   bottomMargin=50,
        title=f"AI Financial Audit — {company}",
        author="AI Audit Engine v3",
    )

    elements = []
    styles   = getSampleStyleSheet()

    # ── Custom styles ─────────────────────────────────────────────────────────
    NAVY      = colors.HexColor("#0C447C")
    DARK_TEAL = colors.HexColor("#085041")
    RED       = colors.HexColor("#A32D2D")
    AMBER     = colors.HexColor("#854F0B")
    LIGHT_BG  = colors.HexColor("#F7F6F2")
    MID_GREY  = colors.HexColor("#888780")
    DARK_GREY = colors.HexColor("#2C2C2A")

    def sty(name, **kw):
        return ParagraphStyle(name, parent=styles["Normal"], **kw)

    S = {
        "cover_title": sty("CoverTitle", fontSize=28, leading=36,
                           alignment=TA_CENTER, textColor=DARK_GREY,
                           fontName="Helvetica-Bold", spaceAfter=6),
        "cover_sub":   sty("CoverSub", fontSize=13, alignment=TA_CENTER,
                           textColor=MID_GREY, spaceAfter=4),
        "h1":          sty("H1", fontSize=18, fontName="Helvetica-Bold",
                           textColor=NAVY, spaceBefore=18, spaceAfter=8,
                           borderPad=4),
        "h2":          sty("H2", fontSize=14, fontName="Helvetica-Bold",
                           textColor=NAVY, spaceBefore=14, spaceAfter=6),
        "h3":          sty("H3", fontSize=12, fontName="Helvetica-Bold",
                           textColor=DARK_GREY, spaceBefore=8, spaceAfter=4),
        "body":        sty("Body", fontSize=10, leading=15, textColor=DARK_GREY,
                           spaceAfter=6),
        "small":       sty("Small", fontSize=9, leading=13, textColor=MID_GREY),
        "callout":     sty("Callout", fontSize=11, leading=16,
                           textColor=DARK_GREY, backColor=LIGHT_BG,
                           borderPad=8, borderWidth=0, leftIndent=12,
                           rightIndent=12, spaceBefore=8, spaceAfter=8),
        "red_flag":    sty("RedFlag", fontSize=10, fontName="Helvetica-Bold",
                           textColor=RED),
        "toc_entry":   sty("TOC", fontSize=11, leading=18, textColor=DARK_GREY),
        "footer":      sty("Footer", fontSize=8, alignment=TA_CENTER,
                           textColor=MID_GREY),
    }

    # ── Helper: divider line ──────────────────────────────────────────────────
    def divider(color=MID_GREY, width=0.5):
        return HRFlowable(width="100%", thickness=width, color=color,
                          spaceAfter=8, spaceBefore=4)

    # ── Helper: metric-card table row ─────────────────────────────────────────
    def metric_table(pairs: list[tuple[str, str]], color=NAVY) -> Table:
        """pairs = [(label, value), ...]"""
        row1 = [Paragraph(f"<font color='#{color.hexval()[2:]}'><b>{v}</b></font>",
                           sty("mv", fontSize=16, fontName="Helvetica-Bold",
                               textColor=color, alignment=TA_CENTER))
                for _, v in pairs]
        row2 = [Paragraph(f"<font color='grey'>{l}</font>",
                           sty("ml", fontSize=9, textColor=MID_GREY,
                               alignment=TA_CENTER))
                for l, _ in pairs]
        t = Table([row1, row2],
                  colWidths=[(_W - 90) / len(pairs)] * len(pairs))
        t.setStyle(TableStyle([
            ("BACKGROUND",   (0, 0), (-1, -1), LIGHT_BG),
            ("ROWBACKGROUNDS",(0,0),(-1,-1),[LIGHT_BG, colors.white]),
            ("TOPPADDING",   (0, 0), (-1, -1), 8),
            ("BOTTOMPADDING",(0, 0), (-1, -1), 8),
            ("LEFTPADDING",  (0, 0), (-1, -1), 6),
            ("RIGHTPADDING", (0, 0), (-1, -1), 6),
            ("GRID",         (0, 0), (-1, -1), 0.3, MID_GREY),
            ("ROUNDEDCORNERS", [4]),
        ]))
        return t

    # ── Helper: risk-coloured badge ───────────────────────────────────────────
    def risk_badge(level: str) -> Paragraph:
        hex_map = {"HIGH RISK": "A32D2D", "MEDIUM RISK": "854F0B", "LOW RISK": "0F6E56"}
        bg_map  = {"HIGH RISK": "FCEBEB", "MEDIUM RISK": "FAEEDA", "LOW RISK": "E1F5EE"}
        h = hex_map.get(level, "444441")
        b = bg_map.get(level, "F1EFE8")
        return Paragraph(
            f'<font color="#{h}"><b>{level}</b></font>',
            sty("rb", fontSize=9, backColor=colors.HexColor(f"#{b}"),
                borderPad=3, alignment=TA_CENTER),
        )

    # ── Helper: matplotlib chart → ReportLab Image ────────────────────────────
    def rl_image(fig, width=480, height=220) -> Image:
        buf = _chart_png(fig)
        return Image(buf, width=width, height=height)

    # ─────────────────────────────────────────────────────────────────────────
    #  PAGE 1 – COVER
    # ─────────────────────────────────────────────────────────────────────────
    elements += [
        Spacer(1, 1.6 * inch),
        Paragraph("INDEPENDENT AI FINANCIAL AUDIT", S["cover_title"]),
        Paragraph("COMPREHENSIVE RISK &amp; ANOMALY REPORT", S["cover_sub"]),
        Spacer(1, 0.3 * inch),
        divider(NAVY, width=1.5),
        Spacer(1, 0.3 * inch),
        Paragraph(f"<b>Client:</b> {company}", S["body"]),
        Paragraph(f"<b>Audit Period:</b> {period}", S["body"]),
        Paragraph(f"<b>Report ID:</b> {report_id}", S["body"]),
        Paragraph(f"<b>Generated:</b> {generated_at}", S["body"]),
        Paragraph(f"<b>Classification:</b> CONFIDENTIAL", S["body"]),
        Spacer(1, 0.6 * inch),
        divider(MID_GREY),
        Paragraph(
            "This report is produced by an AI-assisted audit engine using "
            "Isolation Forest anomaly detection, statistical risk scoring, and "
            "peer-cohort benchmarking. It should be reviewed alongside manual "
            "human oversight before enforcement action is taken.",
            S["small"],
        ),
        PageBreak(),
    ]

    # ─────────────────────────────────────────────────────────────────────────
    #  PAGE 2 – TABLE OF CONTENTS
    # ─────────────────────────────────────────────────────────────────────────
    toc_sections = [
        ("1", "Executive Summary"),
        ("2", "Audit Scope and Methodology"),
        ("3", "Portfolio-Level Budget Analysis"),
        ("4", "AI Anomaly Detection Results"),
        ("5", "Departmental Deep-Dive"),
        ("6", "High-Risk Transaction Logs"),
        ("7", "Trend and Visualisation Analysis"),
        ("8", "Findings and Recommendations"),
        ("9", "Auditor Conclusion"),
        ("A", "Appendix and Legal Disclaimer"),
    ]
    elements.append(Paragraph("Table of Contents", S["h1"]))
    elements.append(divider())
    for num, title in toc_sections:
        elements.append(
            Paragraph(
                f"<b>Section {num}</b> &nbsp;&nbsp; {title}",
                S["toc_entry"],
            )
        )
    elements.append(PageBreak())

    # ─────────────────────────────────────────────────────────────────────────
    #  PAGE 3 – EXECUTIVE SUMMARY
    # ─────────────────────────────────────────────────────────────────────────
    total_spent   = summary.get("total_spent", 0)
    total_planned = summary.get("total_planned", 0)
    leakage       = summary.get("leakage", 0)
    hr_count      = summary.get("high_risk_count", 0)
    breach        = summary.get("budget_breach", False)
    ratio         = summary.get("over_budget_ratio", 0)
    confidence    = summary.get("confidence", 0)

    elements.append(Paragraph("1. Executive Summary", S["h1"]))
    elements.append(divider())

    # Key metrics strip
    elements.append(
        metric_table([
            ("Total Transactions", str(summary.get("total_records", 0))),
            ("High-Risk Flags",    str(hr_count)),
            ("Medium-Risk Flags",  str(summary.get("medium_risk_count", 0))),
            ("Audit Confidence",   f"{confidence:.1f}%"),
        ]),
    )
    elements.append(Spacer(1, 10))
    elements.append(
        metric_table([
            ("Total Planned",     f"₹{total_planned:,.0f}"),
            ("Total Actual",      f"₹{total_spent:,.0f}"),
            ("Flagged Leakage",   f"₹{leakage:,.0f}"),
            ("Over-Budget Ratio", f"{ratio:.2f}×"),
        ], color=RED if breach else DARK_TEAL),
    )
    elements.append(Spacer(1, 12))

    # AI-generated narrative (Gemini) with hard fallback
    try:
        import google.generativeai as genai
        import os
        genai.configure(api_key=os.environ["GEMINI_API_KEY"])
        client = genai.GenerativeModel("gemini-2.0-flash")
        prompt = (
            f"You are a senior financial auditor. Write a 400-word professional "
            f"executive summary for {company} covering the following metrics:\n"
            f"• Audit period: {period}\n"
            f"• Total planned budget: ₹{total_planned:,.0f}\n"
            f"• Total actual spend: ₹{total_spent:,.0f}\n"
            f"• High-risk transactions: {hr_count} (estimated leakage ₹{leakage:,.0f})\n"
            f"• Medium-risk transactions: {summary.get('medium_risk_count', 0)}\n"
            f"• Budget breach: {'YES' if breach else 'NO'}\n"
            f"• Overrun ratio: {ratio:.2f}×\n\n"
            "Use a formal auditor tone. Highlight key risks and give 3 prioritised "
            "remediation recommendations. Do NOT use markdown or bullet symbols."
        )
        ai_text = client.generate_content(prompt).text.strip()
    except Exception:
        ai_text = (
            f"This audit examined {summary.get('total_records', 0)} transactions for "
            f"{company} over {period}. The AI risk engine identified {hr_count} "
            f"high-risk records representing an estimated leakage exposure of "
            f"₹{leakage:,.0f}. "
            + ("The portfolio has breached the approved budget ceiling. "
               if breach else
               "The portfolio remains within the approved budget ceiling. ")
            + "Detailed findings are presented in the sections that follow. "
              "Management attention is directed to the high-risk transactions "
              "flagged in Section 6 and to the departmental variance analysis "
              "in Section 5. Immediate remedial action is recommended for all "
              "transactions classified as HIGH RISK."
        )
    elements.append(Paragraph(ai_text, S["body"]))
    elements.append(PageBreak())

    # ─────────────────────────────────────────────────────────────────────────
    #  PAGE 4 – METHODOLOGY
    # ─────────────────────────────────────────────────────────────────────────
    elements.append(Paragraph("2. Audit Scope and Methodology", S["h1"]))
    elements.append(divider())
    elements.append(Paragraph("Data Sources and Scope", S["h2"]))
    elements.append(
        Paragraph(
            f"The audit ingested {summary.get('total_records', 0)} transaction records "
            f"from the client-provided CSV. Records span departments, vendors, service "
            f"types, and approval statuses. All monetary values are in Indian Rupees (₹). "
            f"The budget flexibility threshold applied was "
            f"<b>{summary.get('meta', {}).get('budget_flexibility', 'Moderate')}</b> "
            f"({int(FLEX_MAP.get(summary.get('meta', {}).get('budget_flexibility', 'Moderate'), 0.10)*100)}% "
            f"above total planned budget).",
            S["body"],
        )
    )
    elements.append(Paragraph("Detection Techniques", S["h2"]))
    methods = [
        ("<b>Isolation Forest (scikit-learn)</b>",
         "Unsupervised ML model trained on 7 engineered features to detect "
         "statistically anomalous transactions in high-dimensional space."),
        ("<b>Adaptive Threshold Scoring</b>",
         "Overrun ratios compared against the 75th-percentile dynamic limit, "
         "preventing rigid fixed-threshold false positives."),
        ("<b>Peer-Cohort Benchmarking</b>",
         "Each transaction is compared with its department × service-type peer "
         "group, surfacing within-category anomalies invisible to global thresholds."),
        ("<b>Temporal Spike Detection</b>",
         "Monthly spend is aggregated and rows exceeding 1.5× the monthly average "
         "are flagged as temporal spikes."),
        ("<b>Concentration Risk</b>",
         "Transactions exceeding 25% of the total planned budget receive additional "
         "risk weight, regardless of other signals."),
    ]
    for title_html, desc in methods:
        elements.append(Paragraph(title_html, S["h3"]))
        elements.append(Paragraph(desc, S["body"]))
    elements.append(PageBreak())

    # ─────────────────────────────────────────────────────────────────────────
    #  PAGE 5 – PORTFOLIO BUDGET ANALYSIS
    # ─────────────────────────────────────────────────────────────────────────
    elements.append(Paragraph("3. Portfolio-Level Budget Analysis", S["h1"]))
    elements.append(divider())

    budget_data = [
        ["Metric", "Value", "Status"],
        ["Total Planned Budget",
         f"₹{total_planned:,.2f}", "—"],
        ["Approved Limit (with flexibility)",
         f"₹{summary.get('budget_limit', 0):,.2f}", "—"],
        ["Total Actual Spend",
         f"₹{total_spent:,.2f}",
         "BREACH" if breach else "OK"],
        ["Remaining Approved Headroom",
         f"₹{summary.get('remaining_budget', 0):,.2f}", "—"],
        ["Over-Budget Multiplier",
         f"{ratio:.4f}×",
         "HIGH" if ratio > 1.2 else ("ELEVATED" if ratio > 1 else "NORMAL")],
        ["Estimated High-Risk Leakage",
         f"₹{leakage:,.2f}",
         "FLAGGED"],
        ["Medium-Risk Exposure",
         f"₹{summary.get('medium_spend', 0):,.2f}",
         "MONITOR"],
        ["Average Anomaly Score",
         f"{summary.get('avg_anomaly_score', 0):.1f}/100", "—"],
    ]

    budget_table = Table(budget_data, colWidths=[240, 170, 85])
    budget_table.setStyle(TableStyle([
        ("BACKGROUND",    (0, 0), (-1, 0),  NAVY),
        ("TEXTCOLOR",     (0, 0), (-1, 0),  colors.white),
        ("FONTNAME",      (0, 0), (-1, 0),  "Helvetica-Bold"),
        ("FONTSIZE",      (0, 0), (-1, -1), 10),
        ("ROWBACKGROUNDS",(0, 1), (-1, -1), [colors.white, LIGHT_BG]),
        ("GRID",          (0, 0), (-1, -1), 0.3, MID_GREY),
        ("ALIGN",         (1, 0), (-1, -1), "RIGHT"),
        ("TOPPADDING",    (0, 0), (-1, -1), 5),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
        ("TEXTCOLOR",     (2, 3), (2, 3),   RED if breach else DARK_TEAL),
        ("TEXTCOLOR",     (2, 6), (2, 6),   RED),
        ("TEXTCOLOR",     (2, 7), (2, 7),   AMBER),
    ]))
    elements.append(budget_table)
    elements.append(PageBreak())

    # ─────────────────────────────────────────────────────────────────────────
    #  PAGE 6 – AI ANOMALY RESULTS (charts)
    # ─────────────────────────────────────────────────────────────────────────
    elements.append(Paragraph("4. AI Anomaly Detection Results", S["h1"]))
    elements.append(divider())

    # Chart A – Risk distribution pie
    if not df.empty:
        rc = df["risk_level"].value_counts()
        labels = rc.index.tolist()
        vals   = rc.values.tolist()
        clrs   = [RISK_COLORS.get(l, "#888") for l in labels]

        fig, ax = plt.subplots(figsize=(5, 3.5), facecolor="#FAFAF8")
        wedges, texts, autotexts = ax.pie(
            vals, labels=labels, colors=clrs,
            autopct="%1.1f%%", startangle=140,
            textprops={"fontsize": 9},
        )
        for at in autotexts:
            at.set_fontsize(8)
        ax.set_title("Risk Level Distribution", fontsize=11, fontweight="bold", pad=10)
        fig.tight_layout()
        elements.append(rl_image(fig, width=340, height=240))

    elements.append(Spacer(1, 8))

    # Chart B – Burn rate timeline
    g1_labels_parsed = json.loads(summary.get("g1_burn_labels", "[]"))
    g1_data_parsed   = json.loads(summary.get("g1_burn_data", "[]"))
    if g1_labels_parsed and g1_data_parsed:
        # subsample for readability
        n   = len(g1_labels_parsed)
        step = max(1, n // 30)
        xs   = list(range(0, n, step))
        fig, ax = plt.subplots(figsize=(7, 2.8), facecolor="#FAFAF8")
        ax.plot(
            [g1_labels_parsed[i] for i in xs],
            [g1_data_parsed[i]   for i in xs],
            color="#185FA5", linewidth=1.2, marker="o", markersize=3,
        )
        ax.fill_between(
            [g1_labels_parsed[i] for i in xs],
            [g1_data_parsed[i]   for i in xs],
            alpha=0.08, color="#185FA5",
        )
        ax.set_title("Spend Burn-Rate Over Time", fontsize=10, fontweight="bold")
        ax.set_xlabel("Date", fontsize=8)
        ax.set_ylabel("₹ Actual Spend", fontsize=8)
        ax.tick_params(labelsize=7)
        plt.xticks(rotation=45, ha="right")
        fig.tight_layout()
        elements.append(rl_image(fig, width=480, height=200))

    elements.append(PageBreak())

    # ─────────────────────────────────────────────────────────────────────────
    #  PAGES 7-N – DEPARTMENTAL DEEP-DIVE
    # ─────────────────────────────────────────────────────────────────────────
    elements.append(Paragraph("5. Departmental Deep-Dive", S["h1"]))
    elements.append(divider())

    if not df.empty and "department" in df.columns:
        departments = df["department"].unique()
        for dept in departments[:10]:  # cap at 10 depts
            dept_df = df[df["department"] == dept]
            dept_total = dept_df["actual_spend"].sum()
            dept_hr    = dept_df[dept_df["risk_level"] == "HIGH RISK"]

            elements.append(Paragraph(f"Department: {dept}", S["h2"]))
            elements.append(
                Paragraph(
                    f"Total Spend: <b>₹{dept_total:,.2f}</b> &nbsp;|&nbsp; "
                    f"Transactions: <b>{len(dept_df)}</b> &nbsp;|&nbsp; "
                    f"High-Risk Flags: <b>{len(dept_hr)}</b>",
                    S["body"],
                )
            )

            # Dept spend bar chart by service_type
            if "service_type" in dept_df.columns:
                st_spend = (
                    dept_df.groupby("service_type")["actual_spend"]
                    .sum().sort_values(ascending=False).head(8)
                )
                if not st_spend.empty:
                    fig, ax = plt.subplots(figsize=(6, 2.5), facecolor="#FAFAF8")
                    bars = ax.bar(
                        range(len(st_spend)), st_spend.values,
                        color=_PDF_PALETTE[:len(st_spend)],
                    )
                    ax.set_xticks(range(len(st_spend)))
                    ax.set_xticklabels(
                        [s[:20] for s in st_spend.index], rotation=30,
                        ha="right", fontsize=7,
                    )
                    ax.set_title(f"{dept} – Spend by Service Type",
                                 fontsize=9, fontweight="bold")
                    ax.tick_params(labelsize=7)
                    fig.tight_layout()
                    elements.append(rl_image(fig, width=400, height=190))

            # Dept transaction table (top 20)
            cols_show = ["transaction_date", "vendor", "service_type",
                         "actual_spend", "planned_budget", "risk_level"]
            cols_show = [c for c in cols_show if c in dept_df.columns]
            preview   = dept_df.sort_values("actual_spend", ascending=False).head(20)

            tdata = [[Paragraph(f"<b>{c.replace('_',' ').title()}</b>", S["small"])
                      for c in cols_show]]
            for _, row in preview.iterrows():
                trow = []
                for c in cols_show:
                    v = row[c]
                    if c in ("actual_spend", "planned_budget"):
                        cell = Paragraph(f"₹{float(v):,.0f}", S["small"])
                    elif c == "risk_level":
                        cell = risk_badge(str(v))
                    elif c == "transaction_date":
                        cell = Paragraph(str(v)[:10], S["small"])
                    else:
                        cell = Paragraph(str(v)[:28], S["small"])
                    trow.append(cell)
                tdata.append(trow)

            col_w = [(_W - 90) / len(cols_show)] * len(cols_show)
            dtable = Table(tdata, colWidths=col_w, repeatRows=1)
            dtable.setStyle(TableStyle([
                ("BACKGROUND",    (0, 0), (-1, 0),  NAVY),
                ("TEXTCOLOR",     (0, 0), (-1, 0),  colors.white),
                ("ROWBACKGROUNDS",(0, 1), (-1, -1), [colors.white, LIGHT_BG]),
                ("GRID",          (0, 0), (-1, -1), 0.25, MID_GREY),
                ("TOPPADDING",    (0, 0), (-1, -1), 3),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
                ("FONTSIZE",      (0, 0), (-1, -1), 8),
            ]))
            elements.append(Spacer(1, 6))
            elements.append(dtable)
            elements.append(Spacer(1, 10))
            elements.append(divider(MID_GREY, 0.3))

    elements.append(PageBreak())

    # ─────────────────────────────────────────────────────────────────────────
    #  HIGH-RISK TRANSACTION LOGS (paginated)
    # ─────────────────────────────────────────────────────────────────────────
    elements.append(Paragraph("6. High-Risk Transaction Logs", S["h1"]))
    elements.append(divider())
    elements.append(
        Paragraph(
            f"The following {hr_count} transaction(s) were classified as "
            f"<b>HIGH RISK</b> by the AI engine. Estimated total leakage exposure: "
            f"<b>₹{leakage:,.2f}</b>.",
            S["body"],
        )
    )
    elements.append(Spacer(1, 8))

    all_hr = summary.get("all_high_risks", [])
    LOG_COLS = ["department", "vendor", "service_type",
                "actual_spend", "planned_budget", "overrun_ratio", "risk_level"]

    CHUNK = 30
    for chunk_start in range(0, len(all_hr), CHUNK):
        chunk = all_hr[chunk_start:chunk_start + CHUNK]
        avail_cols = [c for c in LOG_COLS if any(c in r for r in chunk)]

        tdata = [[Paragraph(f"<b>{c.replace('_',' ').title()}</b>", S["small"])
                  for c in avail_cols]]
        for r in chunk:
            trow = []
            for c in avail_cols:
                v = r.get(c, "")
                if c in ("actual_spend", "planned_budget"):
                    cell = Paragraph(f"₹{float(v or 0):,.0f}", S["small"])
                elif c == "overrun_ratio":
                    cell = Paragraph(f"{float(v or 0):.2f}×", S["small"])
                elif c == "risk_level":
                    cell = risk_badge(str(v))
                else:
                    cell = Paragraph(str(v)[:30], S["small"])
                trow.append(cell)
            tdata.append(trow)

        col_w = [(_W - 90) / len(avail_cols)] * len(avail_cols)
        lt = Table(tdata, colWidths=col_w, repeatRows=1)
        lt.setStyle(TableStyle([
            ("BACKGROUND",    (0, 0), (-1, 0),  colors.HexColor("#A32D2D")),
            ("TEXTCOLOR",     (0, 0), (-1, 0),  colors.white),
            ("FONTNAME",      (0, 0), (-1, 0),  "Helvetica-Bold"),
            ("ROWBACKGROUNDS",(0, 1), (-1, -1), [colors.white, colors.HexColor("#FCEBEB")]),
            ("GRID",          (0, 0), (-1, -1), 0.25, colors.HexColor("#F09595")),
            ("TOPPADDING",    (0, 0), (-1, -1), 3),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
            ("FONTSIZE",      (0, 0), (-1, -1), 8),
        ]))
        elements.append(lt)
        elements.append(Spacer(1, 8))
        if chunk_start + CHUNK < len(all_hr):
            elements.append(PageBreak())

    elements.append(PageBreak())

    # ─────────────────────────────────────────────────────────────────────────
    #  TREND VISUALISATIONS
    # ─────────────────────────────────────────────────────────────────────────
    elements.append(Paragraph("7. Trend and Visualisation Analysis", S["h1"]))
    elements.append(divider())

    # 7A – Top vendors by flagged spend
    g2_labels = json.loads(summary.get("g2_vendor_labels", "[]"))
    g2_data   = json.loads(summary.get("g2_vendor_data", "[]"))
    if g2_labels and g2_data:
        fig, ax = plt.subplots(figsize=(6.5, 3), facecolor="#FAFAF8")
        bars = ax.barh(g2_labels[::-1], g2_data[::-1],
                       color=_PDF_PALETTE[:len(g2_labels)])
        ax.set_title("Top Vendors by Flagged Spend", fontsize=10, fontweight="bold")
        ax.set_xlabel("₹ Actual Spend", fontsize=8)
        ax.tick_params(labelsize=8)
        for bar in bars:
            ax.text(bar.get_width() * 1.01, bar.get_y() + bar.get_height() / 2,
                    f"₹{bar.get_width():,.0f}", va="center", fontsize=7)
        fig.tight_layout()
        elements.append(Paragraph("Top Vendors by Flagged Spend", S["h2"]))
        elements.append(rl_image(fig, width=470, height=220))
        elements.append(Spacer(1, 10))

    # 7B – Monthly spend trend
    g5_labels = json.loads(summary.get("g5_temp_labels", "[]"))
    g5_data   = json.loads(summary.get("g5_temp_data", "[]"))
    if g5_labels and g5_data:
        fig, ax = plt.subplots(figsize=(6.5, 2.8), facecolor="#FAFAF8")
        ax.bar(g5_labels, g5_data, color="#1D9E75", alpha=0.85)
        ax.set_title("Monthly Actual Spend", fontsize=10, fontweight="bold")
        ax.set_xlabel("Month", fontsize=8)
        ax.set_ylabel("₹", fontsize=8)
        ax.tick_params(labelsize=8)
        fig.tight_layout()
        elements.append(Paragraph("Monthly Spend Trend", S["h2"]))
        elements.append(rl_image(fig, width=470, height=200))

    # 7C – Department spend breakdown
    g7_labels = json.loads(summary.get("g7_dept_labels", "[]"))
    g7_data   = json.loads(summary.get("g7_dept_data", "[]"))
    if g7_labels and g7_data:
        fig, ax = plt.subplots(figsize=(6.5, 3.2), facecolor="#FAFAF8")
        ax.barh(g7_labels[::-1][:12], g7_data[::-1][:12],
                color="#7F77DD", alpha=0.85)
        ax.set_title("Spend by Department", fontsize=10, fontweight="bold")
        ax.set_xlabel("₹ Actual Spend", fontsize=8)
        ax.tick_params(labelsize=8)
        fig.tight_layout()
        elements.append(Paragraph("Spend by Department", S["h2"]))
        elements.append(rl_image(fig, width=470, height=230))

    elements.append(PageBreak())

    # ─────────────────────────────────────────────────────────────────────────
    #  FINDINGS AND RECOMMENDATIONS
    # ─────────────────────────────────────────────────────────────────────────
    elements.append(Paragraph("8. Findings and Recommendations", S["h1"]))
    elements.append(divider())

    findings = [
        ("F-01", "Budget Discipline",
         f"The portfolio {'has exceeded' if breach else 'is approaching'} the "
         f"approved limit. Over-budget ratio: {ratio:.2f}×. "
         "Action: Immediate spend freeze on non-critical services.",
         breach),
        ("F-02", "Anomalous Vendor Spend",
         f"{len(g2_labels)} vendors account for a disproportionate share of "
         f"flagged transactions. Action: Commission independent vendor audits and "
         "renegotiate contract terms.",
         len(g2_labels) > 0),
        ("F-03", "Temporal Spike Events",
         "One or more months exhibit spend spikes exceeding 1.5× the monthly "
         "average, suggesting batch processing of backdated invoices or split-"
         "purchase circumvention. Action: Enforce real-time invoice matching.",
         True),
        ("F-04", "Unapproved Transactions",
         "Transactions flagged with non-standard approval statuses show elevated "
         "overrun ratios. Action: Re-route to dual-approval workflow.",
         True),
    ]

    for code, title, text, active in findings:
        color = RED if active else DARK_TEAL
        elements.append(
            Paragraph(
                f'<font color="#{color.hexval()[2:]}"><b>[{code}] {title}</b></font>',
                S["h3"],
            )
        )
        elements.append(Paragraph(text, S["body"]))
        elements.append(Spacer(1, 4))

    elements.append(PageBreak())

    # ─────────────────────────────────────────────────────────────────────────
    #  CONCLUSION
    # ─────────────────────────────────────────────────────────────────────────
    elements.append(Paragraph("9. Auditor Conclusion", S["h1"]))
    elements.append(divider())

    overall_risk = (
        "HIGH RISK" if (breach or hr_count > summary.get("total_records", 1) * 0.15)
        else "MODERATE RISK"
    )
    risk_color   = RED if overall_risk == "HIGH RISK" else AMBER

    elements.append(
        Paragraph(
            f'Overall Portfolio Classification: '
            f'<font color="#{risk_color.hexval()[2:]}"><b>{overall_risk}</b></font>',
            S["h2"],
        )
    )
    elements.append(
        Paragraph(
            f"Based on {summary.get('total_records', 0)} transactions analysed for "
            f"{company} during {period}, the AI audit engine has determined the "
            f"portfolio risk level to be <b>{overall_risk}</b>. "
            f"A total of {hr_count} transactions require immediate management "
            f"intervention. Estimated financial exposure from high-risk items is "
            f"₹{leakage:,.2f}.",
            S["body"],
        )
    )
    elements.append(Spacer(1, 0.8 * inch))
    elements.append(Paragraph("___________________________", S["body"]))
    elements.append(Paragraph("<b>AI Certified Lead Auditor</b>", S["body"]))
    elements.append(Paragraph(f"Report ID: {report_id}", S["small"]))
    elements.append(Paragraph(f"Issued: {generated_at}", S["small"]))
    elements.append(PageBreak())

    # ─────────────────────────────────────────────────────────────────────────
    #  APPENDIX
    # ─────────────────────────────────────────────────────────────────────────
    elements.append(Paragraph("Appendix A — Legal Disclaimer", S["h1"]))
    elements.append(divider())
    elements.append(
        Paragraph(
            "This report is generated by an automated AI financial audit system. "
            "While the system employs statistically rigorous methods, all findings "
            "must be validated by a qualified human auditor before being used as the "
            "basis for legal, regulatory, or disciplinary action. The anomaly scores "
            "and risk classifications are probabilistic in nature and do not constitute "
            "definitive evidence of fraud or malfeasance. "
            "Anthropic or the system operator accepts no liability for decisions made "
            "solely on the basis of this report.",
            S["body"],
        )
    )
    elements.append(Spacer(1, 12))
    elements.append(Paragraph("Appendix B — Feature Glossary", S["h1"]))
    glossary = [
        ["Term", "Definition"],
        ["overrun_ratio",   "actual_spend / planned_budget. Values > 1 indicate overspend."],
        ["anomaly_score",   "Normalised isolation-forest score (0-100). Higher = more anomalous."],
        ["temporal_spike",  "True when actual_spend > 1.5× the monthly cohort average."],
        ["impact_score",    "actual_spend / portfolio mean spend. Measures absolute impact."],
        ["risk_confidence", "Composite score (0-100) combining overrun, anomaly, and spike signals."],
    ]
    gt = Table(glossary, colWidths=[130, 365])
    gt.setStyle(TableStyle([
        ("BACKGROUND",    (0, 0), (-1, 0),  NAVY),
        ("TEXTCOLOR",     (0, 0), (-1, 0),  colors.white),
        ("FONTNAME",      (0, 0), (-1, 0),  "Helvetica-Bold"),
        ("ROWBACKGROUNDS",(0, 1), (-1, -1), [colors.white, LIGHT_BG]),
        ("GRID",          (0, 0), (-1, -1), 0.3, MID_GREY),
        ("FONTSIZE",      (0, 0), (-1, -1), 9),
        ("TOPPADDING",    (0, 0), (-1, -1), 5),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
    ]))
    elements.append(gt)

    # ── Build ─────────────────────────────────────────────────────────────────
    doc.build(elements)
    buffer.seek(0)

    safe_name = "".join(c if c.isalnum() or c in "._- " else "_" for c in company)
    filename  = f"Audit_{safe_name}_{datetime.utcnow().strftime('%Y%m%d')}.pdf"
    return FileResponse(buffer, as_attachment=True, filename=filename,
                        content_type="application/pdf")