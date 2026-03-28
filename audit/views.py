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
import google.generativeai as genai

# 5. LOCAL ML BUSINESS LOGIC
from fraud_detection.emp_fraud_predictor import process_employee_audit
from fraud_detection.dept_fraud_predictor import process_department_audit
from fraud_detection.goods_fraud_predictor import process_goods_audit


# --- CONFIGURATION ---
logger = logging.getLogger(__name__)
api_key = os.getenv("GENAI_API_KEY")
genai.configure(api_key=api_key)

def generate_all_summaries(results: dict) -> dict:
    """
    Final optimized AI summary generator for 2026.
    Fixes NameError, 404 model errors, and improves JSON parsing.
    """
    
    # Updated stable model strings for 2026
    models_to_try = [
    "models/gemini-2.5-flash", 
    "models/gemini-2.5-pro-latest"
]

    # Counts for the prompt
    emp_count = len(results.get('employee', []))
    dept_count = len(results.get('department', []))
    goods_count = len(results.get('goods', []))

    prompt = (
        "You are a Senior  Auditor. Analyze these anomaly counts and provide "
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
            model = genai.GenerativeModel(model_name=model_name)
            # Use generation_config to force JSON if the SDK version supports it
            response = model.generate_content(
                prompt, 
                generation_config={"response_mime_type": "application/json"}
            )

            if response and response.text:
                # Remove markdown formatting if the AI ignores the 'JSON only' instruction
                clean_json = response.text.strip().removeprefix("```json").removesuffix("```").strip()
                parsed = json.loads(clean_json)
                
                return {
                    "employee_summary": parsed.get("employee_summary", "Detailed report pending."),
                    "department_summary": parsed.get("department_summary", "Detailed report pending."),
                    "goods_summary": parsed.get("goods_summary", "Detailed report pending.")
                }
                
        except Exception as e:
            logger.warning(f"Model {model_name} failed or returned invalid JSON: {e}")

    # Fallback Hard-coded Summaries (if all AI attempts fail)
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
