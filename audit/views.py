import os
import json
import logging
import tempfile
import pandas as pd
from pathlib import Path

# Django & REST Framework
from django.shortcuts import render, redirect
from django.http import JsonResponse, FileResponse
from django.utils.safestring import mark_safe
from rest_framework.decorators import api_view
from rest_framework.response import Response
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, PageBreak
from reportlab.lib.enums import TA_CENTER, TA_LEFT


# AI & PDF Generation
import google.generativeai as genai
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib.pagesizes import A4

# Local ML Business Logic
from fraud_detection.emp_fraud_predictor import process_employee_audit
from fraud_detection.dept_fraud_predictor import process_department_audit
from fraud_detection.goods_fraud_predictor import process_goods_audit
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle
from reportlab.lib import colors

# --- CONFIGURATION ---
logger = logging.getLogger(__name__)
genai.configure(api_key="AIzaSyCufq3I7g_NrGxvLx76Y1AgBN5nfSG_zt0")

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

# --- 2. ADVANCED PDF GENERATION ---
def dashboard(request):
    return render(request, 'Dashboard.html')

def draw_page_border(canvas, doc):
    canvas.saveState()
    canvas.setStrokeColor(colors.HexColor("#1A237E")) # Navy Border
    canvas.setLineWidth(1.5)
    canvas.rect(25, 25, A4[0]-50, A4[1]-50)
    # Branding Accent
    canvas.setFillColor(colors.HexColor("#FF6D00")) # Orange Accent
    canvas.rect(25, A4[1]-40, 120, 15, fill=1, stroke=0)
    canvas.restoreState()

def generate_pdf_report(results, summaries):
    os.makedirs("media", exist_ok=True)
    pdf_path = "media/AuditAI_Internal_Report.pdf"
    
    doc = SimpleDocTemplate(pdf_path, pagesize=A4, rightMargin=45, leftMargin=45, topMargin=55, bottomMargin=45)
    styles = getSampleStyleSheet()

    # --- TYPEWRITER & PRO STYLES ---
    typewriter = ParagraphStyle('Type', fontName='Courier-Bold', fontSize=10, textColor=colors.HexColor("#455A64"))
    title_style = ParagraphStyle('Title', fontName='Helvetica-Bold', fontSize=26, textColor=colors.HexColor("#1A237E"), alignment=TA_CENTER)
    alert_box = ParagraphStyle('Alert', fontName='Courier-Bold', fontSize=11, textColor=colors.white, backColor=colors.HexColor("#FF6D00"), borderPadding=6, alignment=TA_CENTER)

    elements = []

    # Metadata / Cover Page
    elements.append(Spacer(1, 30))
    elements.append(Paragraph("AuditAI SYSTEM REPORT", title_style))
    elements.append(Spacer(1, 20))
    
    meta_table = Table([
        ["GEN_TYPE", "SYSTEM_SURGERY_ANOMALY_DETECTION"],
        ["VERSION", "v2.0.4-BETA"],
        ["SECURITY", "ENCRYPTED_INTERNAL"],
        ["VERIFY", "[ QR_AUTH_PENDING ]"]
    ], colWidths=[100, 300])
    
    meta_table.setStyle(TableStyle([
        ('FONTNAME', (0,0), (-1,-1), 'Courier-Bold'),
        ('GRID', (0,0), (-1,-1), 0.5, colors.lightgrey),
        ('BACKGROUND', (0,0), (0,-1), colors.whitesmoke),
        ('TEXTCOLOR', (1,3), (1,3), colors.HexColor("#FF6D00")),
    ]))
    
    elements.append(meta_table)
    elements.append(Spacer(1, 40))
    elements.append(Paragraph("CRITICAL ANALYSIS SUMMARY", alert_box))
    elements.append(Spacer(1, 10))
    elements.append(Paragraph(summaries.get('global', 'No global summary provided.'), typewriter))
    
    elements.append(PageBreak())

    # --- DATA TABLES ---
    for title, key, cols, headers in [("USER ANOMALIES", "employee", ["emp_id_original", "risk_score"], ["ID", "RISK"])]:
        elements.append(Paragraph(f"// ACCESSING DATA NODE: {title}", typewriter))
        elements.append(Spacer(1, 10))
        
        data = [headers]
        for item in results.get(key, []):
            data.append([str(item.get(c, "")) for c in cols])
        
        if len(data) > 1:
            t = Table(data, hAlign='LEFT', colWidths=[300, 120])
            t_style = [
                ('BACKGROUND', (0,0), (-1,0), colors.HexColor("#1A237E")),
                ('TEXTCOLOR', (0,0), (-1,0), colors.white),
                ('FONTNAME', (0,0), (-1,-1), 'Courier-Bold'),
                ('GRID', (0,0), (-1,-1), 1, colors.black),
            ]
            
            # ORANGE HIGHLIGHT LOGIC
            for i in range(1, len(data)):
                try:
                    if float(data[i][1]) > 0.5: # Threshold
                        t_style.append(('BACKGROUND', (0,i), (-1,i), colors.HexColor("#FF6D00")))
                        t_style.append(('TEXTCOLOR', (0,i), (-1,i), colors.white))
                except: pass
                
            t.setStyle(TableStyle(t_style))
            elements.append(t)

    doc.build(elements, onFirstPage=draw_page_border, onLaterPages=draw_page_border)
    return pdf_path

# --- 3. AUDIT VIEWS ---
def upload_zip(request):
    """Handles multi-file uploads and routes them to specific ML predictors."""
    if request.method == "POST" and request.FILES.getlist("files"):
        uploaded_files = request.FILES.getlist("files")
        results = {"employee": [], "department": [], "goods": []}
        
        for file in uploaded_files:
            try:
                # Use context manager for auto-cleanup of temp files
                with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as temp:
                    for chunk in file.chunks():
                        temp.write(chunk)
                    temp_path = temp.name

                file_name = file.name.lower()
                if any(x in file_name for x in ["employee", "emp"]):
                    res = process_employee_audit(temp_path, pd.DataFrame())
                    results["employee"].extend(res.to_dict("records"))
                elif any(x in file_name for x in ["department", "dept"]):
                    res = process_department_audit(temp_path)
                    results["department"].extend(res.to_dict("records"))
                elif any(x in file_name for x in ["goods", "good"]):
                    res = process_goods_audit(temp_path)
                    results["goods"].extend(res.to_dict("records"))
                
                os.unlink(temp_path) # Cleanup
            except Exception as e:
                logger.error(f"File processing error: {file.name} - {e}")

        request.session["results"] = results
        return redirect("anomalies")
    
    return render(request, "upload.html")

def anomalies(request):
    """Dashboard view providing both tables and Chart.js graphs."""
    results = request.session.get("results", {"employee": [], "department": [], "goods": []})

    emp_list = results.get("employee", [])
    dept_list = results.get("department", [])
    goods_list = results.get("goods", [])

    # Optional: basic stats for cards
    stats = {
        "total": len(emp_list) + len(dept_list) + len(goods_list),
        "critical": sum(1 for r in emp_list if float(r.get("risk_score", 0)) < -0.1),
        "high": sum(1 for r in emp_list if -0.1 <= float(r.get("risk_score", 0)) < -0.05),
        "medium": sum(1 for r in emp_list if -0.05 <= float(r.get("risk_score", 0)) < 0),
        "low": sum(1 for r in emp_list if float(r.get("risk_score", 0)) >= 0),
    }

    # Context for template: tables and chart.js
    context = {
        **stats,
        "results": results,  # this provides tables: results.employee, results.department, results.goods

        # Chart.js variables (match template)
        "employee_labels": mark_safe(json.dumps([r.get("emp_id_original") for r in emp_list])),
        "employee_scores": mark_safe(json.dumps([float(r.get("risk_score", 0)) for r in emp_list])),
        "department_labels": mark_safe(json.dumps([r.get("department_original") for r in dept_list])),
        "department_scores": mark_safe(json.dumps([float(r.get("anomaly_score", 0)) for r in dept_list])),
        "goods_labels": mark_safe(json.dumps([r.get("product_name") for r in goods_list])),
        "goods_scores": mark_safe(json.dumps([float(r.get("raw_score", 0)) for r in goods_list])),
    }

    return render(request, "anomalies.html", context)

def show_report(request):
    results = request.session.get("results", {})
    summaries = generate_all_summaries(results)
    
    # Ensure the PDF is updated with the latest data
    generate_pdf_report(results, summaries)

    context = {
        "results": results,
        "employee_summary": summaries.get("employee_summary"),
        "department_summary": summaries.get("department_summary"),
        "goods_summary": summaries.get("goods_summary"),
        # Add these for the charts in the HTML
        "employee_json": mark_safe(json.dumps([r.get("emp_id_original") for r in results.get("employee", [])])),
        "emp_scores_json": mark_safe(json.dumps([r.get("risk_score") for r in results.get("employee", [])])),
        "dept_json": mark_safe(json.dumps([r.get("department_original") for r in results.get("department", [])])),
        "dept_scores_json": mark_safe(json.dumps([r.get("anomaly_score") for r in results.get("department", [])])),
    }
    return render(request, "audit_report.html", context)

def download_report(request):
    """Secure file response for the generated PDF."""
    path = "media/audit_report.pdf"
    if os.path.exists(path):
        return FileResponse(open(path, 'rb'), as_attachment=True, filename='Audit.pdf')
    return JsonResponse({"error": "Report not found. Please run analysis first."}, status=404)

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