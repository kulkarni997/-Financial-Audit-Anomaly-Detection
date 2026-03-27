import os
import json
import pandas as pd
from pathlib import Path
from django.shortcuts import render, redirect
from django.http import JsonResponse
from django.utils.safestring import mark_safe
from rest_framework.decorators import api_view
from rest_framework.response import Response

# ML modules
from fraud_detection.emp_fraud_predictor import process_employee_audit
from fraud_detection.dept_fraud_predictor import process_department_audit
from fraud_detection.goods_fraud_predictor import process_goods_audit
from fraud_detection.zip_handler import handle_uploaded_zip

# ------------------- Upload ZIP -------------------
import tempfile

def save_temp_file(file):
    temp = tempfile.NamedTemporaryFile(delete=False, suffix=".csv")
    for chunk in file.chunks():
        temp.write(chunk)
    temp.close()
    return temp.name


def upload_zip(request):  # keep same name to avoid URL changes
    if request.method == "POST" and request.FILES.getlist("files"):

        uploaded_files = request.FILES.getlist("files")

        results = {
            "employee": [],
            "department": [],
            "goods": []
        }

        print("\n=== FILES RECEIVED ===")

        for file in uploaded_files:
            try:
                file_name = file.name.lower()
                print("\nProcessing:", file_name)

                # ✅ CRITICAL FIX: save file locally
                file_path = save_temp_file(file)

                # Debug
                df = pd.read_csv(file_path)
                print("SHAPE:", df.shape)
                print("COLUMNS:", list(df.columns))

                # 🔍 Detect file type
                if "employee" in file_name or "emp" in file_name:
                    print("→ EMPLOYEE DETECTED")
                    result_df = process_employee_audit(file_path, pd.DataFrame())
                    print("EMP RESULT:", result_df.shape)
                    results["employee"].extend(result_df.to_dict("records"))

                elif "department" in file_name or "dept" in file_name:
                    print("→ DEPARTMENT DETECTED")
                    result_df = process_department_audit(file_path)
                    print("DEPT RESULT:", result_df.shape)
                    results["department"].extend(result_df.to_dict("records"))

                elif "goods" in file_name or "good" in file_name:
                    print("→ GOODS DETECTED")
                    result_df = process_goods_audit(file_path)
                    print("GOODS RESULT:", result_df.shape)
                    results["goods"].extend(result_df.to_dict("records"))

                else:
                    print("⚠️ Unknown file:", file_name)

            except Exception as e:
                print("❌ ERROR:", file.name, e)

        print("\n=== FINAL COUNTS ===")
        print("EMP:", len(results["employee"]))
        print("DEPT:", len(results["department"]))
        print("GOODS:", len(results["goods"]))

        request.session["results"] = results
        request.session.modified = True

        return redirect("anomalies")

    return render(request, "upload.html")
#----
def dashboard(request):
    return render(request, "dashboard.html")


# ------------------- Anomalies -------------------
def anomalies(request):
    results = request.session.get("results", {
        "employee": [],
        "department": [],
        "goods": []
    })

    print("ANOMALIES VIEW RESULTS:", len(results["employee"]),
          len(results["department"]), len(results["goods"]))

    total = len(results["employee"]) + len(results["department"]) + len(results["goods"])

    # Risk buckets (employee only)
    critical = sum(1 for r in results["employee"] if r.get("risk_score", 0) < -0.1)
    high = sum(1 for r in results["employee"] if -0.1 <= r.get("risk_score", 0) < -0.05)
    medium = sum(1 for r in results["employee"] if -0.05 <= r.get("risk_score", 0) < 0)
    low = sum(1 for r in results["employee"] if r.get("risk_score", 0) >= 0)

    # Charts
    employee_labels = [r.get("emp_id_original") for r in results["employee"]]
    employee_scores = [r.get("risk_score") for r in results["employee"]]

    department_labels = [r.get("department_original") for r in results["department"]]
    department_scores = [r.get("anomaly_score") for r in results["department"]]

    goods_labels = [r.get("product_name") for r in results["goods"]]
    goods_scores = [r.get("raw_score") for r in results["goods"]]

    return render(request, "anomalies.html", {
        "results": results,
        "total": total,
        "critical": critical,
        "high": high,
        "medium": medium,
        "low": low,
        "employee_labels": mark_safe(json.dumps(employee_labels)),
        "employee_scores": mark_safe(json.dumps(employee_scores)),
        "department_labels": mark_safe(json.dumps(department_labels)),
        "department_scores": mark_safe(json.dumps(department_scores)),
        "goods_labels": mark_safe(json.dumps(goods_labels)),
        "goods_scores": mark_safe(json.dumps(goods_scores)),
    })


# ------------------- API: Uploads -------------------
def api_get_uploads(request):
    try:
        files = []
        uploads_dir = Path("media/uploads")

        if uploads_dir.exists():
            for file_path in sorted(uploads_dir.glob("*"), key=lambda p: p.stat().st_mtime, reverse=True):
                if not file_path.is_file():
                    continue

                stat = file_path.stat()
                files.append({
                    "name": file_path.name,
                    "size": stat.st_size,
                    "uploaded_at": pd.Timestamp.fromtimestamp(stat.st_mtime).isoformat(),
                })

        return JsonResponse({"files": files})
    except Exception as e:
        return JsonResponse({"error": str(e)}, status=500)


# ------------------- API: Dashboard Summary -------------------
@api_view(["GET"])
def dashboard_summary(request):
    data = {
        "total_transactions": 1000,
        "flagged_count": 120,
        "critical_count": 10,
        "avg_risk_score": 35,
    }
    return Response(data)