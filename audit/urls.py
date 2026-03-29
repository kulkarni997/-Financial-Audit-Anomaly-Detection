from django.urls import path
from . import views

urlpatterns = [
    path('dashboard/', views.dashboard, name='dashboard'),          # Dashboard page
    path('upload/', views.upload_zip, name='upload'),               # Upload CSVs
    path('anomalies/', views.anomalies, name='anomalies'),          # Show anomalies & charts

    # Report views
    path('report/', views.show_report, name='report'),              # Single report view
    path('download-report/', views.download_report, name='download_report'),  # PDF download

    # API endpoints
    path('api/uploads/', views.api_get_uploads, name='api_uploads'),
    path('api/dashboard-summary/', views.dashboard_summary, name='dashboard_summary'),

    path("pro-dashboard/", views.pro_dashboard, name="pro_dashboard"),  # Pro Plan
    path("audit/project/", views.project_audit, name="project_audit"),
    path("audit/reimbursement/", views.reimbursement_audit, name="reimbursement_audit"),
    path("audit/approval/", views.approval_system, name="approval_system"),
    path('download-audit/', views.download_full_project_audit_pdf, name='download_full_project_audit_pdf'),
]
