from django.urls import path
from . import views

urlpatterns = [
    path('dashboard/', views.dashboard, name='dashboard'),
             # Dashboard page
    path('upload/', views.upload_zip, name='upload'),           # Upload CSVs
    path('anomalies/', views.anomalies, name='anomalies'),      # Show anomalies & charts
    path('report/', views.show_report, name='report'),          # Single report view
    path('reports/', views.show_report, name='reports'),        # Alias for /reports/
    path('download-report/', views.download_report, name='download_report'),
    path('api/uploads/', views.api_get_uploads, name='api_uploads'),
    path('api/dashboard-summary/', views.dashboard_summary, name='dashboard_summary'),
]