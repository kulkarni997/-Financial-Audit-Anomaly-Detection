from django.contrib import admin
from django.urls import path
from . import views
from rest_framework_simplejwt.views import TokenObtainPairView

urlpatterns = [
    # Pages
    path('dashboard/', views.dashboard, name='dashboard'),
    path('upload/', views.upload, name='upload'),
    path('anomalies/', views.anomalies, name='anomalies'),
    path('audits/', views.audits, name='audits'),
    path('reports/', views.reports, name='reports'),
    path('settings/', views.settings_view, name='settings'),
    path('api/auth/login/', TokenObtainPairView.as_view()),
    # API - Upload
    path('api/upload/', views.api_upload_file, name='api_upload'),
    path('api/uploads/', views.api_get_uploads, name='api_uploads'),
    
    # API - Anomalies
    path('api/anomalies/', views.api_get_anomalies, name='api_anomalies'),
    path('api/anomalies/stats/', views.api_anomaly_stats, name='api_anomaly_stats'),
    path('api/anomalies/detect/', views.api_detect_anomalies, name='api_detect_anomalies'),
    
    # API - Audit History
    path('api/audit-history/', views.api_audit_history, name='api_audit_history'),
    path('api/audit-stats/', views.api_audit_stats, name='api_audit_stats'),
    
    # API - Auth
    path('api/auth/logout/', views.api_logout, name='api_logout'),
]