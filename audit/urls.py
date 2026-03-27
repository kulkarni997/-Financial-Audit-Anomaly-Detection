# audit/urls.py
from django.urls import path
from . import views

urlpatterns = [
    # Pages
    path('dashboard/', views.dashboard, name='dashboard'),
    path('upload/', views.upload_zip, name='upload'),
    path('anomalies/', views.anomalies, name='anomalies'),

    # APIs
    path('api/upload/', views.upload_zip, name='api_upload'),
    path('api/uploads/', views.api_get_uploads, name='api_get_uploads'),
    path('api/dashboard/summary/', views.dashboard_summary, name='dashboard_summary'),
]