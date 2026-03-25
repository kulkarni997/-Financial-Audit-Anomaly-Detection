from django.urls import path
from . import views

urlpatterns = [
    path('', views.dashboard, name='dashboard'),
    path('dashboard/', views.dashboard, name='dashboard'),
    path('upload/', views.upload, name='upload'),
    path('anomalies/', views.anomalies, name='anomalies'),
    path('audits/', views.audits, name='audits'),
    path('reports/', views.reports, name='reports'),
    path('settings/', views.settings_view, name='settings'),
]