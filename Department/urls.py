from django.urls import path
from .views import audit_dashboard

urlpatterns = [
    path('audit/', audit_dashboard, name='audit_dashboard'),
]