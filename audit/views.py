from django.shortcuts import render

def dashboard(request):
    return render(request, 'Dashboard.html')

def upload(request):
    return render(request, 'upload.html')  # Assuming there's an upload template

def anomalies(request):
    return render(request, 'anomalies.html')  # Assuming

def audits(request):
    return render(request, 'audits.html')  # Assuming

def reports(request):
    return render(request, 'reports.html')  # Assuming

def settings_view(request):
    return render(request, 'settings.html')  # Assuming
