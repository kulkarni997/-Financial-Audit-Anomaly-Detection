# audit/forms.py
from django import forms

class ProjectAuditForm(forms.Form):
    project_name = forms.CharField(max_length=100)
    project_type = forms.CharField(max_length=50)
    department = forms.CharField(max_length=50)
    budget_cycle = forms.CharField(max_length=20)
    total_planned_budget = forms.IntegerField()
    budget_flexibility = forms.ChoiceField(choices=[("Strict","Strict"),("Moderate","Moderate"),("Flexible","Flexible")])
    file = forms.FileField()
