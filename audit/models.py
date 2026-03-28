from django.db import models

# Create your models here.
class Anomaly(models.Model):
    category = models.CharField(max_length=20)  # employee / department / goods
    label = models.CharField(max_length=100)
    score = models.FloatField()
    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f"{self.category} - {self.label} ({self.score})"