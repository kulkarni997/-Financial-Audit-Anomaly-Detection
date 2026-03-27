from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Image, Table, TableStyle
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet

def generate_pdf(results, summary, charts):
    doc = SimpleDocTemplate("media/report.pdf")
    styles = getSampleStyleSheet()

    elements = []

    # Title
    elements.append(Paragraph(" Audit Report", styles["Title"]))
    elements.append(Spacer(1, 12))

    # Summary
    elements.append(Paragraph("AI Summary", styles["Heading2"]))
    elements.append(Paragraph(summary, styles["BodyText"]))
    elements.append(Spacer(1, 12))

    # Charts
    for chart in charts:
        elements.append(Image(chart, width=400, height=200))
        elements.append(Spacer(1, 10))

    # Employee Table
    if results["employee"]:
        elements.append(Paragraph("Employee Anomalies", styles["Heading2"]))

        data = [["Employee", "Risk Score"]]
        for r in results["employee"]:
            data.append([r.get("emp_id_original"), r.get("risk_score")])

        table = Table(data)

        style = TableStyle([
            ('BACKGROUND', (0,0), (-1,0), colors.grey)
        ])

        # Highlight critical
        for i, r in enumerate(results["employee"], start=1):
            if r.get("risk_score", 0) < -0.1:
                style.add('BACKGROUND', (0,i), (-1,i), colors.red)

        table.setStyle(style)

        elements.append(table)

    doc.build(elements)

    return "media/report.pdf"