import matplotlib.pyplot as plt
import os

def generate_charts(results):
    paths = []

    os.makedirs("media/charts", exist_ok=True)

    # Employee
    if results["employee"]:
        scores = [r["risk_score"] for r in results["employee"]]
        plt.figure()
        plt.hist(scores)
        path = "media/charts/employee.png"
        plt.savefig(path)
        plt.close()
        paths.append(path)

    # Department
    if results["department"]:
        scores = [r["anomaly_score"] for r in results["department"]]
        plt.figure()
        plt.hist(scores)
        path = "media/charts/department.png"
        plt.savefig(path)
        plt.close()
        paths.append(path)

    # Goods
    if results["goods"]:
        scores = [r["raw_score"] for r in results["goods"]]
        plt.figure()
        plt.hist(scores)
        path = "media/charts/goods.png"
        plt.savefig(path)
        plt.close()
        paths.append(path)

    return paths