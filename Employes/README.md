# AuditAI Machine Learning Integration

This repository contains the prediction pipeline and integration steps for connecting pre-trained Machine Learning models into our existing Django project.

---

## 🛠 Integration Steps

### 1. Database Setup

The system requires a dedicated database to manage audit data and model outputs.

- **Create Database:** Create a new database named `AuditAI`.
- **Configuration:** Update your Django `settings.py` with the appropriate database engine, name, and credentials.

### 2. Data Ingestion & Migration

To populate the environment with the necessary historical data:

1.  Navigate to the `upload.py` script.
2.  **Update Credentials:** Modify the database connection strings in `upload.py` to match your local setup.
3.  **Execute Upload:** Run the script to ingest the **6-month historical dataset** into the `AuditAI` database.

### 3. Backend & ML Pipeline

The core logic is distributed across the following components:

- **Model Storage:** All trained models are stored as `.pkl` files in the `/Models` directory.
- **Logic:** The prediction pipeline is centralized within `views.py`. Ensure this logic correctly references the file paths for the pickle files.
- **Database Schema:** Update `models.py` to support the prediction outputs and run migrations:
  -- Run this command in the pgAdmin Query Tool for the AuditAI database:
  CREATE TABLE fraud_result (
  id SERIAL PRIMARY KEY,
  emp_id VARCHAR(50) NOT NULL,
  amount NUMERIC(15, 2),
  risk_score NUMERIC(5, 2)
  );

### 4. Frontend Implementation

The user interface must be updated to reflect the new AI-driven insights:

- Integrate prediction results into the dashboard.
- Ensure the frontend handles the data structures returned by the pipeline in `views.py`.

---

## Directory Structure (ML Specific)

- `/Models`: Contains all `.pkl` serialized model files.
- `upload.py`: Utility script for database initialization and data loading.
- `views.py`: Contains the active prediction pipeline and API logic.
