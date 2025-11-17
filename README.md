# Databricks Lakehouse SCD2 Project — Customer Domain

This is a full end-to-end **Databricks-style Delta Lakehouse project** implemented locally using PySpark + Delta Lake.  
It demonstrates a complete **Bronze → Silver → Gold** data engineering pipeline with **Slowly Changing Dimension Type 2 (SCD2)**.

## 📚 Layers Overview

### ✔ Bronze
- Ingest raw JSON customer events  
- Add `ingest_date`  
- Store as Delta Bronze table  

### ✔ Silver (SCD Type 2)
- Apply change detection on customer attributes  
- Track:
  - `effective_from`  
  - `effective_to`  
  - `is_current`  
  - `created_ts`  
  - `updated_ts`  

### ✔ Gold
- Compute business KPIs  
- Example: `active_customer_count`  

## 📁 Project Structure

src/
 ├─ bronze_ingest.py
 ├─ scd_utils.py
 ├─ silver_scd2.py
 └─ gold_kpis.py
tests/
data/
requirements.txt
README.md

## 🛠 Local Setup

Activate virtual environment:
source .venv/bin/activate

Install requirements:
pip install -r requirements.txt

Configure Delta Lake packages:
export PYSPARK_SUBMIT_ARGS="--packages io.delta:delta-core_2.12:2.4.0 pyspark-shell"

## ▶️ Run the Pipeline

Bronze ingestion:
python src/bronze_ingest.py

Silver SCD2 processing:
python src/silver_scd2.py

Gold KPI generation:
python src/gold_kpis.py

## 📦 Raw JSON Input Example

Place files under:
./data/raw/customer/

Example:
{"customer_id":"c1","name":"Alice","email":"alice@example.com","address":"addr1","event_time":"2025-11-17T05:00:00"}

Example change:
{"customer_id":"c1","name":"Alice","email":"alice@example.com","address":"addr2","event_time":"2025-11-18T05:00:00"}

## 🧪 Run Unit Tests

pytest -q

