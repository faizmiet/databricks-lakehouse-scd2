# Databricks Lakehouse SCD2 Project — Customer Domain

This is a full end-to-end **Databricks-style Delta Lakehouse project** implemented locally using PySpark + Delta Lake.
It demonstrates a complete **Bronze → Silver → Gold** data engineering pipeline with **Slowly Changing Dimension Type 2 (SCD2)**.

---

## 📚 Layers Overview

### ✔ Bronze  
Ingest raw JSON customer events  
Add ingestion_date  
Store as Delta Bronze table

### ✔ Silver (SCD Type 2)  
Apply change detection on customer attributes  
Maintain:
- effective_from  
- effective_to  
- is_current  
- created_ts  
- updated_ts  

### ✔ Gold  
Compute KPIs  
Example: active_customer_count

---

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

---

## 🛠 Local Setup

### Activate virtual environment:
source .venv/bin/activate

### Install requirements:
pip install -r requirements.txt

### Configure Delta Lake packages:
export PYSPARK_SUBMIT_ARGS="--packages io.delta:delta-core_2.12:2.4.0 pyspark-shell"

---

## ▶️ Run the Pipeline

### 1) Bronze ingestion
python src/bronze_ingest.py

### 2) Silver SCD2 processing
python src/silver_scd2.py

### 3) Gold KPI generation
python src/gold_kpis.py

---

## 📦 Raw JSON Input Example

Place sample JSON files in:  
`./data/raw/customer/`

Example:
{"customer_id":"c1","name":"Alice","email":"alice@example.com","address":"addr1","event_time":"2025-11-17T05:00:00"}

To simulate change:
{"customer_id":"c1","name":"Alice","email":"alice@example.com","address":"addr2","event_time":"2025-11-18T05:00:00"}

---

## 🧪 Run Unit Tests
pytest -q

---

## 🎤 Interview Talking Points

### Why Delta Lake?
- ACID transactions  
- MERGE support (critical for SCD2)  
- Time travel  
- Schema evolution  
- Efficient upserts  

### SCD2 Logic:
- Compare tracked columns  
- Close old record (set is_current=false, effective_to timestamp)  
- Insert new version (is_current=true)  

### Production Adaptation:
- Replace local paths with DBFS or S3  
- Use Databricks Autoloader for Bronze  
- Use Databricks Jobs to orchestrate  
- Optimize tables using OPTIMIZE and ZORDER  

---

✨ This project is now fully interview-ready and portfolio-ready.
