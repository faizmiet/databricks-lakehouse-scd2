# Databricks Lakehouse SCD2 - Local Demo

This is a local-demo scaffold for a Databricks-style Lakehouse project:
- Local PySpark + Delta smoke test: `src/local_delta_demo.py`
- Requirements in `requirements.txt`
- Project scaffold: `src/`, `notebooks/`, `tests/`, `data/`

Run steps (on Mac):
1. Create and activate venv:
   python3 -m venv .venv
   source .venv/bin/activate
2. Install dependencies:
   pip install -r requirements.txt
3. Run smoke test:
   python src/local_delta_demo.py

Do not commit secrets to this repo. Use Keychain or environment variables for credentials.
