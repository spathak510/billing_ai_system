"""
Demo runner — processes data/sample_billing.csv through the full pipeline
and saves output Excel files to output/.

Run with:
    .\\venv\\Scripts\\python run_demo.py
"""
from __future__ import annotations

import logging
import os

import pandas as pd

from app.config.settings import settings
from app.processing.pipeline import BillingPipeline

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
)

SOURCE_FILE = os.path.join(settings.upload_dir, "sample_billing.csv")

if not os.path.exists(SOURCE_FILE):
    raise FileNotFoundError(f"Sample file not found: {SOURCE_FILE}")

os.makedirs(settings.output_dir, exist_ok=True)

df = pd.read_csv(SOURCE_FILE)
pipeline = BillingPipeline()
result = pipeline.run(df, "sample_billing.csv")

print("\n=== Pipeline Results ===")
print(f"  Total rows   : {result['total_rows']}")
print(f"  Zero rows    : {result['zero_rows']}")
print(f"  CORP rows    : {result['corp_rows']}")
print(f"  NON-CORP rows: {result['non_corp_rows']}")
print("\n=== Output Files ===")
print(f"  Zero data : {result['zero_data_path']}")
print(f"  CORP paid : {result['corp_paid_path']}")
print(f"  NON-CORP  : {result['non_corp_paid_path']}")
print("\nOpen the files in the output/ folder to review the results.")
