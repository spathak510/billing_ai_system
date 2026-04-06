from __future__ import annotations

import logging
import os
from glob import glob

import pandas as pd

from app.config.settings import settings
from app.agents.cleaning_agent import SampleBillingComparisonAgent
from app.agents.reporting_agent import ReportingAgent

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
)

SOURCE_FILE = os.path.join(settings.upload_dir, "sample_billing.csv")

if not os.path.exists(SOURCE_FILE):
    raise FileNotFoundError(f"Sample file not found: {SOURCE_FILE}")

os.makedirs(settings.output_dir, exist_ok=True)


def find_column(columns, candidates):
    normalized = {str(col).strip().upper(): col for col in columns}
    for candidate in candidates:
        if candidate.upper() in normalized:
            return normalized[candidate.upper()]
    return None


# STEP 1: Get comparison files
comparison_files = []
for pattern in ("*.xlsx", "*.xls", "*.csv"):
    comparison_files.extend(glob(os.path.join(settings.upload_dir, pattern)))

comparison_files = [
    path for path in comparison_files
    if os.path.basename(path).lower() not in {
        "sample_billing.xlsx",
        "sample_billing.xls",
        "sample_billing.csv",
    }
]

if not comparison_files:
    raise FileNotFoundError(
        f"No comparison Excel/CSV files found in upload_dir: {settings.upload_dir}"
    )

# STEP 2: Run comparison agent
comparison_agent = SampleBillingComparisonAgent()
filtered_output = comparison_agent.run(
    sample_billing_path=SOURCE_FILE,
    comparison_file_paths=comparison_files,
    output_path=os.path.join(settings.output_dir, "filtered_sample_billing.xlsx"),
)


output_path = os.path.join(settings.output_dir, "filtered_non_zero_data.xlsx")

print(f"Filtered file already saved at: {filtered_output}")

print(f"Filtered file saved at: {output_path}")


new_output_path = os.path.join(settings.output_dir, "filtered_non_zero_data.xlsx")
print("Full path:", os.path.abspath(new_output_path))

# STEP 3: Read output sheets from comparison agent
zero_df = pd.read_excel(filtered_output, sheet_name="Zero_Data")
filtered_non_zero_df = pd.read_excel(filtered_output, sheet_name="Filtered_Non_Zero_Data")

# STEP 4: Identify required columns from filtered non-zero data
user_type_col = find_column(
    filtered_non_zero_df.columns,
    ["USER TYPE", "USER_TYPE", "TYPE", "CATEGORY"]
)

region_col = find_column(
    filtered_non_zero_df.columns,
    ["BILLING_REGION", "REGION", "BILLING REGION", "REGION_NAME", "MARKET"]
)

country_col = find_column(
    filtered_non_zero_df.columns,
    ["COUNTRY", "COUNTRY_NAME", "COUNTRY NAME"]
)

amount_col = find_column(
    filtered_non_zero_df.columns,
    ["AMOUNT", "Amount", "BILL_AMOUNT", "BILLED_AMOUNT", "BILLING_AMOUNT", "TOTAL_AMOUNT", "NET_AMOUNT", "VALUE"]
)

if not user_type_col:
    raise ValueError(f"User type column not found: {list(filtered_non_zero_df.columns)}")

if not region_col:
    raise ValueError(f"Region column not found: {list(filtered_non_zero_df.columns)}")

if not amount_col:
    raise ValueError(f"Amount column not found: {list(filtered_non_zero_df.columns)}")

# STEP 5: Standardize user type values
filtered_non_zero_df[user_type_col] = (
    filtered_non_zero_df[user_type_col]
    .astype(str)
    .str.strip()
    .str.upper()
)

# STEP 6: Split CORP vs NON-CORP from already filtered non-zero data
corp_df = filtered_non_zero_df[
    filtered_non_zero_df[user_type_col] == "C"
].copy()

non_corp_df = filtered_non_zero_df[
    filtered_non_zero_df[user_type_col].isin(["H", "F"])
].copy()

# STEP 7: Standardize column names for reporting agent
rename_map_zero = {}
rename_map_corp = {}
rename_map_non_corp = {}

# Zero DF rename
zero_amount_col = find_column(
    zero_df.columns,
    ["AMOUNT", "Amount", "BILL_AMOUNT", "BILLED_AMOUNT", "BILLING_AMOUNT", "TOTAL_AMOUNT", "NET_AMOUNT", "VALUE"]
)
zero_region_col = find_column(
    zero_df.columns,
    ["BILLING_REGION", "REGION", "BILLING REGION", "REGION_NAME", "MARKET"]
)
zero_country_col = find_column(
    zero_df.columns,
    ["COUNTRY", "COUNTRY_NAME", "COUNTRY NAME"]
)

if zero_amount_col and zero_amount_col != "amount":
    rename_map_zero[zero_amount_col] = "amount"
if zero_region_col and zero_region_col != "billing_region":
    rename_map_zero[zero_region_col] = "billing_region"
if zero_country_col and zero_country_col != "country":
    rename_map_zero[zero_country_col] = "country"

# Corp DF rename
if amount_col != "amount":
    rename_map_corp[amount_col] = "amount"
if region_col != "billing_region":
    rename_map_corp[region_col] = "billing_region"
if country_col and country_col != "country":
    rename_map_corp[country_col] = "country"

# Non-corp DF rename
if amount_col != "amount":
    rename_map_non_corp[amount_col] = "amount"
if region_col != "billing_region":
    rename_map_non_corp[region_col] = "billing_region"
if country_col and country_col != "country":
    rename_map_non_corp[country_col] = "country"

if rename_map_zero:
    zero_df = zero_df.rename(columns=rename_map_zero)

if rename_map_corp:
    corp_df = corp_df.rename(columns=rename_map_corp)

if rename_map_non_corp:
    non_corp_df = non_corp_df.rename(columns=rename_map_non_corp)


# Convert amount to numeric for all DataFrames
for df in [zero_df, corp_df, non_corp_df]:
    if "amount" in df.columns:
        df["amount"] = (
            df["amount"]
            .astype(str)
            .str.replace(",", "", regex=False)
        )
        df["amount"] = pd.to_numeric(df["amount"], errors="coerce").fillna(0)

# STEP 8: Run reporting agent
reporting_agent = ReportingAgent()
report_paths = reporting_agent.run(
    corp_df=corp_df,
    non_corp_df=non_corp_df,
    zero_df=zero_df,
    source_filename=os.path.basename(filtered_output),
)

# FINAL OUTPUT LOGS
print("\n=== Comparison Agent Results ===")
print(f"Sample billing file : {SOURCE_FILE}")
print(f"Comparison files    : {len(comparison_files)}")
for file_path in comparison_files:
    print(f"  - {file_path}")
print(f"Comparison output   : {filtered_output}")

print("\n=== Sheet Counts ===")
print(f"Zero rows                 : {len(zero_df)}")
print(f"Filtered non-zero rows    : {len(filtered_non_zero_df)}")
print(f"Corp rows                 : {len(corp_df)}")
print(f"Non-corp rows             : {len(non_corp_df)}")

print("\n=== Reporting Agent Results ===")
print(f"Zero data file      : {report_paths['zero_data_path']}")
print(f"Corp paid file      : {report_paths['corp_paid_path']}")
print(f"Non-corp paid file  : {report_paths['non_corp_paid_path']}")