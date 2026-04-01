# app/processing/pipeline.py
from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from app.agents.cleaning_agent import CleaningAgent
from app.agents.reporting_agent import ReportingAgent
from app.config.settings import settings

logger = logging.getLogger(__name__)

AMER_COUNTRIES = {"UNITED STATES OF AMERICA", "CANADA"}


class BillingPipeline:
    """Orchestrates the billing-summary workflow from the use case.

    Pipeline stages (in order)
    --------------------------
    1. Clean and normalize source data.
    2. Split zero-amount records.
    3. Split paid records into CORP and NON-CORP.
    4. Segment into AMER/MEXICO/AMEA/EMEAA/GC.
    5. Generate output workbooks.
    """

    def __init__(self) -> None:
        self._cleaner = CleaningAgent()
        self._reporter = ReportingAgent()

    def run(self, df: pd.DataFrame, source_filename: str) -> dict[str, str | int]:
        """Execute the use-case pipeline and return output metadata and paths."""
        logger.info("Pipeline: starting for '%s' (%d rows)", source_filename, len(df))

        cleaned_df = self._cleaner.run(df, cost_center_df=self._load_cost_centers())
        zero_df, paid_df = self._split_zero_data(cleaned_df)

        corp_df = paid_df[paid_df["billing_type"] == "CORP"].copy()
        non_corp_df = paid_df[paid_df["billing_type"] == "NON-CORP"].copy()

        file_paths = self._reporter.run(
            corp_df=corp_df,
            non_corp_df=non_corp_df,
            zero_df=zero_df,
            source_filename=source_filename,
        )

        logger.info("Pipeline: complete for '%s'", source_filename)
        return {
            **file_paths,
            "total_rows": int(len(cleaned_df)),
            "zero_rows": int(len(zero_df)),
            "corp_rows": int(len(corp_df)),
            "non_corp_rows": int(len(non_corp_df)),
        }

    def _load_cost_centers(self) -> pd.DataFrame | None:
        cost_center_path = Path(settings.upload_dir) / "cost_centers.csv"
        if not cost_center_path.exists():
            logger.info("Pipeline: cost center map not found at %s (skipping)", cost_center_path)
            return None
        try:
            return pd.read_csv(cost_center_path)
        except Exception:
            logger.exception("Pipeline: failed to load cost center file %s", cost_center_path)
            return None

    def _split_zero_data(self, df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
        zero_mask = df["amount"] == 0
        zero_df = df[zero_mask].copy()
        paid_df = df[~zero_mask].copy()

        paid_df["billing_type"] = paid_df["user_type"].apply(self._billing_type_from_user_type)
        paid_df["billing_region"] = paid_df.apply(self._billing_region, axis=1)
        return zero_df, paid_df

    def _billing_type_from_user_type(self, value: str) -> str:
        if value == "C":
            return "CORP"
        if value in {"F", "H"}:
            return "NON-CORP"
        return "NON-CORP"

    def _billing_region(self, row: pd.Series) -> str:
        region = str(row.get("region", "")).upper().strip()
        country = str(row.get("country", "")).upper().strip()

        if region == "AMER":
            if country in AMER_COUNTRIES:
                return "AMER"
            return "MEXICO"

        if region in {"AMEA", "EMEAA", "GC", "MEXICO"}:
            return region

        # Fallback to AMEA-style processing bucket for unknown regions.
        return "AMEA"
