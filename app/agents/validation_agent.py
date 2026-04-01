# app/agents/validation_agent.py
from __future__ import annotations

import json
import logging

import pandas as pd
from openai import OpenAI

from app.config.settings import settings

logger = logging.getLogger(__name__)

LARGE_AMOUNT_THRESHOLD = 100_000


class ValidationAgent:
    """Validates cleaned billing data.

    Two validation layers
    --------------------
    1. **Rule-based**: negative amounts, null invoice IDs, future dates, etc.
    2. **AI-based** (optional): LLM reviews suspicious rows and returns
       structured JSON with anomaly descriptions.
    """

    def __init__(self) -> None:
        self._client = OpenAI(api_key=settings.openai_api_key)

    def run(self, df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
        """Return ``(valid_df, issues_df)``."""
        df = df.copy()
        df["issue"] = ""

        df = self._rule_based_validation(df)

        if settings.enable_ai_validation:
            df = self._ai_based_validation(df)

        issues_df = df[df["issue"] != ""].copy()
        valid_df = df[df["issue"] == ""].drop(columns=["issue"])
        logger.info(
            "ValidationAgent: %d valid, %d issues", len(valid_df), len(issues_df)
        )
        return valid_df, issues_df

    def _rule_based_validation(self, df: pd.DataFrame) -> pd.DataFrame:
        checks = [
            (df["invoice_id"].isna(), "missing invoice_id"),
            (df["amount"].isna(), "missing amount"),
            (df["amount"] < 0, "negative amount"),
            (df["billing_date"].isna(), "invalid billing_date"),
            (df["billing_date"] > pd.Timestamp.now(), "billing_date is in the future"),
        ]
        for mask, message in checks:
            df.loc[mask, "issue"] = df.loc[mask, "issue"].apply(
                lambda existing: (existing + "; " + message).lstrip("; ")
            )
        return df

    def _ai_based_validation(self, df: pd.DataFrame) -> pd.DataFrame:
        candidate_mask = (df["amount"] > LARGE_AMOUNT_THRESHOLD) | (df["issue"] != "")
        candidates = df[candidate_mask]
        if candidates.empty:
            return df

        rows_json = candidates[
            ["invoice_id", "client_name", "amount", "billing_date", "description"]
        ].to_json(orient="records", date_format="iso")

        system_prompt = (
            "You are a billing auditor. Review the provided billing rows and identify "
            "any anomalies such as duplicate charges, unusually high amounts for the "
            "client profile, or suspicious descriptions. "
            'Reply ONLY with a JSON array where each element has keys: '
            '"invoice_id" (string) and "anomaly" (string or null).'
        )

        try:
            response = self._client.chat.completions.create(
                model=settings.openai_model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": rows_json},
                ],
                temperature=0,
                response_format={"type": "json_object"},
            )
            content = response.choices[0].message.content or "{}"
            ai_flags: list[dict] = json.loads(content).get("rows", [])
        except Exception:
            logger.exception("ValidationAgent: AI validation call failed — skipping")
            return df

        flag_map: dict[str, str] = {
            item["invoice_id"]: item["anomaly"]
            for item in ai_flags
            if item.get("anomaly")
        }
        for invoice_id, anomaly in flag_map.items():
            mask = df["invoice_id"].astype(str) == str(invoice_id)
            df.loc[mask, "issue"] = df.loc[mask, "issue"].apply(
                lambda existing: (existing + "; AI: " + anomaly).lstrip("; ")
            )

        return df
