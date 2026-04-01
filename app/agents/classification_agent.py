# app/agents/classification_agent.py
from __future__ import annotations

import json
import logging

import pandas as pd
from openai import OpenAI

from app.config.settings import settings

logger = logging.getLogger(__name__)

BATCH_SIZE = 50

SYSTEM_PROMPT = """You are a billing classification assistant.
Given a list of billing rows as JSON, assign a category to each row.
Valid categories: Software, Hardware, Consulting, Support, Subscription, Other.
Reply ONLY with a JSON object: {"classifications": [{"invoice_id": "...", "category": "..."}]}
"""


class ClassificationAgent:
    """Uses an LLM to assign a billing category to every valid row.

    The ``category`` column is added (or overwritten) in-place.
    Rows that the LLM cannot classify default to ``"Other"``.
    """

    def __init__(self) -> None:
        self._client = OpenAI(api_key=settings.openai_api_key)

    def run(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df

        df = df.copy()
        df["category"] = "Other"

        for batch_start in range(0, len(df), BATCH_SIZE):
            batch = df.iloc[batch_start : batch_start + BATCH_SIZE]
            self._classify_batch(df, batch)

        logger.info("ClassificationAgent: classified %d rows", len(df))
        return df

    def _classify_batch(self, full_df: pd.DataFrame, batch: pd.DataFrame) -> None:
        rows_json = batch[
            ["invoice_id", "description", "amount", "client_name"]
        ].to_json(orient="records")

        try:
            response = self._client.chat.completions.create(
                model=settings.openai_model,
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": rows_json},
                ],
                temperature=0,
                response_format={"type": "json_object"},
            )
            content = response.choices[0].message.content or "{}"
            results: list[dict] = json.loads(content).get("classifications", [])
        except Exception:
            logger.exception(
                "ClassificationAgent: batch %d failed — skipping", batch.index[0]
            )
            return

        for item in results:
            inv_id = str(item.get("invoice_id", ""))
            category = item.get("category", "Other")
            mask = full_df["invoice_id"].astype(str) == inv_id
            full_df.loc[mask, "category"] = category
