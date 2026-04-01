# app/agents/cleaning_agent.py
from __future__ import annotations

import logging
import re

import pandas as pd

logger = logging.getLogger(__name__)

REQUIRED_COLUMNS: list[str] = [
    "amount",
    "user_type",
    "region",
    "country",
    "holidex",
    "person_holidex",
    "course_name",
    "myid",
]

# Allowed characters for cleaned course names: alnum, whitespace, '-', '&', '(', ')'
_COURSE_ALLOWED_PATTERN = re.compile(r"[^A-Za-z0-9\s\-&()]+")

_COLUMN_ALIASES: dict[str, tuple[str, ...]] = {
    "amount": ("amount", "amt", "billing_amount"),
    "user_type": ("user_type", "usertype", "corp_noncorp", "type"),
    "region": ("region", "geo_region"),
    "country": ("country", "country_name"),
    "holidex": ("holidex", "holidex_code", "column_c", "c"),
    "person_holidex": (
        "person_holidex",
        "person_holidex_code",
        "column_y",
        "y",
    ),
    "course_name": ("course_name", "course", "course_title"),
    "myid": ("myid", "my_id", "employee_id", "employee", "person_id", "username", "id"),
}


class CleaningAgent:
    """Prepares raw billing data for downstream processing.

    Steps
    -----
    1. Normalise column names and apply alias mapping to canonical names.
    2. Drop fully duplicate rows.
    3. Coerce key columns and fill sensible defaults.
    4. Validate Holidex format and sync from person_holidex where required.
    5. Remove disallowed special characters from course names.
    """

    def run(self, df: pd.DataFrame, cost_center_df: pd.DataFrame | None = None) -> pd.DataFrame:
        logger.info("CleaningAgent: starting — %d rows", len(df))
        df = self._normalise_columns(df)
        df = self._apply_aliases(df)
        df = self._drop_duplicates(df)
        df = self._fill_defaults(df)
        df = self._coerce_types(df)
        self._validate_columns(df)
        df = self._sync_holidex(df)
        df = self._clean_course_names(df)
        df = self._add_cost_centers(df, cost_center_df)
        logger.info("CleaningAgent: finished — %d rows retained", len(df))
        return df

    def _normalise_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        df.columns = (
            df.columns.str.strip().str.lower().str.replace(r"\s+", "_", regex=True)
        )
        return df

    def _apply_aliases(self, df: pd.DataFrame) -> pd.DataFrame:
        rename_map: dict[str, str] = {}
        for canonical, aliases in _COLUMN_ALIASES.items():
            for alias in aliases:
                if alias in df.columns:
                    rename_map[alias] = canonical
                    break
        if rename_map:
            df = df.rename(columns=rename_map)
        return df

    def _drop_duplicates(self, df: pd.DataFrame) -> pd.DataFrame:
        before = len(df)
        df = df.drop_duplicates()
        removed = before - len(df)
        if removed:
            logger.warning("CleaningAgent: removed %d duplicate rows", removed)
        return df

    def _fill_defaults(self, df: pd.DataFrame) -> pd.DataFrame:
        defaults: dict[str, object] = {
            "region": "",
            "country": "",
            "course_name": "",
            "user_type": "",
            "holidex": "",
            "person_holidex": "",
            "myid": "",
        }
        for col, value in defaults.items():
            if col in df.columns:
                df[col] = df[col].fillna(value)
        return df

    def _coerce_types(self, df: pd.DataFrame) -> pd.DataFrame:
        if "amount" in df.columns:
            df["amount"] = pd.to_numeric(df["amount"], errors="coerce").fillna(0)

        # Normalise commonly used text columns for downstream grouping rules.
        for col in ["user_type", "region", "country", "holidex", "person_holidex", "myid"]:
            if col in df.columns:
                df[col] = df[col].astype(str).str.strip()

        if "user_type" in df.columns:
            df["user_type"] = df["user_type"].str.upper()
        if "region" in df.columns:
            df["region"] = df["region"].str.upper()
        return df

    def _validate_columns(self, df: pd.DataFrame) -> None:
        missing = set(REQUIRED_COLUMNS) - set(df.columns)
        if missing:
            raise ValueError(f"CleaningAgent: missing required columns: {missing}")

    def _sync_holidex(self, df: pd.DataFrame) -> pd.DataFrame:
        # When person_holidex is a valid 5-char code and differs, update holidex.
        person_clean = df["person_holidex"].str.replace(r"[^A-Za-z0-9]", "", regex=True)
        person_valid = person_clean.str.match(r"^[A-Za-z0-9]{5}$", na=False)
        holidex_clean = df["holidex"].str.replace(r"[^A-Za-z0-9]", "", regex=True)
        mismatch = person_valid & (holidex_clean != person_clean)

        if mismatch.any():
            logger.info("CleaningAgent: holidex updated from person_holidex for %d rows", int(mismatch.sum()))
            df.loc[mismatch, "holidex"] = person_clean[mismatch]

        return df

    def _clean_course_names(self, df: pd.DataFrame) -> pd.DataFrame:
        df["course_name"] = (
            df["course_name"]
            .astype(str)
            .str.replace(",", " ", regex=False)
            .str.replace(_COURSE_ALLOWED_PATTERN, "", regex=True)
            .str.replace(r"\s+", " ", regex=True)
            .str.strip()
        )
        return df

    def _add_cost_centers(
        self,
        df: pd.DataFrame,
        cost_center_df: pd.DataFrame | None,
    ) -> pd.DataFrame:
        if cost_center_df is None or cost_center_df.empty:
            if "cost_center" not in df.columns:
                df["cost_center"] = ""
            return df

        map_df = cost_center_df.copy()
        map_df.columns = (
            map_df.columns.astype(str).str.strip().str.lower().str.replace(r"\s+", "_", regex=True)
        )

        if "myid" not in map_df.columns or "cost_center" not in map_df.columns:
            logger.warning("CleaningAgent: cost center file missing required columns myid/cost_center")
            if "cost_center" not in df.columns:
                df["cost_center"] = ""
            return df

        map_df = map_df[["myid", "cost_center"]].copy()
        map_df["myid"] = map_df["myid"].astype(str).str.strip()
        map_df["cost_center"] = map_df["cost_center"].astype(str).str.strip()

        merged = df.merge(map_df, on="myid", how="left", suffixes=("", "_mapped"))
        if "cost_center" in merged.columns and "cost_center_mapped" in merged.columns:
            merged["cost_center"] = merged["cost_center"].replace("", pd.NA)
            merged["cost_center"] = merged["cost_center"].fillna(merged["cost_center_mapped"])
            merged = merged.drop(columns=["cost_center_mapped"])
        elif "cost_center" not in merged.columns and "cost_center_mapped" in merged.columns:
            merged = merged.rename(columns={"cost_center_mapped": "cost_center"})

        merged["cost_center"] = merged["cost_center"].fillna("")
        return merged
