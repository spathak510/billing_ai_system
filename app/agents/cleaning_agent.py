# app/agents/cleaning_agent.py
from __future__ import annotations


from typing import Iterable, Optional

import logging
import re
import os
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
    

    #comparison agent
class SampleBillingComparisonAgent:
    SAMPLE_ORDER_CANDIDATES = [
        "ORDER_NO",
        "ORDER NO",
        "ORDERID",
        "ORDER ID",
        "ORDER_NUMBER",
        "ORDER NUMBER",
    ]

    COMPARE_ORDER_CANDIDATES = [
        "ORDER_NO",
        "ORDER NO",
        "ORDERID",
        "ORDER ID",
        "ORDER_NUMBER",
        "ORDER NUMBER",
    ]

    REGION_CANDIDATES = [
        "REGION",
        "REGION_NAME",
        "MARKET",
        "BILLING_REGION",
    ]

    USER_TYPE_CANDIDATES = [
        "USER TYPE",
        "USER_TYPE",
        "TYPE",
        "CATEGORY",
    ]

    AMOUNT_CANDIDATES = [
        "AMOUNT",
        "BILL_AMOUNT",
        "BILLING_AMOUNT",
        "TOTAL_AMOUNT",
        "NET_AMOUNT",
        "VALUE",
    ]

    def __init__(self):
        pass

    def _normalize(self, value) -> str:
        if pd.isna(value):
            return ""
        text = str(value).strip()
        if text.endswith(".0"):
            text = text[:-2]
        return text.upper()

    def _find_column(self, columns: Iterable[str], candidates: list[str]) -> Optional[str]:
        normalized_map = {str(col).strip().upper(): col for col in columns}
        for candidate in candidates:
            if candidate.upper() in normalized_map:
                return normalized_map[candidate.upper()]
        return None

    def _normalize_region(self, value) -> str:
        text = self._normalize(value)
        region_map = {
            "EMEA": "EMEAA",
            "EMEAA": "EMEAA",
            "AMER": "AMER",
            "AMEA": "AMEA",
            "MEXICO": "MEXICO",
            "GC": "GC",
        }
        return region_map.get(text, text)

    def _normalize_user_type(self, value) -> str:
        text = self._normalize(value)

        if text in ["F", "H", "NON-CORP", "NONCORP", "NON CORP"]:
            return "NON-CORP"
        if text in ["C", "CORP"]:
            return "CORP"

        return text

    def _parse_amount(self, value) -> float:
        if pd.isna(value):
            return 0.0
        text = str(value).strip().replace(",", "")
        if text == "":
            return 0.0
        try:
            return float(text)
        except ValueError:
            return 0.0

    def _detect_region_and_type_from_filename(self, file_path: str) -> tuple[Optional[str], Optional[str]]:
        filename = os.path.basename(file_path).upper()

        region = None
        if "EMEAA" in filename or "EMEA" in filename:
            region = "EMEAA"
        elif "AMER" in filename:
            region = "AMER"
        elif "AMEA" in filename:
            region = "AMEA"
        elif "MEXICO" in filename:
            region = "MEXICO"
        elif "GC" in filename:
            region = "GC"

        user_type = None
        if "NON-CORP" in filename or "NONCORP" in filename or "NON CORP" in filename:
            user_type = "NON-CORP"
        elif "CORP" in filename:
            user_type = "CORP"

        return region, user_type

    def run(
        self,
        sample_billing_path: str,
        comparison_file_paths: list[str],
        output_path: str,
    ) -> str:
        if not os.path.exists(sample_billing_path):
            raise FileNotFoundError(f"Sample billing file not found: {sample_billing_path}")

        # Read sample billing file
        if sample_billing_path.lower().endswith(".csv"):
            sample_df = pd.read_csv(sample_billing_path)
        else:
            sample_df = pd.read_excel(sample_billing_path)

        sample_order_col = self._find_column(sample_df.columns, self.SAMPLE_ORDER_CANDIDATES)
        sample_region_col = self._find_column(sample_df.columns, self.REGION_CANDIDATES)
        sample_user_type_col = self._find_column(sample_df.columns, self.USER_TYPE_CANDIDATES)
        sample_amount_col = self._find_column(sample_df.columns, self.AMOUNT_CANDIDATES)

        if not sample_order_col:
            raise ValueError("Could not find ORDER_NO-like column in sample billing file.")
        if not sample_region_col:
            raise ValueError("Could not find REGION column in sample billing file.")
        if not sample_user_type_col:
            raise ValueError("Could not find USER TYPE column in sample billing file.")
        if not sample_amount_col:
            raise ValueError("Could not find AMOUNT column in sample billing file.")

        # Step 1 + 2: Split zero and non-zero amount rows
        sample_df["_parsed_amount"] = sample_df[sample_amount_col].apply(self._parse_amount)

        zero_df = sample_df[sample_df["_parsed_amount"] == 0].copy()
        non_zero_df = sample_df[sample_df["_parsed_amount"] != 0].copy()

        # Build lookup: (region, user_type) -> set(order_ids)
        comparison_lookup: dict[tuple[str, str], set[str]] = {}
        scanned_files = 0

        for file_path in comparison_file_paths:
            if not os.path.exists(file_path):
                continue

            try:
                if file_path.lower().endswith(".csv"):
                    compare_df = pd.read_csv(file_path)
                else:
                    compare_df = pd.read_excel(file_path)
            except Exception:
                continue

            compare_order_col = self._find_column(compare_df.columns, self.COMPARE_ORDER_CANDIDATES)
            if not compare_order_col:
                continue

            region, user_type = self._detect_region_and_type_from_filename(file_path)
            if not region or not user_type:
                continue

            values = compare_df[compare_order_col].dropna().astype(str)
            normalized_values = {
                self._normalize(value)
                for value in values
                if self._normalize(value)
            }

            comparison_lookup[(region, user_type)] = normalized_values
            scanned_files += 1

        # Step 3: Compare only non-zero rows with correct region/type file
        kept_rows = []
        removed_rows = []

        for _, row in non_zero_df.iterrows():
            order_no = self._normalize(row[sample_order_col])
            region = self._normalize_region(row[sample_region_col])
            user_type = self._normalize_user_type(row[sample_user_type_col])

            matching_orders = comparison_lookup.get((region, user_type), set())

            if order_no and order_no in matching_orders:
                removed_rows.append(row.to_dict())
            else:
                kept_rows.append(row.to_dict())

        filtered_non_zero_df = pd.DataFrame(kept_rows)
        removed_matched_df = pd.DataFrame(removed_rows)

        # Remove helper column before saving
        for df in [zero_df, non_zero_df, filtered_non_zero_df, removed_matched_df]:
            if not df.empty and "_parsed_amount" in df.columns:
                df.drop(columns=["_parsed_amount"], inplace=True, errors="ignore")

        output_dir = os.path.dirname(output_path)
        if output_dir:
            os.makedirs(output_dir, exist_ok=True)

        summary_df = pd.DataFrame(
            [
                {"Metric": "Total rows in sample billing", "Value": len(sample_df)},
                {"Metric": "Zero amount rows", "Value": len(zero_df)},
                {"Metric": "Non-zero rows", "Value": len(non_zero_df)},
                {"Metric": "Rows removed after comparison", "Value": len(removed_matched_df)},
                {"Metric": "Final rows kept after comparison", "Value": len(filtered_non_zero_df)},
                {"Metric": "Comparison files scanned", "Value": scanned_files},
            ]
        )

        with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
            summary_df.to_excel(writer, sheet_name="Summary", index=False)
            zero_df.to_excel(writer, sheet_name="Zero_Data", index=False)
            non_zero_df.to_excel(writer, sheet_name="Non_Zero_Data", index=False)
            filtered_non_zero_df.to_excel(writer, sheet_name="Filtered_Non_Zero_Data", index=False)
            removed_matched_df.to_excel(writer, sheet_name="Removed_Matched_Rows", index=False)

        return output_path
