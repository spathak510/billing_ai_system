# app/agents/cleaning_agent.py
from __future__ import annotations

import logging
import re
from datetime import datetime
from pathlib import Path
from typing import Iterable

import pandas as pd

logger = logging.getLogger(__name__)

AMER_COUNTRIES = {"UNITED STATES OF AMERICA", "CANADA"}

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

    def run(
        self, df: pd.DataFrame, cost_center_df: pd.DataFrame | None = None
    ) -> pd.DataFrame:
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
        for col in [
            "user_type",
            "region",
            "country",
            "holidex",
            "person_holidex",
            "myid",
        ]:
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
            logger.info(
                "CleaningAgent: holidex updated from person_holidex for %d rows",
                int(mismatch.sum()),
            )
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
            map_df.columns.astype(str)
            .str.strip()
            .str.lower()
            .str.replace(r"\s+", "_", regex=True)
        )

        if "myid" not in map_df.columns or "cost_center" not in map_df.columns:
            logger.warning(
                "CleaningAgent: cost center file missing required columns myid/cost_center"
            )
            if "cost_center" not in df.columns:
                df["cost_center"] = ""
            return df

        map_df = map_df[["myid", "cost_center"]].copy()
        map_df["myid"] = map_df["myid"].astype(str).str.strip()
        map_df["cost_center"] = map_df["cost_center"].astype(str).str.strip()

        merged = df.merge(map_df, on="myid", how="left", suffixes=("", "_mapped"))
        if "cost_center" in merged.columns and "cost_center_mapped" in merged.columns:
            merged["cost_center"] = merged["cost_center"].replace("", pd.NA)
            merged["cost_center"] = merged["cost_center"].fillna(
                merged["cost_center_mapped"]
            )
            merged = merged.drop(columns=["cost_center_mapped"])
        elif (
            "cost_center" not in merged.columns
            and "cost_center_mapped" in merged.columns
        ):
            merged = merged.rename(columns={"cost_center_mapped": "cost_center"})

        merged["cost_center"] = merged["cost_center"].fillna("")
        return merged


class ComparisonAgent:
    # Section 1: Column candidates used to identify fields in monthly/history files
    ORDER_CANDIDATES = [
        "ORDER_NO",
        "ORDER NO",
        "ORDERID",
        "ORDER ID",
        "ORDER_NUMBER",
        "ORDER NUMBER",
    ]
    REGION_CANDIDATES = ["REGION", "REGION_NAME", "MARKET", "BILLING_REGION"]
    COUNTRY_CANDIDATES = ["COUNTRY", "COUNTRY_NAME"]
    USER_TYPE_CANDIDATES = ["USER TYPE", "USER_TYPE", "TYPE", "CATEGORY"]
    AMOUNT_CANDIDATES = [
        "AMOUNT",
        "BILL_AMOUNT",
        "BILLING_AMOUNT",
        "TOTAL_AMOUNT",
        "NET_AMOUNT",
        "VALUE",
    ]
    INSTRUCTOR_CANDIDATES = ["INSTRUCTOR", "INSTRUCTOR_NAME", "FACILITATOR", "TRAINER"]
    APPLY_REVENUE_CANDIDATES = [
        "APPLY REVENUE",
        "APPLY_REVENUE",
        "APPLYREVENUE",
    ]
    BUSINESS_UNIT_CANDIDATES = ["BUSINESS UNIT", "BUSINESS_UNIT", "BUSINESSUNIT", "BU"]

    def __init__(
        self,
        monthly_dir: str | Path = "data/Monthly_data",
        history_dir: str | Path = "data/History_data",
        apac_country_file: str | Path = "data/APAC COUNTRIES.xlsx",
        output_dir: str | Path = "output/Monthly_cleaned_report",
    ) -> None:
        self.monthly_dir = Path(monthly_dir)
        self.history_dir = Path(history_dir)
        self.apac_country_file = Path(apac_country_file)
        self.output_dir = Path(output_dir)

    # Section 2: Main flow
    # 1. Read monthly file
    # 2. Split zero and non-zero rows
    # 3. Compare non-zero order numbers with history files
    # 4. Enrich apply_revenue/business_unit from history files using instructor
    # 5. Apply APAC override as the final step
    # 6. Write output workbook
    def run(
        self,
        source_file_path: str | Path | None = None,
        output_file_path: str | Path | None = None,
    ) -> dict[str, object]:
        monthly_file = (
            Path(source_file_path)
            if source_file_path
            else self._get_latest_monthly_file()
        )
        monthly_df = self._normalise_columns(self._read_table(monthly_file))

        order_col = self._require_column(
            monthly_df.columns, self.ORDER_CANDIDATES, "order number"
        )
        region_col = self._require_column(
            monthly_df.columns, self.REGION_CANDIDATES, "region"
        )
        country_col = self._require_column(
            monthly_df.columns, self.COUNTRY_CANDIDATES, "country"
        )
        user_type_col = self._require_column(
            monthly_df.columns, self.USER_TYPE_CANDIDATES, "user type"
        )
        amount_col = self._require_column(
            monthly_df.columns, self.AMOUNT_CANDIDATES, "amount"
        )
        instructor_col = self._find_column(
            monthly_df.columns, self.INSTRUCTOR_CANDIDATES
        )

        monthly_df["_amount_value"] = self._get_column_values(
            monthly_df, amount_col
        ).apply(self._parse_amount)
        monthly_df["_billing_type"] = self._get_column_values(
            monthly_df, user_type_col
        ).apply(self._normalize_user_type)
        monthly_df["_source_region"] = self._get_column_values(
            monthly_df, region_col
        ).apply(self._normalize_text)
        monthly_df["_country_value"] = self._get_column_values(
            monthly_df, country_col
        ).apply(self._normalize_text)
        monthly_df["_billing_region"] = monthly_df.apply(
            lambda row: self._derive_billing_region(
                row["_source_region"], row["_country_value"]
            ),
            axis=1,
        )

        zero_df = monthly_df[monthly_df["_amount_value"] == 0].copy()
        non_zero_df = monthly_df[monthly_df["_amount_value"] != 0].copy()

        history_files = self._collect_history_files()
        history_sources = self._prepare_sources(history_files, mixed_region_file=False)

        # Pre-build set of all order numbers from history files (O(n) once instead of O(n²) per row)
        all_history_orders = self._build_order_number_set(history_sources)

        non_zero_df["_normalized_order"] = self._get_column_values(
            non_zero_df, order_col
        ).apply(self._normalize_text)
        non_zero_df["_matched_in_history"] = non_zero_df["_normalized_order"].apply(
            lambda order_no: self._normalize_text(order_no) in all_history_orders
        )

        filtered_df = non_zero_df[~non_zero_df["_matched_in_history"]].copy()
        removed_df = non_zero_df[non_zero_df["_matched_in_history"]].copy()

        if "apply_revenue" not in filtered_df.columns:
            filtered_df["apply_revenue"] = ""
        if "business_unit" not in filtered_df.columns:
            filtered_df["business_unit"] = ""

        if instructor_col:
            # Build lookup dictionaries ONCE for all instructors (much faster than row-by-row search)
            lookup_cache = self._build_instructor_lookup_cache(history_sources)

            filtered_df[["apply_revenue", "business_unit"]] = filtered_df.apply(
                lambda row: pd.Series(
                    self._resolve_instructor_values_cached(
                        row=row,
                        instructor_col=instructor_col,
                        lookup_cache=lookup_cache,
                        country_value=row.get("_country_value", ""),
                        source_region=row.get("_source_region", ""),
                    )
                ),
                axis=1,
            )

        self._apply_apac_region_override(filtered_df, region_col)
        self._drop_helper_columns(filtered_df)

        # Format output: rename BU column and uppercase all headers
        filtered_df = filtered_df.rename(columns={"business_unit": "BU"})
        filtered_df.columns = [col.upper() for col in filtered_df.columns]

        self.output_dir.mkdir(parents=True, exist_ok=True)
        final_output = (
            Path(output_file_path)
            if output_file_path
            else self.output_dir / self._default_output_name()
        )
        final_output.parent.mkdir(parents=True, exist_ok=True)

        with pd.ExcelWriter(final_output, engine="openpyxl") as writer:
            filtered_df.to_excel(
                writer, sheet_name="Monthly_Cleaned_Report", index=False
            )

        return {
            "status": "success",
            "source_file": str(monthly_file.resolve()),
            "output_file": str(final_output.resolve()),
            "total_rows": int(len(monthly_df)),
            "zero_rows": int(len(zero_df)),
            "output_rows": int(len(filtered_df)),
            "history_files_scanned": len(history_files),
        }

    # Section 3: Input file discovery
    def _get_latest_monthly_file(self) -> Path:
        files = sorted(
            [
                path
                for path in self.monthly_dir.iterdir()
                if path.is_file() and path.suffix.lower() in {".csv", ".xlsx", ".xls"}
            ],
            key=lambda item: item.stat().st_mtime,
            reverse=True,
        )
        if not files:
            raise FileNotFoundError(
                f"No monthly billing file found in {self.monthly_dir}"
            )
        return files[0]

    # Section 4: History file discovery
    def _collect_history_files(self) -> list[Path]:
        if not self.history_dir.exists():
            raise FileNotFoundError(f"History folder not found: {self.history_dir}")
        return sorted(
            [
                path
                for path in self.history_dir.rglob("*")
                if path.is_file() and path.suffix.lower() in {".csv", ".xlsx", ".xls"}
            ],
            key=lambda item: item.stat().st_mtime,
            reverse=True,
        )

    # Section 5: Prepare history sources for order-number comparison
    def _prepare_sources(
        self, file_paths: list[Path], mixed_region_file: bool
    ) -> list[dict[str, object]]:
        sources: list[dict[str, object]] = []
        for file_path in file_paths:
            if not file_path.exists():
                continue

            df = self._safe_read_table(file_path)
            if df is None or df.empty:
                continue

            instructor_col = self._select_best_instructor_column(df)
            apply_revenue_col = self._select_best_apply_revenue_column(df)
            business_unit_col = self._select_best_business_unit_column(df)
            order_col = self._select_best_order_column(df)
            region_col = self._find_column(df.columns, self.REGION_CANDIDATES)
            country_col = self._find_column(df.columns, self.COUNTRY_CANDIDATES)
            user_type_col = self._find_column(df.columns, self.USER_TYPE_CANDIDATES)

            base_region, base_billing_type = self._detect_region_and_type_from_path(
                file_path
            )

            source = {
                "path": file_path,
                "mtime": file_path.stat().st_mtime,
                "df": df,
                "mixed_region_file": mixed_region_file,
                "base_region": base_region,
                "base_billing_type": base_billing_type,
                "instructor_col": instructor_col,
                "apply_revenue_col": apply_revenue_col,
                "business_unit_col": business_unit_col,
                "order_col": order_col,
                "region_col": region_col,
                "country_col": country_col,
                "user_type_col": user_type_col,
            }
            sources.append(source)
        return sources

    # Section 6: Order number comparison with history files (OPTIMIZED: O(1) set lookup)
    def _build_order_number_set(
        self, history_sources: list[dict[str, object]]
    ) -> set[str]:
        """Pre-build set of all normalized order numbers from history files for O(1) lookup."""
        all_orders = set()
        for source in history_sources:
            order_col = source["order_col"]
            if not order_col:
                continue
            # Vectorized normalization of all order numbers at once
            order_values = self._get_column_values(source["df"], order_col).apply(
                self._normalize_text
            )
            all_orders.update(value for value in order_values if value)
        return all_orders

    # Section 7: Instructor enrichment using history files
    # OPTIMIZED: Build lookup cache once, then use O(1) dictionary lookups for each row
    def _build_instructor_lookup_cache(
        self, history_sources: list[dict[str, object]]
    ) -> dict[str, dict[str, tuple[str, str]]]:
        """
        Pre-build instructor lookup dictionaries using vectorized pandas operations.
        Returns: {source_key: {normalized_instructor: (apply_revenue, business_unit)}}
        This is done ONCE instead of for each row, resulting in massive speedup.
        """
        cache = {}

        for source_idx, source in enumerate(history_sources):
            if not source.get("instructor_col"):
                continue
            if not source.get("apply_revenue_col") and not source.get(
                "business_unit_col"
            ):
                continue

            instructor_cache = {}
            df = source["df"]
            instructor_col = source["instructor_col"]
            apply_revenue_col = source["apply_revenue_col"]
            business_unit_col = source["business_unit_col"]

            # Vectorized extraction of all columns at once
            instructors = self._get_column_values(df, instructor_col).apply(
                self._normalize_text
            )
            apply_revenues = (
                self._get_column_values(df, apply_revenue_col).apply(
                    self._extract_apply_revenue_vectorized
                )
                if apply_revenue_col
                else pd.Series([""] * len(df))
            )
            business_units = (
                self._get_column_values(df, business_unit_col).apply(
                    self._extract_business_unit_vectorized
                )
                if business_unit_col
                else pd.Series([""] * len(df))
            )

            # Build cache from vectorized arrays (much faster than iterrows)
            for instructor, apply_revenue, business_unit in zip(
                instructors, apply_revenues, business_units
            ):
                if instructor and (apply_revenue or business_unit):
                    instructor_cache[instructor] = (apply_revenue, business_unit)

            if instructor_cache:
                cache[f"source_{source_idx}"] = {
                    "data": instructor_cache,
                    "mtime": source.get("mtime", 0.0),
                    "path": source.get("path", ""),
                }

        return cache

    def _extract_apply_revenue_vectorized(self, value: object) -> str:
        """Extract apply_revenue from value (vectorized version)."""
        value = self._clean_output_value(value)
        if not value:
            return ""
        normalized = value.replace(",", "").strip()
        return normalized if normalized.isdigit() and len(normalized) > 0 else ""

    def _extract_business_unit_vectorized(self, value: object) -> str:
        """Extract business_unit from value (vectorized version)."""
        value = self._clean_output_value(value)
        if not value:
            return ""
        if (
            2 <= len(value) <= 10
            and value[0].isalpha()
            and " " not in value
            and any(c.isdigit() for c in value)
        ):
            return value
        return ""

    def _resolve_instructor_values_cached(
        self,
        row: pd.Series,
        instructor_col: str,
        lookup_cache: dict[str, dict],
        country_value: str = "",
        source_region: str = "",
    ) -> tuple[str, str]:
        """
        Fast lookup using pre-built cache dictionaries (O(1) instead of O(n)).
        """
        instructor = self._normalize_text(self._get_row_value(row, instructor_col))
        if not instructor:
            return "", ""

        normalized_source_region = self._normalize_text(source_region)
        normalized_country = self._normalize_text(country_value)

        # Determine if we should exclude Mexico files from first pass
        exclude_mexico_in_first_pass = (
            normalized_source_region == "amer" and normalized_country != "mexico"
        )

        matches: list[tuple[str, str, float]] = []

        # First pass: Search all relevant sources using cache
        for source_key, source_cache in lookup_cache.items():
            # Skip Mexico files if processing AMER + non-Mexico country
            if (
                exclude_mexico_in_first_pass
                and "mexico" in str(source_cache.get("path", "")).lower()
            ):
                continue

            # O(1) dictionary lookup instead of O(n) dataframe search
            if instructor in source_cache["data"]:
                apply_revenue, business_unit = source_cache["data"][instructor]
                if apply_revenue or business_unit:
                    matches.append(
                        (apply_revenue, business_unit, source_cache.get("mtime", 0.0))
                    )

        # If no matches and AMER+Mexico, search Mexico files as fallback
        if (
            not matches
            and normalized_source_region == "amer"
            and normalized_country == "mexico"
        ):
            for source_key, source_cache in lookup_cache.items():
                if "mexico" not in str(source_cache.get("path", "")).lower():
                    continue

                if instructor in source_cache["data"]:
                    apply_revenue, business_unit = source_cache["data"][instructor]
                    if apply_revenue or business_unit:
                        matches.append(
                            (
                                apply_revenue,
                                business_unit,
                                source_cache.get("mtime", 0.0),
                            )
                        )

        if not matches:
            return "", ""

        # Sort by modification time (most recent first) and return latest
        matches.sort(key=lambda x: x[2], reverse=True)
        return matches[0][0], matches[0][1]

    # Section 8: Final APAC override
    def _apply_apac_region_override(self, df: pd.DataFrame, region_col: str) -> None:
        if df.empty or not self.apac_country_file.exists():
            return

        apac_df = self._read_table(self.apac_country_file)
        if apac_df.empty:
            return

        apac_country_col = (
            self._find_column(apac_df.columns, self.COUNTRY_CANDIDATES)
            or apac_df.columns[0]
        )
        apac_countries = {
            self._normalize_text(value)
            for value in self._get_column_values(apac_df, apac_country_col).tolist()
            if self._normalize_text(value)
        }
        if not apac_countries:
            return

        output_country_col = self._find_column(df.columns, self.COUNTRY_CANDIDATES)
        if not output_country_col:
            return

        apac_mask = self._get_column_values(df, output_country_col).apply(
            lambda value: self._normalize_text(value) in apac_countries
        )
        df.loc[apac_mask, region_col] = "APAC"

    # Section 9: File reading and column normalization
    def _safe_read_table(self, file_path: Path) -> pd.DataFrame | None:
        try:
            return self._normalise_columns(self._read_table(file_path))
        except Exception:
            logger.exception("ComparisonAgent: failed to read file %s", file_path)
            return None

    def _read_table(self, file_path: Path) -> pd.DataFrame:
        if file_path.suffix.lower() == ".csv":
            return pd.read_csv(file_path)
        return pd.read_excel(file_path)

    def _normalise_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        normalized = df.copy()
        base_columns = [
            str(column).strip().lower().replace("\n", " ").replace("\r", " ")
            for column in normalized.columns
        ]
        seen: dict[str, int] = {}
        unique_columns: list[str] = []
        for column in base_columns:
            seen[column] = seen.get(column, 0) + 1
            if seen[column] == 1:
                unique_columns.append(column)
            else:
                unique_columns.append(f"{column}__dup{seen[column]}")
        normalized.columns = unique_columns
        return normalized

    # Section 10: Column matching helpers
    def _find_column(self, columns: Iterable[str], candidates: list[str]) -> str | None:
        matches = self._find_matching_columns(columns, candidates)
        return matches[0] if matches else None

    def _require_column(
        self, columns: Iterable[str], candidates: list[str], label: str
    ) -> str:
        column = self._find_column(columns, candidates)
        if not column:
            raise ValueError(f"Could not find a {label} column.")
        return column

    def _normalize_column_name(self, value: object) -> str:
        base_value = str(value).split("__dup", 1)[0]
        return "".join(char for char in base_value.strip().upper() if char.isalnum())

    def _find_matching_columns(
        self, columns: Iterable[str], candidates: list[str]
    ) -> list[str]:
        candidate_keys = {
            self._normalize_column_name(candidate) for candidate in candidates
        }
        return [
            column
            for column in columns
            if self._normalize_column_name(column) in candidate_keys
        ]

    # Section 11: Column scoring helpers for messy history files
    def _get_column_values(self, df: pd.DataFrame, column_name: str) -> pd.Series:
        selected = df.loc[:, column_name]
        if isinstance(selected, pd.DataFrame):
            return selected.apply(self._coerce_scalar, axis=1)
        return selected.apply(self._coerce_scalar)

    def _get_row_value(self, row: pd.Series, column_name: str | None) -> object:
        if not column_name or column_name not in row.index:
            return ""
        return self._coerce_scalar(row[column_name])

    def _select_best_order_column(self, df: pd.DataFrame) -> str | None:
        candidates = self._find_matching_columns(df.columns, self.ORDER_CANDIDATES)
        return self._select_best_column_by_score(
            df, candidates, self._score_order_column
        )

    def _select_best_instructor_column(self, df: pd.DataFrame) -> str | None:
        candidates = self._find_matching_columns(df.columns, self.INSTRUCTOR_CANDIDATES)
        return self._select_best_column_by_score(
            df, candidates, self._score_instructor_column
        )

    def _select_best_apply_revenue_column(self, df: pd.DataFrame) -> str | None:
        candidates = self._find_matching_columns(
            df.columns, self.APPLY_REVENUE_CANDIDATES
        )
        return self._select_best_column_by_score(
            df, candidates, self._score_apply_revenue_column
        )

    def _select_best_business_unit_column(self, df: pd.DataFrame) -> str | None:
        candidates = self._find_matching_columns(
            df.columns, self.BUSINESS_UNIT_CANDIDATES
        )
        return self._select_best_column_by_score(
            df, candidates, self._score_business_unit_column
        )

    def _select_best_column_by_score(
        self,
        df: pd.DataFrame,
        candidates: list[str],
        scorer,
    ) -> str | None:
        if not candidates:
            return None
        scored = [
            (scorer(self._get_column_values(df, column)), index, column)
            for index, column in enumerate(candidates)
        ]
        scored.sort(key=lambda item: (-item[0], item[1]))
        return scored[0][2]

    def _score_order_column(self, series: pd.Series) -> float:
        values = [
            self._clean_output_value(value)
            for value in series.tolist()
            if self._clean_output_value(value)
        ]
        if not values:
            return 0.0
        numeric_ratio = sum(
            1 for value in values if value.replace(",", "").isdigit()
        ) / len(values)
        return numeric_ratio

    def _score_instructor_column(self, series: pd.Series) -> float:
        values = [
            self._clean_output_value(value)
            for value in series.tolist()
            if self._clean_output_value(value)
        ]
        if not values:
            return 0.0
        alpha_ratio = sum(
            1
            for value in values
            if any(ch.isalpha() for ch in value)
            and not any(ch.isdigit() for ch in value)
        ) / len(values)
        name_like_ratio = sum(1 for value in values if " " in value.strip()) / len(
            values
        )
        return (alpha_ratio * 2) + name_like_ratio

    def _score_apply_revenue_column(self, series: pd.Series) -> float:
        values = [
            self._clean_output_value(value)
            for value in series.tolist()
            if self._clean_output_value(value)
        ]
        if not values:
            return 0.0
        numeric_like = sum(
            1 for value in values if value.replace(",", "").isdigit()
        ) / len(values)
        short_code_penalty = sum(1 for value in values if len(value) <= 3) / len(values)

        score = (numeric_like * 3) - short_code_penalty

        # If NO numeric values found, this is not an apply_revenue column
        if numeric_like == 0.0:
            return -999.0

        return score

    def _score_business_unit_column(self, series: pd.Series) -> float:
        values = [
            self._clean_output_value(value)
            for value in series.tolist()
            if self._clean_output_value(value)
        ]
        if not values:
            return 0.0

        # Score based on valid business unit patterns:
        # - Short codes with letters+digits (BU001, HR05, A0903, P6066, etc.)
        # - No spaces, no long text
        valid_code_ratio = sum(
            1
            for value in values
            if len(value) >= 2
            and len(value) <= 10
            and value[0].isalpha()
            and " " not in value
            and any(ch.isdigit() for ch in value)
        ) / len(values)

        # Heavy penalty for columns with long text or spaces
        long_text_penalty = sum(
            1 for value in values if len(value) > 15 or " " in value.strip()
        ) / len(values)

        score = (valid_code_ratio * 5) - (long_text_penalty * 3)

        # If NO valid codes found at all, this is not a business unit column
        if valid_code_ratio == 0.0:
            return -999.0

        return score

    def _coerce_scalar(self, value: object) -> object:
        if isinstance(value, pd.DataFrame):
            if value.empty:
                return ""
            return self._coerce_scalar(value.iloc[0, 0])
        if isinstance(value, pd.Series):
            for item in value.tolist():
                if pd.notna(item) and str(item).strip():
                    return item
            return ""
        return value

    def _normalize_text(self, value: object) -> str:
        value = self._coerce_scalar(value)
        if pd.isna(value):
            return ""
        text = str(value).strip()
        if text.endswith(".0"):
            text = text[:-2]
        return " ".join(text.upper().split())

    def _clean_output_value(self, value: object) -> str:
        value = self._coerce_scalar(value)
        if pd.isna(value):
            return ""
        return str(value).strip()

    def _parse_amount(self, value: object) -> float:
        value = self._coerce_scalar(value)
        if pd.isna(value):
            return 0.0
        text = str(value).strip().replace(",", "")
        if not text:
            return 0.0
        try:
            return float(text)
        except ValueError:
            return 0.0

    def _extract_apply_revenue(self, row: pd.Series, column_name: str | None) -> str:
        if not column_name:
            return ""
        value = self._clean_output_value(self._get_row_value(row, column_name))
        if not value:
            return ""
        # Apply revenue must be purely numeric (can have commas)
        # Examples: 1000, 5000, 25000
        normalized = value.replace(",", "").strip()
        return normalized if normalized.isdigit() and len(normalized) > 0 else ""

    def _extract_business_unit(self, row: pd.Series, column_name: str | None) -> str:
        if not column_name:
            return ""
        value = self._clean_output_value(self._get_row_value(row, column_name))
        if not value:
            return ""
        # Business unit must be a SHORT code: 2-10 chars, alphanumeric, no spaces
        # Pattern: starts with letter, contains mix of letters+digits (e.g., BU001, HR05)
        # Reject long descriptive text like "Instructor Led Class"
        if (
            len(value) >= 2
            and len(value) <= 10
            and value[0].isalpha()
            and " " not in value
            and any(ch.isdigit() for ch in value)  # Must have at least one digit
        ):
            return value
        return ""

    # Section 12: Value normalization helpers
    def _normalize_user_type(self, value: object) -> str:
        normalized = self._normalize_text(value)
        if normalized in {"C", "CORP"}:
            return "CORP"
        if normalized in {"H", "F", "NON-CORP", "NON CORP", "NONCORP"}:
            return "NON-CORP"
        return "NON-CORP"

    def _derive_billing_region(
        self, region_value: object, country_value: object
    ) -> str:
        region = self._normalize_region_token(region_value)
        country = self._normalize_text(country_value)

        if region == "AMER":
            return "AMER" if country in AMER_COUNTRIES else "MEXICO"
        if region in {"GC", "APAC", "MEXICO", "EMEAA", "AMEA"}:
            return region
        return region

    def _normalize_region_token(self, value: object) -> str:
        region = self._normalize_text(value)
        if "APAC GC" in region:
            return "GC"
        if "GC" in region:
            return "GC"
        if "APAC" in region:
            return "APAC"
        if region in {"EMEA", "EMEAA"}:
            return "EMEAA"
        if region in {"AMEA", "AMER", "MEXICO"}:
            return region
        return region

    def _detect_region_and_type_from_path(
        self, file_path: Path
    ) -> tuple[str | None, str | None]:
        text = " ".join(part.upper() for part in file_path.parts)

        region = None
        if "APAC GC" in text:
            region = "GC"
        elif " GC " in f" {text} ":
            region = "GC"
        elif "APAC" in text:
            region = "APAC"
        elif "MEXICO" in text:
            region = "MEXICO"
        elif "EMEAA" in text or "EMEA" in text:
            region = "EMEAA"
        elif "AMEA" in text:
            region = "AMEA"
        elif "AMER" in text:
            region = "AMER"

        billing_type = None
        if (
            "NON-CORP" in text
            or "NON CORP" in text
            or "NONCORP" in text
            or "NONCROP" in text
        ):
            billing_type = "NON-CORP"
        elif "CROP" in text or "CORP" in text:
            billing_type = "CORP"

        return region, billing_type

    # Section 13: Output cleanup helpers
    def _drop_helper_columns(self, df: pd.DataFrame) -> None:
        """Remove temporary helper columns."""
        helpers = [
            "_amount_value",
            "_billing_type",
            "_source_region",
            "_country_value",
            "_billing_region",
            "_normalized_order",
            "_matched_in_history",
        ]
        removable = [col for col in helpers if col in df.columns]
        if removable:
            df.drop(columns=removable, inplace=True, errors="ignore")

    def _default_output_name(self) -> str:
        return f"Monthly Billing Records ({datetime.now().strftime('%B %Y')}).xlsx"


# Section 14: Public entry point used by API / other modules
def run_monthly_comparison(
    source_file_path: str | Path | None = None,
    output_file_path: str | Path | None = None,
) -> dict[str, object]:
    """Entry point for API/modules."""
    agent = ComparisonAgent()
    return agent.run(
        source_file_path=source_file_path, output_file_path=output_file_path
    )


def cleaning_data_prosessing():
    print("Cleaning Data Processing flow Initiated...............................")
    # Define paths for history and monthly data
    crop_dir = Path("data/History_data/Crop")
    noncrop_dir = Path("data/History_data/NonCrop")
    history_dir = Path("data/History_data")
    monthly_data_dir = Path("data/Monthly_data")
    output_path = Path("output/Monthly_cleaned_report")

    # Collect monthly billing files
    files = [
        f
        for f in monthly_data_dir.iterdir()
        if f.is_file() and f.suffix.lower() in {".csv", ".xlsx", ".xls"}
    ]
    if not files:
        raise FileNotFoundError(f"No input files found in {monthly_data_dir}")

    output_path.mkdir(parents=True, exist_ok=True)

    input_file = files[0]

    if input_file.suffix.lower() == ".csv":
        df = pd.read_csv(input_file)
    else:
        df = pd.read_excel(input_file)

    cleaner = CleaningAgent()
    cleaned_df = cleaner.run(df, cost_center_df=None)

    # Initialize ComparisonAgent with paths from this function
    comparison_agent = ComparisonAgent(
        monthly_dir=monthly_data_dir,
        history_dir=history_dir,
        apac_country_file="data/APAC COUNTRIES.xlsx",
        output_dir=output_path,
    )

    month_year = datetime.now().strftime("%B %Y")
    legacy_output_file = output_path / "filtered_non_zero_data.xlsx"
    if legacy_output_file.exists():
        legacy_output_file.unlink()

    output_file = output_path / f"Monthly Billing Records ({month_year}).xlsx"

    # Run comparison with cleaned data
    result = comparison_agent.run(
        source_file_path=input_file,
        output_file_path=output_file,
    )

    print("Cleaning Data Processing flow Completed...............................")
    return True
