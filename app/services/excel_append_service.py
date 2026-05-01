"""Service to append matching data from a source Excel file to a target Excel file based on matching headers."""
import logging
import os
import pandas as pd
from pathlib import Path
from typing import Union
from datetime import datetime
from app.config.settings import settings

logger = logging.getLogger(__name__)

class ExcelAppendService:
    @staticmethod
    def append_matching_data(
        target_file: Union[str, Path],
        source_file: Union[str, Path],
        sheet_name: str = 0,
        output_file: Union[str, Path, None] = None,
        output_dir: Union[str, Path, None] = None
    ) -> str:
        """
        Appends rows from source_file to target_file where headers match.
        If output_file is provided, writes result there; otherwise, overwrites target_file.
        Returns the path to the updated file.
        """
        target_file = Path(target_file)
        source_file = Path(source_file)
        # Determine output directory
        if output_dir is not None:
            output_dir = Path(output_dir)
        else:
            output_dir = Path("output/history_data_output")
        output_dir.mkdir(parents=True, exist_ok=True)
        output_file = Path(output_file) if output_file else output_dir / target_file.name

        # Read both Excel files
        df_target = pd.read_excel(target_file, sheet_name=sheet_name)
        df_source = pd.read_excel(source_file, sheet_name=sheet_name)


        # Normalize column names for better matching (ignore case, whitespace, underscores, and non-alphanumeric)
        import re
        def normalize(col):
            return re.sub(r'[^a-z0-9]', '', str(col).strip().lower())

        target_cols_norm = {normalize(col): col for col in df_target.columns}
        source_cols_norm = {normalize(col): col for col in df_source.columns}

        # Find common normalized columns that exist in both original files
        common_norm = set(target_cols_norm.keys()) & set(source_cols_norm.keys())
        if not common_norm:
            raise ValueError("No matching headers found between the two files.")

        # Only include columns that exist in both original files
        common_cols = []
        source_cols = []
        for norm in common_norm:
            tgt_col = target_cols_norm[norm]
            src_col = source_cols_norm[norm]
            if tgt_col in df_target.columns and src_col in df_source.columns:
                common_cols.append(tgt_col)
                source_cols.append(src_col)


        # Prepare DataFrame for new rows: only fill values for columns common to both, others remain NaN
        df_new_rows = pd.DataFrame(columns=df_target.columns)
        for tgt_col, src_col in zip(common_cols, source_cols):
            df_new_rows[tgt_col] = df_source[src_col]

        # If 'Month' column exists in the target, fill it for new rows only
        if 'Month' in df_target.columns:
            current_month = datetime.now().strftime('%b%y')  # e.g., May26
            df_new_rows['Month'] = current_month

        # Append new rows to the original target DataFrame
        df_appended = pd.concat([df_target, df_new_rows], ignore_index=True)

        # Ensure column order matches target (should already be the case)
        df_appended = df_appended[df_target.columns]

        # Write to output file
        df_appended.to_excel(output_file, index=False)
        return str(output_file)

    def new_history_data_preparation(self):
        # --- Corp (Crop) Data ---
        try:
            logger.info("Starting new history data preparation for Corp (Crop) files............")
            target_dir = os.path.join(settings.upload_dir, "History_data", "Crop")
            source_dir = os.path.join(settings.output_dir, "Corp_NonCorp_Split")
            output_dir = os.path.join("output", "History_Data_Output", "Corp")
            obj = ExcelAppendService()
            logger.info(f"Processing Corp (Crop) files: target_dir={target_dir}, source_dir={source_dir}, output_dir={output_dir}")
            for target_file in os.listdir(target_dir):
                if not target_file.lower().endswith((".xlsx", ".xlsm")):
                    continue
                region = target_file.split("_")[0].upper()
                for source_file in os.listdir(source_dir):
                    if not source_file.lower().endswith((".xlsx", ".xlsm")):
                        continue
                    src_upper = source_file.upper()
                    if src_upper.startswith(region) and "CROP" in src_upper:
                        output_file_path = os.path.join(output_dir, target_file)
                        logger.info(f"Appending Corp: target={target_file}, source={source_file}, output={output_file_path}")
                        obj.append_matching_data(
                            target_file=os.path.join(target_dir, target_file),
                            source_file=os.path.join(source_dir, source_file),
                            output_file=output_file_path,
                            output_dir=output_dir,
                        )
                        break
            logger.info("Completed new history data preparation for Corp (Crop) files.....................")            
        except Exception as exc:
            logger.error(f"Error processing Corp (Crop) data: {exc}")
            raise            

        # --- NonCorp (NonCrop) Data ---
        try:
            logger.info("Starting new history data preparation for NonCorp (NonCrop) files............")
            target_dir_nc = os.path.join(settings.upload_dir, "History_data", "NonCrop")
            output_dir_nc = os.path.join("output", "History_Data_Output", "NonCorp")
            logger.info(f"Processing NonCorp (NonCrop) files: target_dir={target_dir_nc}, source_dir={source_dir}, output_dir={output_dir_nc}")
            for target_file in os.listdir(target_dir_nc):
                if not target_file.lower().endswith((".xlsx", ".xlsm")):
                    continue
                region = target_file.split("_")[0].upper()
                for source_file in os.listdir(source_dir):
                    if not source_file.lower().endswith((".xlsx", ".xlsm")):
                        continue
                    src_upper = source_file.upper()
                    if src_upper.startswith(region) and "NONCROP" in src_upper:
                        output_file_path = os.path.join(output_dir_nc, target_file)
                        logger.info(f"Appending NonCorp: target={target_file}, source={source_file}, output={output_file_path}")
                        obj.append_matching_data(
                            target_file=os.path.join(target_dir_nc, target_file),
                            source_file=os.path.join(source_dir, source_file),
                            output_file=output_file_path,
                            output_dir=output_dir_nc,
                        )
                        break
            logger.info("Completed new history data preparation for NonCorp (NonCrop) files.....................")            
        except Exception as exc:
            logger.error(f"Error processing NonCorp (NonCrop) data: {exc}")
            raise 
        return "New history data preparation completed successfully."
