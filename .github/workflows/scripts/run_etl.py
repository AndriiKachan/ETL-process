import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent / "prefect" / "flows"))

from csv_to_db_flow import csv_to_db_etl

if __name__ == "__main__":
    result = csv_to_db_etl(
        input_csv=os.environ["INPUT_CSV"],
        output_db=os.environ["OUTPUT_DB"],
        output_table=os.environ["OUTPUT_TABLE"],
        report_output_path=os.environ["REPORT_OUTPUT_PATH"],
    )
    print("ETL execution result:", result)
