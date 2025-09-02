import os
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent.parent))

from csv_to_db_flow import csv_to_db_etl

if __name__ == "__main__":
    # Get base path from environment variable or use default
    base_path = Path(os.getenv("ETL_BASE_PATH", "/Users/andriikachan/ETL-process"))

    deployment = csv_to_db_etl.from_source(
        source=str(Path(__file__).parent.parent),
        entrypoint="csv_to_db_flow.py:csv_to_db_etl",
    ).to_deployment(
        name="csv-to-db-deployment",
        work_pool_name="PROD",
        cron="0 * * * *",
        parameters={
            "input_csv": str(base_path / "data" / "trades.csv"),
            "output_db": str(base_path / "agg_result.db"),
            "output_table": "agg_trades_weekly",
            "report_output_path": str(base_path / "output"),
        },
        tags=["etl", "trading", "production"],
    )

    deployment.apply()
    print(f"Deployment created successfully: {deployment.name}")
