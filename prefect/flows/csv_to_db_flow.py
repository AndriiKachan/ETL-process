"""
ETL Flow for Processing Trading Data from CSV to SQLite Database.

This module implements an ETL pipeline that extracts trading data from a CSV file,
transforms and aggregates it by week, client type, user ID, and symbol, then loads
the results into a SQLite database. It also generates reports and visualizations.
"""

from prefect import flow, task, get_run_logger
from prefect.runtime import flow_run
from prefect.tasks import task_input_hash
from datetime import timedelta
import pandas as pd
import sqlite3
from pathlib import Path
import matplotlib.pyplot as plt
import seaborn as sns
from sqlalchemy import create_engine
from typing import Dict, Any, List
import pendulum
from dataclasses import dataclass
from enum import Enum


class ClientType(Enum):
    BRONZE = "bronze"
    SILVER = "silver"
    GOLD = "gold"


@dataclass
class ETLSettings:
    """Configuration settings for the ETL pipeline."""

    input_csv: str
    output_db: str
    output_table: str
    report_output_path: str
    chunk_size: int = 1000
    retry_attempts: int = 3
    retry_delay: int = 30


@dataclass
class ExecutionMetadata:
    """Metadata container for ETL execution."""

    start_time: str
    end_time: str = ""
    status: str = "started"
    input_file: str = ""
    output_database: str = ""
    output_table: str = ""
    report_output_path: str = ""
    flow_run_id: str = ""
    raw_row_count: int = 0
    cleaned_row_count: int = 0
    filtered_rows: int = 0
    aggregated_row_count: int = 0
    files_created: List[str] = None
    error: str = ""

    def __post_init__(self):
        if self.files_created is None:
            self.files_created = []

    def to_dict(self) -> Dict[str, Any]:
        return {k: v for k, v in self.__dict__.items() if not k.startswith("_")}


class DataValidator:
    """Utility class for data validation operations."""

    REQUIRED_COLUMNS = [
        "timestamp",
        "user_id",
        "client_type",
        "symbol",
        "side",
        "quantity",
        "price",
    ]

    NUMERIC_COLUMNS = ["quantity", "price"]

    @staticmethod
    def validate_required_columns(df: pd.DataFrame, logger) -> None:
        """Validate that all required columns are present."""
        missing_columns = set(DataValidator.REQUIRED_COLUMNS) - set(df.columns)
        if missing_columns:
            error_msg = f"Missing required columns: {missing_columns}"
            logger.error(error_msg)
            raise ValueError(error_msg)

    @staticmethod
    def validate_numeric_columns(df: pd.DataFrame, logger) -> pd.DataFrame:
        """Validate and convert numeric columns."""
        df_clean = df.copy()
        for col in DataValidator.NUMERIC_COLUMNS:
            if col in df_clean.columns:
                df_clean[col] = pd.to_numeric(df_clean[col], errors="coerce")
                invalid_numeric = df_clean[col].isna().sum()
                if invalid_numeric > 0:
                    logger.warning(f"Found {invalid_numeric} invalid values in {col}")
        return df_clean


class ChartGenerator:
    """Utility class for generating visualization charts."""

    @staticmethod
    def create_weekly_volume_chart(
        df: pd.DataFrame, output_dir: Path, timestamp: str
    ) -> str:
        """Create weekly volume by client type chart."""
        weekly_volume = (
            df.groupby(["week_start_date", "client_type"])["total_volume"]
            .sum()
            .reset_index()
        )

        plt.figure(figsize=(12, 6))
        sns.lineplot(
            data=weekly_volume, x="week_start_date", y="total_volume", hue="client_type"
        )
        plt.title("Weekly Trading Volume by Client Type")
        plt.xlabel("Week Start Date")
        plt.ylabel("Total Volume")
        plt.xticks(rotation=45)
        plt.tight_layout()
        volume_chart_path = output_dir / f"weekly_volume_by_client_type_{timestamp}.png"
        plt.savefig(volume_chart_path, dpi=300, bbox_inches="tight")
        plt.close()
        return str(volume_chart_path)

    @staticmethod
    def create_pnl_distribution_chart(
        df: pd.DataFrame, output_dir: Path, timestamp: str
    ) -> str:
        """Create PnL distribution chart."""
        plt.figure(figsize=(10, 6))
        sns.histplot(data=df, x="total_pnl", hue="client_type", kde=True, bins=30)
        plt.title("PnL Distribution by Client Type")
        plt.xlabel("Profit and Loss (PnL)")
        plt.ylabel("Frequency")
        plt.tight_layout()
        pnl_chart_path = output_dir / f"pnl_distribution_{timestamp}.png"
        plt.savefig(pnl_chart_path, dpi=300, bbox_inches="tight")
        plt.close()
        return str(pnl_chart_path)


@task(
    retries=2,
    retry_delay_seconds=30,
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(hours=1),
)
def extract_csv(file_path: str) -> pd.DataFrame:
    """
    Extract trading data from a CSV file.

    Args:
        file_path (str): Path to the CSV file containing trading data.

    Returns:
        pd.DataFrame: Raw trading data extracted from the CSV file.

    Raises:
        FileNotFoundError: If the specified CSV file does not exist.
        pd.errors.EmptyDataError: If the CSV file is empty.
    """
    logger = get_run_logger()
    logger.info(f"Extracting data from CSV file: {file_path}")

    try:
        df = pd.read_csv(file_path)
        logger.info(f"Successfully extracted {len(df)} rows from {file_path}")
        return df
    except FileNotFoundError:
        logger.error(f"CSV file not found: {file_path}")
        raise
    except pd.errors.EmptyDataError:
        logger.error(f"CSV file is empty: {file_path}")
        raise


@task(
    retries=2,
    retry_delay_seconds=30,
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(hours=1),
)
def clean_and_validate_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and validate the raw trading data.

    Handles:
    - Invalid dates
    - Missing values
    - Data type conversions

    Args:
        df (pd.DataFrame): Raw trading data.

    Returns:
        pd.DataFrame: Cleaned and validated data.
    """
    logger = get_run_logger()
    logger.info("Cleaning and validating data")

    df_clean = df.copy()

    # Handle invalid dates - use multiple parsing strategies
    try:
        # First try standard format
        df_clean["timestamp"] = pd.to_datetime(df_clean["timestamp"], errors="coerce")

        # Count invalid dates
        invalid_dates = df_clean["timestamp"].isna().sum()
        if invalid_dates > 0:
            logger.warning(
                f"Found {invalid_dates} rows with invalid dates. These will be filtered out."
            )

    except Exception as e:
        logger.error(f"Date parsing failed: {str(e)}")
        raise

    # Validate numeric columns using utility class
    df_clean = DataValidator.validate_numeric_columns(df_clean, logger)

    # Filter out rows with invalid essential data
    initial_count = len(df_clean)
    df_clean = df_clean.dropna(subset=["timestamp", "user_id", "client_type", "symbol"])
    filtered_count = initial_count - len(df_clean)

    if filtered_count > 0:
        logger.warning(
            f"Filtered out {filtered_count} rows with missing essential data"
        )

    logger.info(f"Data cleaning completed. {len(df_clean)} valid rows remaining.")
    return df_clean


@task(
    retries=2,
    retry_delay_seconds=30,
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(hours=1),
)
def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform and aggregate trading data.

    This function performs the following transformations:
    1. Converts timestamp to datetime
    2. Calculates week start date (Monday)
    3. Computes PnL (Profit and Loss)
    4. Aggregates data by week, client type, user ID, and symbol

    Args:
        df (pd.DataFrame): Cleaned trading data.

    Returns:
        pd.DataFrame: Transformed and aggregated data.

    Raises:
        ValueError: If required columns are missing from the input DataFrame.
    """
    logger = get_run_logger()
    logger.info("Starting data transformation and aggregation")

    # Validate required columns using utility class
    DataValidator.validate_required_columns(df, logger)

    # Create a copy to avoid modifying the original DataFrame
    df_transformed = df.copy()

    # Calculate week start date (Monday)
    df_transformed["week_start_date"] = (
        df_transformed["timestamp"].dt.to_period("W").dt.start_time
    )
    df_transformed["week_start_date"] = df_transformed["week_start_date"].dt.date

    # Calculate PnL (Profit and Loss)
    df_transformed["pnl"] = df_transformed.apply(
        lambda x: x["price"] * x["quantity"] * (-1 if x["side"] == "buy" else 1), axis=1
    )

    # Aggregate data
    aggregated = (
        df_transformed.groupby(["week_start_date", "client_type", "user_id", "symbol"])
        .agg(
            total_volume=("quantity", "sum"),
            total_pnl=("pnl", "sum"),
            trade_count=("timestamp", "count"),
        )
        .reset_index()
    )

    logger.info(f"Aggregated data from {len(df)} rows to {len(aggregated)} groups")
    return aggregated


@task(
    retries=3,
    retry_delay_seconds=60,
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(hours=1),
)
def load_to_sqlite(df: pd.DataFrame, db_path: str, table_name: str) -> None:
    """
    Load transformed data into SQLite database.

    Args:
        df (pd.DataFrame): Transformed and aggregated data.
        db_path (str): Path to the SQLite database file.
        table_name (str): Name of the table to create or replace.

    Raises:
        sqlite3.Error: If there's an error with SQLite operations.
    """
    logger = get_run_logger()
    logger.info(f"Loading data to SQLite database: {db_path}, table: {table_name}")

    try:
        # Create database directory if it doesn't exist
        db_file = Path(db_path)
        db_file.parent.mkdir(parents=True, exist_ok=True)

        # Create SQLite connection
        engine = create_engine(f"sqlite:///{db_path}")

        # Load data to database
        df.to_sql(
            table_name,
            engine,
            if_exists="replace",
            index=False,
            method="multi",
            chunksize=1000,
        )

        logger.info(f"Successfully loaded {len(df)} rows to {table_name}")

    except sqlite3.Error as e:
        logger.error(f"SQLite error occurred: {str(e)}")
        raise


@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=1))
def generate_top_clients_report(df: pd.DataFrame, output_path: str) -> Dict[str, Any]:
    """
    Generate top clients report and visualizations.

    Args:
        df (pd.DataFrame): Aggregated trading data.
        output_path (str): Path where to save the output files.

    Returns:
        Dict[str, Any]: Dictionary containing report metadata and file paths.
    """
    logger = get_run_logger()
    logger.info("Generating top clients report and visualizations")

    # Create output directory - use the provided path directly
    output_dir = Path(output_path)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Generate timestamp for file naming
    timestamp = pendulum.now().format("YYYYMMDD_HHmmss")

    report_metadata = {
        "output_dir": str(output_dir),
        "created_at": pendulum.now().to_iso8601_string(),
        "files_created": [],
    }

    # Check if we have bronze clients
    if ClientType.BRONZE.value not in df["client_type"].unique():
        logger.warning("No bronze clients found in the data")
        return report_metadata

    # Filter for bronze clients
    bronze_clients = df[df["client_type"] == ClientType.BRONZE.value]

    if bronze_clients.empty:
        logger.warning("No bronze clients found in the data")
        return report_metadata

    # Aggregate by user_id for top clients
    top_clients = (
        bronze_clients.groupby("user_id")
        .agg({"total_volume": "sum", "total_pnl": "sum", "trade_count": "sum"})
        .reset_index()
    )

    # Get top 3 by volume with timestamp in filename
    top_volume = top_clients.nlargest(3, "total_volume")
    volume_csv_path = str(output_dir / f"top_clients_volume_{timestamp}.csv")
    top_volume.to_csv(volume_csv_path, index=False)
    report_metadata["files_created"].append(volume_csv_path)

    # Get top 3 by PnL with timestamp in filename
    top_pnl = top_clients.nlargest(3, "total_pnl")
    pnl_csv_path = str(output_dir / f"top_clients_pnl_{timestamp}.csv")
    top_pnl.to_csv(pnl_csv_path, index=False)
    report_metadata["files_created"].append(pnl_csv_path)

    # Create charts
    chart_files = create_charts(df, output_dir, timestamp)
    report_metadata["files_created"].extend(chart_files)

    logger.info(f"Report generated with {len(report_metadata['files_created'])} files")
    return report_metadata


def create_charts(df: pd.DataFrame, output_dir: Path, timestamp: str) -> List[str]:
    """
    Create visualization charts from aggregated data.

    Args:
        df (pd.DataFrame): Aggregated trading data.
        output_dir (Path): Directory where to save the charts.
        timestamp (str): Timestamp for file naming.

    Returns:
        list: List of paths to created chart files.
    """
    chart_files = []

    try:
        # Use ChartGenerator utility class
        volume_chart = ChartGenerator.create_weekly_volume_chart(
            df, output_dir, timestamp
        )
        pnl_chart = ChartGenerator.create_pnl_distribution_chart(
            df, output_dir, timestamp
        )

        chart_files.extend([volume_chart, pnl_chart])

    except Exception as e:
        get_run_logger().warning(f"Chart creation failed: {str(e)}")

    return chart_files


@flow(
    name="csv-to-db-etl-pipeline",
    description="ETL pipeline for processing trading data from CSV to SQLite database with weekly aggregation",
    retries=2,
    retry_delay_seconds=60,
    timeout_seconds=3600,
    log_prints=True,
    persist_result=True,
)
def csv_to_db_etl(
    input_csv: str,
    output_db: str,
    output_table: str,
    report_output_path: str,
) -> Dict[str, Any]:
    """
    Main ETL flow for processing trading data from CSV to SQLite database.

    This flow performs the following steps:
    1. Extract: Read trading data from CSV file
    2. Clean: Handle invalid data, missing values, and data validation
    3. Transform: Clean, process, and aggregate data by week, client type, user ID, and symbol
    4. Load: Store aggregated results in SQLite database
    5. Report: Generate top clients report and visualizations

    Args:
        input_csv (str): Path to the input CSV file.
        output_db (str): Path to the output SQLite database.
        output_table (str): Name of the output table.
        report_output_path (str): Directory path for the output reports.

    Returns:
        Dict[str, Any]: Metadata about the ETL execution including file paths and statistics.

    Raises:
        Exception: If any step in the ETL process fails.
    """
    logger = get_run_logger()
    logger.info("Starting CSV to DB ETL pipeline")

    # Initialize execution metadata
    execution_metadata = ExecutionMetadata(
        start_time=pendulum.now().to_iso8601_string(),
        input_file=input_csv,
        output_database=output_db,
        output_table=output_table,
        report_output_path=report_output_path,
        flow_run_id=flow_run.id if flow_run else "unknown",
    )

    try:
        # Extract phase
        raw_data = extract_csv(input_csv)
        execution_metadata.raw_row_count = len(raw_data)

        # Clean phase
        cleaned_data = clean_and_validate_data(raw_data)
        execution_metadata.cleaned_row_count = len(cleaned_data)
        execution_metadata.filtered_rows = (
            execution_metadata.raw_row_count - execution_metadata.cleaned_row_count
        )

        # Transform phase
        transformed_data = transform_data(cleaned_data)
        execution_metadata.aggregated_row_count = len(transformed_data)

        # Load phase
        load_to_sqlite(transformed_data, output_db, output_table)

        # Report generation phase
        report_metadata = generate_top_clients_report(
            transformed_data, report_output_path
        )
        execution_metadata.files_created = report_metadata.get("files_created", [])

        execution_metadata.end_time = pendulum.now().to_iso8601_string()
        execution_metadata.status = "success"

        logger.info("ETL pipeline completed successfully")
        return execution_metadata.to_dict()

    except Exception as e:
        execution_metadata.end_time = pendulum.now().to_iso8601_string()
        execution_metadata.status = "failed"
        execution_metadata.error = str(e)

        logger.error(f"ETL pipeline failed: {str(e)}")
        raise


# Utility function for running the flow with configuration
def run_etl_with_config(config: ETLSettings) -> Dict[str, Any]:
    """
    Run the ETL pipeline with a configuration object.

    Args:
        config (ETLSettings): Configuration settings for the ETL pipeline.

    Returns:
        Dict[str, Any]: Execution metadata.
    """
    return csv_to_db_etl(
        input_csv=config.input_csv,
        output_db=config.output_db,
        output_table=config.output_table,
        report_output_path=config.report_output_path,
    )
