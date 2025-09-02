# ETL Pipeline for Trading Data

This project implements an ETL (Extract, Transform, Load) pipeline for processing trading data from a CSV file into a SQLite database with weekly aggregations, built using [Prefect](https://www.prefect.io/) for workflow orchestration. The pipeline extracts data from `data/trades.csv`, cleans and validates it, aggregates it by week, client type, user ID, and symbol, loads it into `agg_result.db`, and generates reports and visualizations in the `output/` directory.

## Table of Contents
- [Prerequisites](#prerequisites)
- [Project Structure](#project-structure)
- [Setup Instructions](#setup-instructions)
- [Running the ETL Pipeline Manually](#running-the-etl-pipeline-manually)
- [CI/CD Workflow](#cicd-workflow)
- [Scaling to 100+ Million Rows](#scaling-to-100-million-rows)
  - [Technologies to Replace/Add](#technologies-to-replaceadd)
  - [Proposed ETL Architecture](#proposed-etl-architecture)
  - [Monitoring Metrics](#monitoring-metrics)
  - [Data Storage](#data-storage)
- [Troubleshooting](#troubleshooting)

## Prerequisites
- **Python**: Version 3.11 or higher
- **Git**: For cloning the repository
- **Operating System**: Linux, macOS, or Windows (with WSL recommended for Windows users)
- **Dependencies**: Listed in `requirements.txt`
- **Disk Space**: Ensure sufficient space for `data/trades.csv`, `agg_result.db`, and `output/` files
- **Memory**: At least 4GB RAM for local execution; more for large datasets

## Project Structure
```
ETL-process/
├── .github/
│   └── workflows/
│       └── ci.yaml           # GitHub Actions workflow for CI/CD
├── data/
│   └── trades.csv            # Input CSV file with trading data
├── docs/
│   └── README.md             # Project documentation
├── output/                   # Directory for output reports and visualizations
├── prefect/
│   └── flows/
│       ├── csv_to_db_flow.py # Main ETL flow implementation
│       └── deployments/
│           └── csv_to_db_deployment.py # Deployment configuration
├── agg_result.db             # Output SQLite database
└── requirements.txt          # Python dependencies
```

## Setup Instructions
Follow these steps to set up and run the project locally:

1. **Clone the Repository**
   ```bash
   git clone https://github.com/AndriiKachan/ETL-process
   cd ETL-process
   ```

2. **Install Dependencies**
   Install the required Python packages:
   ```bash
   pip install -r requirements.txt
   ```

3. **Start Prefect Server**
   Open a terminal (Terminal 1) and start the Prefect server:
   ```bash
   prefect server start
   ```
   Keep this terminal running. The server will be accessible at `http://127.0.0.1:4200` (this is the default URL for all users running the Prefect server locally, unless the port is customized).

4. **Set API URL**
   Open a new terminal (Terminal 2) and set the Prefect API URL:
   ```bash
   export PREFECT_API_URL=http://127.0.0.1:4200/api
   ```

5. **Create Work Pool**
   In Terminal 2, create a work pool for the deployment:
   ```bash
   prefect work-pool create "PROD" --type process
   ```

6. **Start Worker**
   In Terminal 2, start a worker to process flow runs:
   ```bash
   prefect worker start --pool PROD
   ```
   Keep this terminal running.

7. **Test Flow Directly**
   To test the ETL flow directly without a deployment, run:
   ```bash
   cd prefect/flows
   python csv_to_db_flow.py
   ```
   This executes the `csv_to_db_etl` flow with default parameters defined in the script.

8. **Run Deployment**
   To deploy the flow for scheduled or manual execution, run:
   ```bash
   cd prefect/flows
   python deployments/csv_to_db_deployment.py
   ```
   This creates a deployment named `csv-to-db-deployment` with a cron schedule (`0 * * * *` for hourly runs) and parameters pointing to `data/trades.csv`, `agg_result.db`, and `output/`.

9. **Verify in UI**
   Open your browser and navigate to:
   ```
   http://127.0.0.1:4200
   ```
   In the Prefect UI, you can:
   - Check flow runs under the "Flow Runs" tab
   - Monitor deployments under the "Deployments" tab
   - View work pools under the "Work Pools" tab

<img width="1449" height="232" alt="Screenshot 2025-09-02 at 21 49 43" src="https://github.com/user-attachments/assets/a46437f1-93f2-4466-aa5d-d84c15be7fb4" />


<img width="1709" height="867" alt="Screenshot 2025-09-02 at 21 49 56" src="https://github.com/user-attachments/assets/417b9a76-ae92-4e61-b967-01ae3dd1b762" />


## Running the ETL Pipeline Manually
To manually deploy the ETL pipeline, navigate to the `prefect/flows` directory and run:
```bash
cd prefect/flows
python deployments/csv_to_db_deployment.py
```

This deploys the `csv-to-db-deployment` with the configured parameters for scheduled or manual execution via the Prefect server.

## CI/CD Workflow
The project includes a GitHub Actions workflow defined in `.github/workflows/ci.yaml`. Here's how it works:

- **Triggers**:
  - Runs on `push` to the `main` branch.
  - Supports manual triggering via `workflow_dispatch` with an `environment` input (`development`, `staging`, or `production`).

- **Steps**:
  1. **Checkout Code**: Clones the repository.
  2. **Set up Python**: Configures Python 3.11.
  3. **Install Dependencies**: Installs packages from `requirements.txt`.
  4. **Create Directories**: Sets up `data/` and `output/` directories.
  5. **Run ETL Pipeline**: Executes the `csv_to_db_etl` flow with environment variables for input/output paths.
  6. **Upload Artifacts**: Uploads `output/` files and `*.db` files as artifacts, retained for 7 days.

 Click the link to download the uploaded data:
 
<img width="1370" height="696" alt="Screenshot 2025-09-02 at 21 56 18" src="https://github.com/user-attachments/assets/39d64a37-54d7-4e40-ac4d-b1123d9ec4e3" />


- **Environment Handling**:
  The workflow uses the `environment` input to set the `ENVIRONMENT` variable, defaulting to `production`. This allows environment-specific configurations if extended in the future.

To trigger the workflow manually:
1. Go to the repository on GitHub.
2. Navigate to the "Actions" tab.
3. Select the "ETL Pipeline" workflow.
4. Click "Run workflow" and choose an environment (`development`, `staging`, or `production`).

## Scaling to 100+ Million Rows
To handle datasets with 100+ million rows, the current solution (using Pandas, SQLite, and local file storage) may face performance and scalability limitations. Below are recommendations for adapting the solution.

### Technologies to Replace/Add
- **Replace Pandas with Dask or Spark**:
  - **Dask**: For distributed computing on large datasets, enabling parallel processing across multiple cores or clusters. Suitable for Python-centric workflows.
  - **Apache Spark**: For massive datasets, Spark's distributed processing is more robust, with built-in fault tolerance and scalability. Use PySpark for integration with Python.
- **Replace SQLite with a Distributed Database**:
  - **PostgreSQL**: For structured data with strong consistency, suitable for transactional workloads.
  - **Snowflake/Redshift/BigQuery**: Cloud-based data warehouses optimized for analytical queries and large-scale data storage.
- **Add Distributed Storage**:
  - Use cloud storage like AWS S3, Google Cloud Storage, or Azure Blob Storage for input CSV files and output reports to handle large-scale data storage and access.
- **Add Workflow Orchestration Enhancements**:
  - Use Prefect Cloud instead of a local Prefect server for better scalability, monitoring, and fault tolerance.
  - Consider Apache Airflow for complex dependencies or Kubernetes-based orchestration for containerized workflows.
- **Add Caching/Optimization Layers**:
  - Introduce Redis or Memcached for caching intermediate results.
  - Use Parquet or ORC file formats instead of CSV for faster I/O and compression.

### Proposed ETL Architecture
For 100+ million rows, adopt a distributed, cloud-native architecture:
1. **Data Ingestion**:
   - Store input CSV files in cloud storage (e.g., S3).
   - Use a message queue (e.g., Kafka, AWS SQS) for streaming data ingestion if real-time processing is needed.
2. **Extraction**:
   - Use Dask or Spark to read and process large CSV files in parallel.
   - Partition input data into smaller chunks for distributed processing.
3. **Transformation**:
   - Perform cleaning, validation, and aggregation using Dask DataFrames or Spark DataFrames.
   - Distribute computations across a cluster (e.g., AWS EMR, Databricks, or Kubernetes).
   - Cache intermediate results in memory (e.g., Spark RDDs) or temporary storage (e.g., Parquet files).
4. **Loading**:
   - Write aggregated data to a cloud data warehouse (e.g., Snowflake).
   - Use bulk loading mechanisms (e.g., Snowflake's `COPY INTO` or Redshift's `COPY`) for efficiency.
5. **Reporting**:
   - Generate reports using distributed compute engines (e.g., Spark for aggregations).
   - Store visualizations and CSV reports in cloud storage.
   - Serve reports via a web interface (e.g., Flask or FastAPI) or BI tools (e.g., Tableau, Power BI).
6. **Orchestration**:
   - Use Prefect Cloud or Airflow to schedule and monitor ETL jobs.
   - Deploy workers on Kubernetes for scalability.
   - Implement retry mechanisms and error handling for robustness.

### Monitoring Metrics
To ensure reliability and performance, implement the following metrics:
- **Pipeline Performance**:
  - **Execution Time**: Total runtime and per-task runtime (extract, clean, transform, load, report).
  - **Throughput**: Number of rows processed per second.
  - **Latency**: Time from data ingestion to output availability.
- **Data Quality**:
  - **Raw Row Count**: Number of rows in input CSV.
  - **Cleaned Row Count**: Number of rows after cleaning/validation.
  - **Filtered Rows**: Number of rows dropped due to invalid data.
  - **Aggregated Row Count**: Number of rows in final output.
  - **Missing Values**: Count of missing or invalid values per column.
- **System Health**:
  - **Memory Usage**: Peak memory consumption during processing.
  - **CPU Usage**: CPU load across workers or cluster nodes.
  - **Disk I/O**: Read/write rates for input/output files.
- **Error Metrics**:
  - **Error Rate**: Number of failed runs or tasks.
  - **Error Types**: Categorize errors (e.g., file not found, database connection issues).
  - **Retry Attempts**: Number of retries per task.
- **Output Metrics**:
  - **Files Created**: Number and size of output files (CSVs, PNGs, DB files).
  - **Database Size**: Size of `agg_result.db` or equivalent.
- **Monitoring Tools**:
  - Use Prefect Cloud's dashboard for flow run metrics.
  - Integrate with Prometheus/Grafana for system metrics.
  - Log errors and metrics to a centralized system (e.g., ELK Stack, CloudWatch).

### Data Storage
- **Input Data**:
  - **Current**: Stored in `data/trades.csv` on local disk or GitHub repository.
  - **Scaled**: Store in cloud storage (e.g., S3) as partitioned Parquet files for efficient access and scalability. Example: `s3://etl-bucket/trades/year=2025/month=09/`.
- **Output Data**:
  - **Current**: SQLite database (`agg_result.db`) and reports/visualizations in `output/`.
  - **Scaled**: Store aggregated data in a cloud data warehouse (e.g., Snowflake). Save reports and visualizations in cloud storage (e.g., S3) with structured paths (e.g., `s3://etl-bucket/output/year=2025/month=09/`).
- **Intermediate Data**:
  - Use temporary storage (e.g., S3, HDFS) for intermediate Parquet files during transformation.
  - Cache frequently accessed data in Redis or Spark memory for performance.

## Troubleshooting
- **Prefect Server Not Accessible**:
  - Ensure the server is running (`prefect server start`) and accessible at `http://127.0.0.1:4200`.
  - Check for port conflicts and use a different port if needed (e.g., `prefect server start --port 4300`).
- **Worker Not Picking Up Jobs**:
  - Verify the work pool exists (`prefect work-pool ls`) and the worker is running (`prefect worker start --pool PROD`).
  - Ensure `PREFECT_API_URL` is set correctly.
- **File Not Found Errors**:
  - Confirm `data/trades.csv` exists in the `data/` directory.
  - Check file paths in `csv_to_db_deployment.py` or manual run commands.
- **Memory Issues**:
  - For large datasets, reduce `chunk_size` in `ETLSettings` or scale to Dask/Spark.
- **Visualization Failures**:
  - Ensure `matplotlib` and `seaborn` are installed.
  - Verify write permissions in the `output/` directory.
