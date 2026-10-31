from pathlib import Path
from pyspark.sql import SparkSession, DataFrame
from src.schemas import TRANSACTION_SCHEMA

def create_spark_session() -> SparkSession:
    """Create and return a Spark session."""
    return (
        SparkSession.builder
        .appName("DataPipelineCICDLab")
        .master("local[*]")
        .getOrCreate()
    )


def ingest_csv(spark: SparkSession, file_path: str) -> DataFrame:
    """Read a CSV file and return a Spark DataFrame."""
    path = Path(file_path)

    if not path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")

    return (
        spark.read
        .option("header", True)
        .schema(TRANSACTION_SCHEMA)
        .csv(str(path))
    )