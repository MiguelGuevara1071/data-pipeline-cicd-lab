import pytest

from src.ingestion import create_spark_session, ingest_csv


@pytest.fixture(scope="session")
def spark():
    session = create_spark_session()
    yield session
    session.stop()


def test_ingest_csv_returns_dataframe(spark):
    file_path = "data/transactions.csv"

    df = ingest_csv(spark, file_path)

    assert df.count() == 10
    assert "transaction_id" in df.columns
    assert "customer_id" in df.columns
    assert "amount" in df.columns


def test_ingest_csv_raises_error_for_missing_file(spark):
    with pytest.raises(FileNotFoundError):
        ingest_csv(spark, "data/nonexistent.csv")


def test_ingest_csv_uses_correct_schema(spark):
    file_path = "data/transactions.csv"

    df = ingest_csv(spark, file_path)

    assert df.schema["transaction_id"].dataType.simpleString() == "int"
    assert df.schema["customer_id"].dataType.simpleString() == "int"
    assert df.schema["amount"].dataType.simpleString() == "double"
    assert df.schema["currency"].dataType.simpleString() == "string"
    assert df.schema["transaction_date"].dataType.simpleString() == "date"