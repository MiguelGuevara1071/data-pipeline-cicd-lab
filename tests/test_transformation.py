
import pytest
from datetime import date
from pyspark.sql import SparkSession

from src.transformation import transform_transactions


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder
        .appName("TransformationTests")
        .master("local[*]")
        .getOrCreate()
    )

    yield session
    session.stop()


@pytest.fixture
def transactions_df(spark):
    data = [
        (1001, 101, 250000.0, "COP", date(2026, 10, 28)),
        (1002, 102, 150000.0, "COP", date(2026, 10, 28)),
        (1003, 103, 320.50, "USD", date(2026, 10, 29)),
        (1004, 104, 780000.0, "COP", date(2026, 10, 29)),
        (1005, 105, 120.75, "EUR", date(2026, 10, 30)),
        (1006, 106, 500000.0, "COP", date(2026, 10, 30)),
        (1007, 107, 200.00, "USD", date(2026, 10, 31)),
        (1008, 108, 300000.0, "COP", date(2026, 10, 31)),
        (1009, 109, 450.00, "EUR", date(2026, 11, 1)),
        (1010, 110, 600000.0, "COP", date(2026, 11, 1)),
        (1011, 111, 100.00, "USD", date(2026, 11, 2)),
        (1012, 112, 250000.0, "COP", date(2026, 11, 2)),
        (1013, 113, 75.50, "EUR", date(2026, 11, 3)),
        (1014, 114, 400000.0, "COP", date(2026, 11, 3)),
        (1015, 115, 150.00, "USD", date(2026, 11, 4)),
    ]

    columns = [
        "transaction_id",
        "customer_id",
        "amount",
        "currency",
        "transaction_date",
    ]

    return spark.createDataFrame(data, columns)


def test_transform_transactions_classifies_amounts(transactions_df):
    result = transform_transactions(transactions_df)

    transactions = {
        row.transaction_id: row.transaction_type
        for row in result.collect()
    }

    assert transactions[1001] == "LOW"
    assert transactions[1003] == "MEDIUM"
    assert transactions[1013] == "LOW"


def test_transform_transactions_preserves_all_columns(transactions_df):
    result = transform_transactions(transactions_df)

    expected_columns = [
        "transaction_id",
        "customer_id",
        "amount",
        "currency",
        "transaction_date",
        "amount_in_usd",
        "transaction_type",
    ]

    assert result.columns == expected_columns


def test_transform_transactions_removes_duplicates(transactions_df):
    duplicated_df = transactions_df.unionByName(
        transactions_df.limit(1)
    )

    result = transform_transactions(duplicated_df)

    assert result.count() == 15


def test_transform_transactions_filters_invalid_amounts(spark):
    data = [
        (1001, 101, 250000.0, "COP", date(2026, 10, 28)),
        (1002, 102, -100.0, "COP", date(2026, 10, 28)),
        (1003, 103, 0.0, "USD", date(2026, 10, 29)),
        (1004, 104, None, "EUR", date(2026, 10, 29)),
    ]

    columns = [
        "transaction_id",
        "customer_id",
        "amount",
        "currency",
        "transaction_date",
    ]

    df = spark.createDataFrame(data, columns)

    result = transform_transactions(df)

    assert result.count() == 1
    assert result.first().transaction_id == 1001


def test_transform_transactions_standardizes_currency(transactions_df):
    result = transform_transactions(transactions_df)

    currencies = {
        row.currency
        for row in result.select("currency").distinct().collect()
    }

    assert currencies == {"COP", "USD", "EUR"}


def test_transform_transactions_converts_amounts_to_usd(transactions_df):
    result = transform_transactions(transactions_df)

    amounts = {
        row.transaction_id: row.amount_in_usd
        for row in result.collect()
    }

    assert amounts[1001] == pytest.approx(62.50)
    assert amounts[1003] == pytest.approx(320.50)
    assert amounts[1005] == pytest.approx(132.825)
