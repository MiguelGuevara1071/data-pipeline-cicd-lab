
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, upper, when, create_map, lit


EXCHANGE_RATES = {
    "COP": 0.00025,
    "USD": 1.0,
    "EUR": 1.10,
}


def transform_transactions(df: DataFrame) -> DataFrame:
    """
    Applies business rules to transaction data.

    Rules:
    - Remove duplicated transactions.
    - Keep records with valid positive amounts.
    - Standardize currency to uppercase.
    - Classify transactions by amount.
    """

    transformed_df = (
        df
        .dropDuplicates(["transaction_id"])
        .filter(
            col("amount").isNotNull()
            & (col("amount") > 0)
        )
        .withColumn("currency", upper(col("currency")))
        .withColumn(
            "amount_in_usd",
            col("amount") * create_map(
                *[
                    item
                    for pair in EXCHANGE_RATES.items()
                    for item in (lit(pair[0]), lit(pair[1]))
                ]
            )[col("currency")]
        )
        .withColumn(
            "transaction_type",
            when(col("amount_in_usd") < 100, "LOW")
            .when(col("amount_in_usd") < 1000, "MEDIUM")
            .otherwise("HIGH")
        )
    )

    return transformed_df
