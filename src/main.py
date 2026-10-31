
from src.ingestion import create_spark_session, ingest_csv
from src.transformation import transform_transactions


def main():
    spark = create_spark_session()

    try:
        input_path = "data/transactions.csv"

        # 1. Ingest data
        raw_df = ingest_csv(spark, input_path)

        print("=== RAW TRANSACTIONS ===")
        raw_df.show()

        # 2. Transform data
        transformed_df = transform_transactions(raw_df)

        print("=== TRANSFORMED TRANSACTIONS ===")
        transformed_df.show()

        print(f"Total transformed records: {transformed_df.count()}")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
