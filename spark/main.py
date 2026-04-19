from config import ClickHouseConfig, PostgresConfig
from postgres_star_etl import PostgresStarEtl
from reports import ReportBuilder
from sinks.clickhouse import ClickHouseSink
from spark_session import create_spark_session


def main() -> None:
    spark = create_spark_session()
    postgres = PostgresConfig()

    try:
        print("=== Step 1: CSV -> PostgreSQL star schema ===")
        PostgresStarEtl(spark, postgres).run()

        print("=== Step 2: Build report DataFrames ===")
        reports = ReportBuilder(spark, postgres).build()
        print(f"Built {len(reports)} report tables")

        print("=== Step 3: Write reports to ClickHouse ===")
        ClickHouseSink(ClickHouseConfig()).write_reports(reports)

        print("=== BigDataSpark lab completed successfully ===")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
