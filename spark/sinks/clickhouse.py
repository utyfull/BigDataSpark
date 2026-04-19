from collections import OrderedDict

from pyspark.sql import DataFrame

from config import ClickHouseConfig


class ClickHouseSink:
    def __init__(self, config: ClickHouseConfig):
        self.config = config

    def write_reports(self, reports: OrderedDict[str, DataFrame]) -> None:
        for table_name, df in reports.items():
            print(f"Writing ClickHouse report: {table_name}")
            (
                df.write
                .format("jdbc")
                .option("url", self.config.jdbc_url)
                .option("dbtable", table_name)
                .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
                .option("user", self.config.user)
                .option("password", self.config.password)
                .option("createTableOptions", "ENGINE = MergeTree() ORDER BY tuple()")
                .mode("overwrite")
                .save()
            )
