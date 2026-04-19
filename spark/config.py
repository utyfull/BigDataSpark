import os
from dataclasses import dataclass


def env(name: str, default: str) -> str:
    return os.getenv(name, default)


@dataclass(frozen=True)
class PostgresConfig:
    host: str = env("POSTGRES_HOST", "postgres-db")
    port: str = env("POSTGRES_PORT", "5432")
    database: str = env("POSTGRES_DB", "spark_lab")
    user: str = env("POSTGRES_USER", "postgres")
    password: str = env("POSTGRES_PASSWORD", "postgres")

    @property
    def jdbc_url(self) -> str:
        return f"jdbc:postgresql://{self.host}:{self.port}/{self.database}"

    @property
    def jdbc_properties(self) -> dict:
        return {
            "user": self.user,
            "password": self.password,
            "driver": "org.postgresql.Driver",
        }


@dataclass(frozen=True)
class ClickHouseConfig:
    jdbc_url: str = env("CLICKHOUSE_URL", "jdbc:clickhouse://clickhouse:8123/default")
    user: str = env("CLICKHOUSE_USER", "clickhouse")
    password: str = env("CLICKHOUSE_PASSWORD", "clickhouse")


DATA_PATH = env("MOCK_DATA_PATH", "/mock_data/*.csv")
