from functools import reduce

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    coalesce,
    col,
    concat_ws,
    date_format,
    dayofmonth,
    input_file_name,
    lit,
    month,
    quarter,
    regexp_extract,
    sha2,
    to_date,
    trim,
    year,
)
from pyspark.sql.types import DecimalType, IntegerType, StringType, StructField, StructType

from config import DATA_PATH, PostgresConfig


RAW_SCHEMA = StructType([
    StructField("id", StringType()),
    StructField("customer_first_name", StringType()),
    StructField("customer_last_name", StringType()),
    StructField("customer_age", StringType()),
    StructField("customer_email", StringType()),
    StructField("customer_country", StringType()),
    StructField("customer_postal_code", StringType()),
    StructField("customer_pet_type", StringType()),
    StructField("customer_pet_name", StringType()),
    StructField("customer_pet_breed", StringType()),
    StructField("seller_first_name", StringType()),
    StructField("seller_last_name", StringType()),
    StructField("seller_email", StringType()),
    StructField("seller_country", StringType()),
    StructField("seller_postal_code", StringType()),
    StructField("product_name", StringType()),
    StructField("product_category", StringType()),
    StructField("product_price", StringType()),
    StructField("product_quantity", StringType()),
    StructField("sale_date", StringType()),
    StructField("sale_customer_id", StringType()),
    StructField("sale_seller_id", StringType()),
    StructField("sale_product_id", StringType()),
    StructField("sale_quantity", StringType()),
    StructField("sale_total_price", StringType()),
    StructField("store_name", StringType()),
    StructField("store_location", StringType()),
    StructField("store_city", StringType()),
    StructField("store_state", StringType()),
    StructField("store_country", StringType()),
    StructField("store_phone", StringType()),
    StructField("store_email", StringType()),
    StructField("pet_category", StringType()),
    StructField("product_weight", StringType()),
    StructField("product_color", StringType()),
    StructField("product_size", StringType()),
    StructField("product_brand", StringType()),
    StructField("product_material", StringType()),
    StructField("product_description", StringType()),
    StructField("product_rating", StringType()),
    StructField("product_reviews", StringType()),
    StructField("product_release_date", StringType()),
    StructField("product_expiry_date", StringType()),
    StructField("supplier_name", StringType()),
    StructField("supplier_contact", StringType()),
    StructField("supplier_email", StringType()),
    StructField("supplier_phone", StringType()),
    StructField("supplier_address", StringType()),
    StructField("supplier_city", StringType()),
    StructField("supplier_country", StringType()),
])


class PostgresStarEtl:
    def __init__(self, spark: SparkSession, config: PostgresConfig):
        self.spark = spark
        self.config = config

    def run(self) -> None:
        self.recreate_schema()
        raw = self.read_source_csv()
        prepared = self.prepare_source(raw)
        self.write_jdbc(prepared.select(*MOCK_DATA_COLUMNS), "mock_data")

        staged = self.prepare_source(self.read_jdbc("mock_data")).cache()
        self.load_dimensions(staged)
        self.load_fact(staged)
        self.validate()
        staged.unpersist()

    def recreate_schema(self) -> None:
        ddl = """
        DROP TABLE IF EXISTS fact_sale CASCADE;
        DROP TABLE IF EXISTS dim_date CASCADE;
        DROP TABLE IF EXISTS dim_pet CASCADE;
        DROP TABLE IF EXISTS dim_product CASCADE;
        DROP TABLE IF EXISTS dim_supplier CASCADE;
        DROP TABLE IF EXISTS dim_store CASCADE;
        DROP TABLE IF EXISTS dim_seller CASCADE;
        DROP TABLE IF EXISTS dim_customer CASCADE;
        DROP TABLE IF EXISTS mock_data CASCADE;

        CREATE TABLE mock_data (
            stage_id BIGSERIAL PRIMARY KEY,
            source_file TEXT,
            id INTEGER,
            customer_first_name TEXT,
            customer_last_name TEXT,
            customer_age INTEGER,
            customer_email TEXT,
            customer_country TEXT,
            customer_postal_code TEXT,
            customer_pet_type TEXT,
            customer_pet_name TEXT,
            customer_pet_breed TEXT,
            seller_first_name TEXT,
            seller_last_name TEXT,
            seller_email TEXT,
            seller_country TEXT,
            seller_postal_code TEXT,
            product_name TEXT,
            product_category TEXT,
            product_price NUMERIC(10, 2),
            product_quantity INTEGER,
            sale_date TEXT,
            sale_customer_id INTEGER,
            sale_seller_id INTEGER,
            sale_product_id INTEGER,
            sale_quantity INTEGER,
            sale_total_price NUMERIC(12, 2),
            store_name TEXT,
            store_location TEXT,
            store_city TEXT,
            store_state TEXT,
            store_country TEXT,
            store_phone TEXT,
            store_email TEXT,
            pet_category TEXT,
            product_weight NUMERIC(10, 2),
            product_color TEXT,
            product_size TEXT,
            product_brand TEXT,
            product_material TEXT,
            product_description TEXT,
            product_rating NUMERIC(3, 2),
            product_reviews INTEGER,
            product_release_date TEXT,
            product_expiry_date TEXT,
            supplier_name TEXT,
            supplier_contact TEXT,
            supplier_email TEXT,
            supplier_phone TEXT,
            supplier_address TEXT,
            supplier_city TEXT,
            supplier_country TEXT
        );

        CREATE TABLE dim_customer (
            customer_id BIGSERIAL PRIMARY KEY,
            first_name TEXT NOT NULL,
            last_name TEXT NOT NULL,
            age INTEGER NOT NULL,
            email TEXT NOT NULL UNIQUE,
            country TEXT NOT NULL,
            postal_code TEXT
        );

        CREATE TABLE dim_seller (
            seller_id BIGSERIAL PRIMARY KEY,
            first_name TEXT NOT NULL,
            last_name TEXT NOT NULL,
            email TEXT NOT NULL UNIQUE,
            country TEXT NOT NULL,
            postal_code TEXT
        );

        CREATE TABLE dim_store (
            store_id BIGSERIAL PRIMARY KEY,
            store_name TEXT NOT NULL,
            store_location TEXT NOT NULL,
            city TEXT NOT NULL,
            state TEXT,
            country TEXT NOT NULL,
            phone TEXT NOT NULL,
            email TEXT NOT NULL UNIQUE
        );

        CREATE TABLE dim_supplier (
            supplier_id BIGSERIAL PRIMARY KEY,
            supplier_name TEXT NOT NULL,
            contact_name TEXT NOT NULL,
            email TEXT NOT NULL UNIQUE,
            phone TEXT NOT NULL,
            address TEXT NOT NULL,
            city TEXT NOT NULL,
            country TEXT NOT NULL
        );

        CREATE TABLE dim_pet (
            pet_id BIGSERIAL PRIMARY KEY,
            pet_key TEXT NOT NULL UNIQUE,
            pet_type TEXT NOT NULL,
            pet_name TEXT NOT NULL,
            pet_breed TEXT NOT NULL,
            customer_email TEXT NOT NULL
        );

        CREATE TABLE dim_product (
            product_id BIGSERIAL PRIMARY KEY,
            product_key TEXT NOT NULL UNIQUE,
            product_name TEXT NOT NULL,
            product_category TEXT NOT NULL,
            pet_category TEXT NOT NULL,
            supplier_email TEXT NOT NULL,
            product_price NUMERIC(10, 2) NOT NULL,
            stock_quantity INTEGER NOT NULL,
            product_weight NUMERIC(10, 2) NOT NULL,
            product_color TEXT NOT NULL,
            product_size TEXT NOT NULL,
            product_brand TEXT NOT NULL,
            product_material TEXT NOT NULL,
            product_description TEXT NOT NULL,
            product_rating NUMERIC(3, 2) NOT NULL,
            product_reviews INTEGER NOT NULL,
            release_date DATE NOT NULL,
            expiry_date DATE NOT NULL
        );

        CREATE TABLE dim_date (
            date_id INTEGER PRIMARY KEY,
            full_date DATE NOT NULL UNIQUE,
            day_number SMALLINT NOT NULL,
            month_number SMALLINT NOT NULL,
            month_name TEXT NOT NULL,
            quarter_number SMALLINT NOT NULL,
            year_number SMALLINT NOT NULL
        );

        CREATE TABLE fact_sale (
            sale_id BIGSERIAL PRIMARY KEY,
            source_stage_id BIGINT NOT NULL UNIQUE REFERENCES mock_data(stage_id),
            source_sale_id INTEGER NOT NULL,
            sale_date_id INTEGER NOT NULL REFERENCES dim_date(date_id),
            customer_id BIGINT NOT NULL REFERENCES dim_customer(customer_id),
            seller_id BIGINT NOT NULL REFERENCES dim_seller(seller_id),
            store_id BIGINT NOT NULL REFERENCES dim_store(store_id),
            supplier_id BIGINT NOT NULL REFERENCES dim_supplier(supplier_id),
            product_id BIGINT NOT NULL REFERENCES dim_product(product_id),
            pet_id BIGINT NOT NULL REFERENCES dim_pet(pet_id),
            sale_quantity INTEGER NOT NULL,
            sale_total_price NUMERIC(12, 2) NOT NULL
        );

        CREATE INDEX idx_fact_sale_date ON fact_sale(sale_date_id);
        CREATE INDEX idx_fact_sale_customer ON fact_sale(customer_id);
        CREATE INDEX idx_fact_sale_product ON fact_sale(product_id);
        CREATE INDEX idx_dim_product_category ON dim_product(product_category);
        """
        for statement in [part.strip() for part in ddl.split(";") if part.strip()]:
            self.execute_postgres_statement(statement)

    def execute_postgres_statement(self, sql: str) -> None:
        jvm = self.spark.sparkContext._gateway.jvm
        jvm.java.lang.Class.forName("org.postgresql.Driver")
        connection = jvm.java.sql.DriverManager.getConnection(
            self.config.jdbc_url,
            self.config.user,
            self.config.password,
        )
        try:
            statement = connection.createStatement()
            try:
                statement.execute(sql)
            finally:
                statement.close()
        finally:
            connection.close()

    def read_source_csv(self) -> DataFrame:
        df = (
            self.spark.read
            .schema(RAW_SCHEMA)
            .option("header", "true")
            .option("multiLine", "true")
            .option("escape", "\"")
            .option("quote", "\"")
            .option("nullValue", "")
            .csv(DATA_PATH)
        )
        count = df.count()
        if count != 10000:
            raise RuntimeError(f"Expected 10000 rows in CSV files, got {count}")
        return df

    def prepare_source(self, df: DataFrame) -> DataFrame:
        prepared = (
            df
            .withColumn("source_file", regexp_extract(input_file_name(), r"([^/\\]+)$", 1))
            .withColumn("id", col("id").cast(IntegerType()))
            .withColumn("customer_age", col("customer_age").cast(IntegerType()))
            .withColumn("product_price", col("product_price").cast(DecimalType(10, 2)))
            .withColumn("product_quantity", col("product_quantity").cast(IntegerType()))
            .withColumn("sale_customer_id", col("sale_customer_id").cast(IntegerType()))
            .withColumn("sale_seller_id", col("sale_seller_id").cast(IntegerType()))
            .withColumn("sale_product_id", col("sale_product_id").cast(IntegerType()))
            .withColumn("sale_quantity", col("sale_quantity").cast(IntegerType()))
            .withColumn("sale_total_price", col("sale_total_price").cast(DecimalType(12, 2)))
            .withColumn("product_weight", col("product_weight").cast(DecimalType(10, 2)))
            .withColumn("product_rating", col("product_rating").cast(DecimalType(3, 2)))
            .withColumn("product_reviews", col("product_reviews").cast(IntegerType()))
            .withColumn("sale_dt", to_date(col("sale_date"), "M/d/yyyy"))
            .withColumn("product_release_dt", to_date(col("product_release_date"), "M/d/yyyy"))
            .withColumn("product_expiry_dt", to_date(col("product_expiry_date"), "M/d/yyyy"))
        )

        return (
            prepared
            .withColumn("product_key", self.hash_columns(PRODUCT_KEY_COLUMNS))
            .withColumn("pet_key", self.hash_columns(PET_KEY_COLUMNS))
        )

    def load_dimensions(self, staged: DataFrame) -> None:
        self.write_jdbc(
            staged.select(
                col("customer_first_name").alias("first_name"),
                col("customer_last_name").alias("last_name"),
                col("customer_age").alias("age"),
                col("customer_email").alias("email"),
                col("customer_country").alias("country"),
                col("customer_postal_code").alias("postal_code"),
            ).where(col("email").isNotNull()).dropDuplicates(["email"]),
            "dim_customer",
        )

        self.write_jdbc(
            staged.select(
                col("seller_first_name").alias("first_name"),
                col("seller_last_name").alias("last_name"),
                col("seller_email").alias("email"),
                col("seller_country").alias("country"),
                col("seller_postal_code").alias("postal_code"),
            ).where(col("email").isNotNull()).dropDuplicates(["email"]),
            "dim_seller",
        )

        self.write_jdbc(
            staged.select(
                "store_name",
                col("store_location"),
                col("store_city").alias("city"),
                col("store_state").alias("state"),
                col("store_country").alias("country"),
                col("store_phone").alias("phone"),
                col("store_email").alias("email"),
            ).where(col("email").isNotNull()).dropDuplicates(["email"]),
            "dim_store",
        )

        self.write_jdbc(
            staged.select(
                "supplier_name",
                col("supplier_contact").alias("contact_name"),
                col("supplier_email").alias("email"),
                col("supplier_phone").alias("phone"),
                col("supplier_address").alias("address"),
                col("supplier_city").alias("city"),
                col("supplier_country").alias("country"),
            ).where(col("email").isNotNull()).dropDuplicates(["email"]),
            "dim_supplier",
        )

        self.write_jdbc(
            staged.select(
                "pet_key",
                col("customer_pet_type").alias("pet_type"),
                col("customer_pet_name").alias("pet_name"),
                col("customer_pet_breed").alias("pet_breed"),
                col("customer_email"),
            ).where(col("pet_key").isNotNull()).dropDuplicates(["pet_key"]),
            "dim_pet",
        )

        self.write_jdbc(
            staged.select(
                "product_key",
                "product_name",
                "product_category",
                "pet_category",
                "supplier_email",
                "product_price",
                col("product_quantity").alias("stock_quantity"),
                "product_weight",
                "product_color",
                "product_size",
                "product_brand",
                "product_material",
                "product_description",
                "product_rating",
                "product_reviews",
                col("product_release_dt").alias("release_date"),
                col("product_expiry_dt").alias("expiry_date"),
            ).where(col("product_key").isNotNull()).dropDuplicates(["product_key"]),
            "dim_product",
        )

        date_frames = [
            staged.select(col("sale_dt").alias("full_date")),
            staged.select(col("product_release_dt").alias("full_date")),
            staged.select(col("product_expiry_dt").alias("full_date")),
        ]
        dates = reduce(lambda left, right: left.unionByName(right), date_frames)
        dates = (
            dates
            .where(col("full_date").isNotNull())
            .dropDuplicates(["full_date"])
            .select(
                date_format("full_date", "yyyyMMdd").cast(IntegerType()).alias("date_id"),
                "full_date",
                dayofmonth("full_date").alias("day_number"),
                month("full_date").alias("month_number"),
                date_format("full_date", "MMMM").alias("month_name"),
                quarter("full_date").alias("quarter_number"),
                year("full_date").alias("year_number"),
            )
        )
        self.write_jdbc(dates, "dim_date")

    def load_fact(self, staged: DataFrame) -> None:
        customer = self.read_jdbc("dim_customer")
        seller = self.read_jdbc("dim_seller")
        store = self.read_jdbc("dim_store")
        supplier = self.read_jdbc("dim_supplier")
        pet = self.read_jdbc("dim_pet")
        product = self.read_jdbc("dim_product")
        date = self.read_jdbc("dim_date")

        fact = (
            staged.alias("md")
            .join(date.alias("dt"), col("md.sale_dt") == col("dt.full_date"), "inner")
            .join(customer.alias("customer"), col("md.customer_email") == col("customer.email"), "inner")
            .join(seller.alias("seller"), col("md.seller_email") == col("seller.email"), "inner")
            .join(store.alias("store"), col("md.store_email") == col("store.email"), "inner")
            .join(supplier.alias("supplier"), col("md.supplier_email") == col("supplier.email"), "inner")
            .join(product.alias("product"), col("md.product_key") == col("product.product_key"), "inner")
            .join(pet.alias("pet"), col("md.pet_key") == col("pet.pet_key"), "inner")
            .select(
                col("md.stage_id").alias("source_stage_id"),
                col("md.id").alias("source_sale_id"),
                col("dt.date_id").alias("sale_date_id"),
                col("customer.customer_id"),
                col("seller.seller_id"),
                col("store.store_id"),
                col("supplier.supplier_id"),
                col("product.product_id"),
                col("pet.pet_id"),
                col("md.sale_quantity"),
                col("md.sale_total_price"),
            )
        )
        self.write_jdbc(fact, "fact_sale")

    def validate(self) -> None:
        checks = self.read_query("""
            SELECT
                (SELECT COUNT(*) FROM mock_data) AS stage_rows,
                (SELECT COUNT(*) FROM fact_sale) AS fact_rows,
                (
                    SELECT COUNT(*)
                    FROM mock_data md
                    LEFT JOIN fact_sale fs ON fs.source_stage_id = md.stage_id
                    WHERE fs.sale_id IS NULL
                ) AS missing_fact_rows
        """).collect()[0]
        if checks["stage_rows"] != 10000 or checks["fact_rows"] != 10000 or checks["missing_fact_rows"] != 0:
            raise RuntimeError(f"PostgreSQL validation failed: {checks.asDict()}")
        print(f"PostgreSQL star schema validation passed: {checks.asDict()}")

    def read_jdbc(self, table: str) -> DataFrame:
        return self.spark.read.jdbc(
            url=self.config.jdbc_url,
            table=table,
            properties=self.config.jdbc_properties,
        )

    def read_query(self, query: str) -> DataFrame:
        return self.spark.read.jdbc(
            url=self.config.jdbc_url,
            table=f"({query}) AS source_query",
            properties=self.config.jdbc_properties,
        )

    def write_jdbc(self, df: DataFrame, table: str) -> None:
        (
            df.write
            .mode("append")
            .jdbc(url=self.config.jdbc_url, table=table, properties=self.config.jdbc_properties)
        )

    @staticmethod
    def hash_columns(column_names: list[str]):
        normalized = [
            coalesce(trim(col(column_name).cast("string")), lit("<NULL>"))
            for column_name in column_names
        ]
        return sha2(concat_ws("||", *normalized), 256)


MOCK_DATA_COLUMNS = [
    "source_file",
    "id",
    "customer_first_name",
    "customer_last_name",
    "customer_age",
    "customer_email",
    "customer_country",
    "customer_postal_code",
    "customer_pet_type",
    "customer_pet_name",
    "customer_pet_breed",
    "seller_first_name",
    "seller_last_name",
    "seller_email",
    "seller_country",
    "seller_postal_code",
    "product_name",
    "product_category",
    "product_price",
    "product_quantity",
    "sale_date",
    "sale_customer_id",
    "sale_seller_id",
    "sale_product_id",
    "sale_quantity",
    "sale_total_price",
    "store_name",
    "store_location",
    "store_city",
    "store_state",
    "store_country",
    "store_phone",
    "store_email",
    "pet_category",
    "product_weight",
    "product_color",
    "product_size",
    "product_brand",
    "product_material",
    "product_description",
    "product_rating",
    "product_reviews",
    "product_release_date",
    "product_expiry_date",
    "supplier_name",
    "supplier_contact",
    "supplier_email",
    "supplier_phone",
    "supplier_address",
    "supplier_city",
    "supplier_country",
]

PRODUCT_KEY_COLUMNS = [
    "product_name",
    "product_category",
    "pet_category",
    "supplier_email",
    "product_price",
    "product_quantity",
    "product_weight",
    "product_color",
    "product_size",
    "product_brand",
    "product_material",
    "product_description",
    "product_rating",
    "product_reviews",
    "product_release_dt",
    "product_expiry_dt",
]

PET_KEY_COLUMNS = [
    "customer_pet_type",
    "customer_pet_name",
    "customer_pet_breed",
    "customer_email",
]
