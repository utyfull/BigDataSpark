from collections import OrderedDict

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    avg,
    coalesce,
    col,
    concat_ws,
    corr,
    count,
    countDistinct,
    date_format,
    dense_rank,
    lag,
    lit,
    max,
    min,
    round,
    sum,
    when,
)
from pyspark.sql.window import Window

from config import PostgresConfig


class ReportBuilder:
    def __init__(self, spark: SparkSession, postgres: PostgresConfig):
        self.spark = spark
        self.postgres = postgres

    def build(self) -> OrderedDict[str, DataFrame]:
        sales = self.read_sales_enriched().cache()

        reports = OrderedDict()
        reports["product_sales_report"] = self.product_sales_report(sales)
        reports["customer_sales_report"] = self.customer_sales_report(sales)
        reports["time_sales_report"] = self.time_sales_report(sales)
        reports["store_sales_report"] = self.store_sales_report(sales)
        reports["supplier_sales_report"] = self.supplier_sales_report(sales)
        reports["product_quality_report"] = self.product_quality_report(sales)

        sales.unpersist()
        return reports

    def read_sales_enriched(self) -> DataFrame:
        fact = self.read_pg("fact_sale")
        date = self.read_pg("dim_date")
        customer = self.read_pg("dim_customer")
        seller = self.read_pg("dim_seller")
        store = self.read_pg("dim_store")
        supplier = self.read_pg("dim_supplier")
        product = self.read_pg("dim_product")
        pet = self.read_pg("dim_pet")

        return (
            fact.alias("sale")
            .join(date.alias("date"), col("sale.sale_date_id") == col("date.date_id"), "inner")
            .join(customer.alias("customer"), col("sale.customer_id") == col("customer.customer_id"), "inner")
            .join(seller.alias("seller"), col("sale.seller_id") == col("seller.seller_id"), "inner")
            .join(store.alias("store"), col("sale.store_id") == col("store.store_id"), "inner")
            .join(supplier.alias("supplier"), col("sale.supplier_id") == col("supplier.supplier_id"), "inner")
            .join(product.alias("product"), col("sale.product_id") == col("product.product_id"), "inner")
            .join(pet.alias("pet"), col("sale.pet_id") == col("pet.pet_id"), "inner")
            .select(
                col("sale.sale_id"),
                col("date.full_date").alias("sale_date"),
                col("date.year_number").alias("sale_year"),
                col("date.month_number").alias("sale_month"),
                col("date.month_name").alias("sale_month_name"),
                col("sale.sale_quantity"),
                col("sale.sale_total_price"),
                col("customer.customer_id"),
                col("customer.first_name").alias("customer_first_name"),
                col("customer.last_name").alias("customer_last_name"),
                col("customer.email").alias("customer_email"),
                col("customer.country").alias("customer_country"),
                col("seller.seller_id"),
                col("seller.email").alias("seller_email"),
                col("store.store_id"),
                col("store.store_name"),
                col("store.city").alias("store_city"),
                col("store.country").alias("store_country"),
                col("supplier.supplier_id"),
                col("supplier.supplier_name"),
                col("supplier.country").alias("supplier_country"),
                col("product.product_id"),
                col("product.product_name"),
                col("product.product_category"),
                col("product.pet_category"),
                col("product.product_price"),
                col("product.product_rating"),
                col("product.product_reviews"),
                col("pet.pet_type"),
                col("pet.pet_breed"),
            )
        )

    def product_sales_report(self, sales: DataFrame) -> DataFrame:
        category_window = Window.partitionBy("product_category")
        rank_window = Window.orderBy(col("total_quantity_sold").desc(), col("total_revenue").desc())

        return (
            sales.groupBy("product_id", "product_name", "product_category")
            .agg(
                sum("sale_quantity").alias("total_quantity_sold"),
                round(sum("sale_total_price"), 2).alias("total_revenue"),
                count("*").alias("orders_count"),
                round(avg("sale_total_price"), 2).alias("avg_order_value"),
                round(avg("product_rating"), 2).alias("avg_rating"),
                max("product_reviews").alias("reviews_count"),
            )
            .withColumn("category_total_revenue", round(sum("total_revenue").over(category_window), 2))
            .withColumn("sales_rank", dense_rank().over(rank_window))
            .withColumn("is_top_10_product", col("sales_rank") <= 10)
            .orderBy(col("sales_rank").asc(), col("product_name").asc())
        )

    def customer_sales_report(self, sales: DataFrame) -> DataFrame:
        country_window = Window.partitionBy("customer_country")
        rank_window = Window.orderBy(col("total_spent").desc())

        return (
            sales.withColumn("customer_name", concat_ws(" ", "customer_first_name", "customer_last_name"))
            .groupBy("customer_id", "customer_name", "customer_email", "customer_country")
            .agg(
                round(sum("sale_total_price"), 2).alias("total_spent"),
                count("*").alias("orders_count"),
                round(avg("sale_total_price"), 2).alias("avg_check"),
                sum("sale_quantity").alias("items_bought"),
            )
            .withColumn("country_customers_count", count("*").over(country_window))
            .withColumn("country_total_revenue", round(sum("total_spent").over(country_window), 2))
            .withColumn("customer_rank", dense_rank().over(rank_window))
            .withColumn("is_top_10_customer", col("customer_rank") <= 10)
            .orderBy(col("customer_rank").asc(), col("customer_email").asc())
        )

    def time_sales_report(self, sales: DataFrame) -> DataFrame:
        window = Window.orderBy("year_month")
        monthly = (
            sales.withColumn("year_month", date_format("sale_date", "yyyy-MM"))
            .groupBy("sale_year", "sale_month", "sale_month_name", "year_month")
            .agg(
                round(sum("sale_total_price"), 2).alias("total_revenue"),
                sum("sale_quantity").alias("total_quantity"),
                count("*").alias("orders_count"),
                round(avg("sale_total_price"), 2).alias("avg_order_value"),
                countDistinct("customer_id").alias("unique_customers"),
            )
        )

        return (
            monthly
            .withColumn("previous_month_revenue", lag("total_revenue").over(window))
            .withColumn(
                "revenue_delta",
                round(col("total_revenue") - coalesce(col("previous_month_revenue"), lit(0)), 2),
            )
            .withColumn(
                "revenue_growth_percent",
                round(
                    when(col("previous_month_revenue").isNull() | (col("previous_month_revenue") == 0), lit(0))
                    .otherwise((col("total_revenue") - col("previous_month_revenue")) / col("previous_month_revenue") * 100),
                    2,
                ),
            )
            .orderBy("year_month")
        )

    def store_sales_report(self, sales: DataFrame) -> DataFrame:
        city_window = Window.partitionBy("store_city", "store_country")
        country_window = Window.partitionBy("store_country")
        rank_window = Window.orderBy(col("total_revenue").desc())

        return (
            sales.groupBy("store_id", "store_name", "store_city", "store_country")
            .agg(
                round(sum("sale_total_price"), 2).alias("total_revenue"),
                sum("sale_quantity").alias("items_sold"),
                count("*").alias("orders_count"),
                round(avg("sale_total_price"), 2).alias("avg_check"),
            )
            .withColumn("city_total_revenue", round(sum("total_revenue").over(city_window), 2))
            .withColumn("country_total_revenue", round(sum("total_revenue").over(country_window), 2))
            .withColumn("store_rank", dense_rank().over(rank_window))
            .withColumn("is_top_5_store", col("store_rank") <= 5)
            .orderBy(col("store_rank").asc(), col("store_name").asc())
        )

    def supplier_sales_report(self, sales: DataFrame) -> DataFrame:
        country_window = Window.partitionBy("supplier_country")
        rank_window = Window.orderBy(col("total_revenue").desc())

        return (
            sales.groupBy("supplier_id", "supplier_name", "supplier_country")
            .agg(
                round(sum("sale_total_price"), 2).alias("total_revenue"),
                sum("sale_quantity").alias("items_sold"),
                count("*").alias("orders_count"),
                countDistinct("product_id").alias("products_count"),
                round(avg("product_price"), 2).alias("avg_product_price"),
                min("product_price").alias("min_product_price"),
                max("product_price").alias("max_product_price"),
            )
            .withColumn("country_total_revenue", round(sum("total_revenue").over(country_window), 2))
            .withColumn("supplier_rank", dense_rank().over(rank_window))
            .withColumn("is_top_5_supplier", col("supplier_rank") <= 5)
            .orderBy(col("supplier_rank").asc(), col("supplier_name").asc())
        )

    def product_quality_report(self, sales: DataFrame) -> DataFrame:
        rank_best = Window.orderBy(col("product_rating").desc(), col("product_reviews").desc())
        rank_worst = Window.orderBy(col("product_rating").asc(), col("product_reviews").desc())
        correlations = sales.agg(
            round(corr("product_rating", "sale_quantity"), 6).alias("rating_quantity_correlation"),
            round(corr("product_rating", "sale_total_price"), 6).alias("rating_revenue_correlation"),
        )

        quality = (
            sales.groupBy("product_id", "product_name", "product_category")
            .agg(
                max("product_rating").alias("product_rating"),
                max("product_reviews").alias("product_reviews"),
                sum("sale_quantity").alias("quantity_sold"),
                round(sum("sale_total_price"), 2).alias("total_revenue"),
                count("*").alias("orders_count"),
            )
            .withColumn("best_rating_rank", dense_rank().over(rank_best))
            .withColumn("worst_rating_rank", dense_rank().over(rank_worst))
            .withColumn("is_highest_rated_top_10", col("best_rating_rank") <= 10)
            .withColumn("is_lowest_rated_top_10", col("worst_rating_rank") <= 10)
            .orderBy(col("product_reviews").desc(), col("product_rating").desc())
        )

        return quality.crossJoin(correlations)

    def read_pg(self, table: str) -> DataFrame:
        return self.spark.read.jdbc(
            url=self.postgres.jdbc_url,
            table=table,
            properties=self.postgres.jdbc_properties,
        )
