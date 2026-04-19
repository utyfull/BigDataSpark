# Report Tables

The Spark job creates 6 ClickHouse report tables.

## `product_sales_report`

Grain: one row per product.

Columns:

- `product_id`
- `product_name`
- `product_category`
- `total_quantity_sold`
- `total_revenue`
- `orders_count`
- `avg_order_value`
- `avg_rating`
- `reviews_count`
- `category_total_revenue`
- `sales_rank`
- `is_top_10_product`

## `customer_sales_report`

Grain: one row per customer.

Columns:

- `customer_id`
- `customer_name`
- `customer_email`
- `customer_country`
- `total_spent`
- `orders_count`
- `avg_check`
- `items_bought`
- `country_customers_count`
- `country_total_revenue`
- `customer_rank`
- `is_top_10_customer`

## `time_sales_report`

Grain: one row per month.

Columns:

- `sale_year`
- `sale_month`
- `sale_month_name`
- `year_month`
- `total_revenue`
- `total_quantity`
- `orders_count`
- `avg_order_value`
- `unique_customers`
- `previous_month_revenue`
- `revenue_delta`
- `revenue_growth_percent`

## `store_sales_report`

Grain: one row per store.

Columns:

- `store_id`
- `store_name`
- `store_city`
- `store_country`
- `total_revenue`
- `items_sold`
- `orders_count`
- `avg_check`
- `city_total_revenue`
- `country_total_revenue`
- `store_rank`
- `is_top_5_store`

## `supplier_sales_report`

Grain: one row per supplier.

Columns:

- `supplier_id`
- `supplier_name`
- `supplier_country`
- `total_revenue`
- `items_sold`
- `orders_count`
- `products_count`
- `avg_product_price`
- `min_product_price`
- `max_product_price`
- `country_total_revenue`
- `supplier_rank`
- `is_top_5_supplier`

## `product_quality_report`

Grain: one row per product.

Columns:

- `product_id`
- `product_name`
- `product_category`
- `product_rating`
- `product_reviews`
- `quantity_sold`
- `total_revenue`
- `orders_count`
- `best_rating_rank`
- `worst_rating_rank`
- `is_highest_rated_top_10`
- `is_lowest_rated_top_10`
- `rating_quantity_correlation`
- `rating_revenue_correlation`
