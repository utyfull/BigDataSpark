# BigDataSpark

Лабораторную работу № 2 выполнил Пинчук Михаил Сергеевич, группа М8О-308Б-23.

Лабораторная работа по построению ETL-пайплайна на Apache Spark. Spark читает исходные CSV-файлы, загружает staging-таблицу в PostgreSQL, строит модель данных "звезда", а затем формирует аналитические отчеты и записывает их в ClickHouse.

## Структура проекта

```text
BigDataSpark/
|-- docker-compose.yaml
|-- README.md
|-- исходные данные/
|   |-- MOCK_DATA.csv
|   |-- MOCK_DATA (1).csv
|   |-- ...
|   |-- MOCK_DATA (9).csv
|-- spark/
|   |-- main.py
|   |-- config.py
|   |-- spark_session.py
|   |-- postgres_star_etl.py
|   |-- reports.py
|   |-- sinks/
|       |-- clickhouse.py
|-- docs/
|   |-- architecture.plantuml
|   |-- shema.pdf
|   |-- report_tables.md
```

## Что делает пайплайн

1. Поднимает PostgreSQL, Spark и ClickHouse.
2. Spark читает 10 CSV-файлов из папки `исходные данные`.
3. Spark загружает 10000 исходных строк в PostgreSQL-таблицу `mock_data`.
4. Spark строит в PostgreSQL модель "звезда":
   - `fact_sale`;
   - `dim_customer`;
   - `dim_seller`;
   - `dim_store`;
   - `dim_supplier`;
   - `dim_product`;
   - `dim_pet`;
   - `dim_date`.
5. Spark строит набор отчетов по продажам, клиентам, времени, магазинам, поставщикам и качеству товаров.
6. Набор отчетов записывается в ClickHouse.

## Запуск

Запуск всех сервисов без выполнения ETL:

```bash
docker compose up -d
```

Запуск сервисов вместе с ETL и созданием отчетов:

```bash
docker compose --profile run-etl up
```

Повторный запуск ETL без пересоздания сервисов:

```bash
docker compose --profile run-etl run --rm etl-runner
```

Полная очистка volumes и повторный запуск:

```bash
docker compose down -v
docker compose --profile run-etl up
```

## Подключения

PostgreSQL:

```text
Host: localhost
Port: 5433
Database: spark_lab
User: postgres
Password: postgres
```

ClickHouse:

```text
Host: localhost
HTTP Port: 8123
Native Port: 9000
Database: default
User: clickhouse
Password: clickhouse
```

Spark UI:

```text
http://localhost:8082
```

## Отчеты

Подробный список таблиц находится в `docs/report_tables.md`. Диаграмма архитектуры описана в `docs/architecture.plantuml`, готовая PDF-версия лежит в `docs/shema.pdf`.

Таблицы отчетов:

```text
product_sales_report
customer_sales_report
time_sales_report
store_sales_report
supplier_sales_report
product_quality_report
```
