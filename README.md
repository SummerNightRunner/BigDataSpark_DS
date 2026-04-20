# BigDataSpark

Анализ больших данных - лабораторная работа №2 - ETL реализованный с помощью Spark

Одним из самых популярных фреймворков для работы с Big Data является Apache Spark. Apache Spark - мощный фреймворк, который предлагает широкий набор функциональности для простого написания ETL-пайплайнов.

## Реализация и запуск

В репозитории реализован обязательный вариант лабораторной:

- загрузка 10 CSV-файлов в PostgreSQL в таблицу `mock_data`;
- Spark ETL из `mock_data` в модель звезда/снежинка в PostgreSQL;
- Spark ETL из PostgreSQL-звезды в 6 таблиц отчетов в ClickHouse.

### 1. Запустить инфраструктуру

```bash
docker compose up -d postgres clickhouse spark
```

PostgreSQL и ClickHouse запускаются автоматически. При первом старте PostgreSQL выполняет `init/01_init.sql` и загружает все CSV из папки `data`.

JupyterLab доступен по адресу:

```text
http://localhost:8888/lab
```

### 2. Создать модель звезда/снежинка в PostgreSQL

```bash
docker exec spark_lab spark-submit \
  --packages org.postgresql:postgresql:42.6.0 \
  /home/jovyan/work/etl_to_postgres.py
```

Скрипт создает таблицы:

- `dim_customer`
- `dim_seller`
- `dim_store`
- `dim_supplier`
- `dim_product`
- `dim_date`
- `fact_sales`

### 3. Создать отчеты в ClickHouse

```bash
docker exec spark_lab spark-submit \
  --packages org.postgresql:postgresql:42.6.0,com.clickhouse:clickhouse-jdbc:0.6.0 \
  /home/jovyan/work/etl_to_clickhouse.py
```

Скрипт создает в базе ClickHouse `reports` 6 таблиц:

- `product_sales_report` - продажи по продуктам;
- `customer_sales_report` - продажи по клиентам;
- `time_sales_report` - продажи по времени;
- `store_sales_report` - продажи по магазинам;
- `supplier_sales_report` - продажи по поставщикам;
- `product_quality_report` - качество продукции, рейтинги, отзывы и корреляция рейтинга с продажами.

### 4. Проверка PostgreSQL

```bash
docker exec postgres_lab psql -U postgres -d bigdata_lab -c "
select 'mock_data' table_name, count(*) from mock_data
union all select 'dim_customer', count(*) from dim_customer
union all select 'dim_seller', count(*) from dim_seller
union all select 'dim_store', count(*) from dim_store
union all select 'dim_supplier', count(*) from dim_supplier
union all select 'dim_product', count(*) from dim_product
union all select 'dim_date', count(*) from dim_date
union all select 'fact_sales', count(*) from fact_sales
order by table_name;
"
```

Ожидаемый результат:

```text
dim_customer | 10000
dim_date     |   364
dim_product  | 10000
dim_seller   | 10000
dim_store    | 10000
dim_supplier | 10000
fact_sales   | 10000
mock_data    | 10000
```

### 5. Проверка ClickHouse

```bash
docker exec clickhouse_lab clickhouse-client --database reports --query "
SELECT *
FROM (
  SELECT 'product_sales_report' AS table_name, count() AS rows FROM product_sales_report
  UNION ALL SELECT 'customer_sales_report', count() FROM customer_sales_report
  UNION ALL SELECT 'time_sales_report', count() FROM time_sales_report
  UNION ALL SELECT 'store_sales_report', count() FROM store_sales_report
  UNION ALL SELECT 'supplier_sales_report', count() FROM supplier_sales_report
  UNION ALL SELECT 'product_quality_report', count() FROM product_quality_report
)
ORDER BY table_name
"
```

Ожидаемый результат:

```text
customer_sales_report   10000
product_quality_report  10000
product_sales_report    10000
store_sales_report      10000
supplier_sales_report   10000
time_sales_report          12
```

Примеры запросов для проверки витрин:

```bash
docker exec clickhouse_lab clickhouse-client --database reports --query "
SELECT product_name, total_quantity_sold, total_revenue, sales_rank
FROM product_sales_report
ORDER BY sales_rank
LIMIT 10
"

docker exec clickhouse_lab clickhouse-client --database reports --query "
SELECT year, month, monthly_revenue, previous_month_revenue, revenue_delta
FROM time_sales_report
ORDER BY year, month
"
```

### 6. Бонусные NoSQL БД

Optional-сервисы запускаются отдельным профилем:

```bash
docker compose --profile optional up -d cassandra mongodb neo4j valkey
```

Запуск Spark-джоб:

```bash
docker exec spark_lab spark-submit \
  --packages org.postgresql:postgresql:42.6.0,org.mongodb.spark:mongo-spark-connector_2.12:10.2.1 \
  /home/jovyan/work/etl_to_mongodb.py

docker exec spark_lab spark-submit \
  --packages org.postgresql:postgresql:42.6.0 \
  /home/jovyan/work/etl_to_valkey.py

docker exec spark_lab spark-submit \
  --packages org.postgresql:postgresql:42.6.0 \
  /home/jovyan/work/etl_to_cassandra.py

docker exec spark_lab spark-submit \
  --packages org.postgresql:postgresql:42.6.0 \
  /home/jovyan/work/etl_to_neo4j.py
```

В MongoDB создаются 6 коллекций с полными документами отчетов. В Cassandra создается keyspace `reports` и 6 таблиц формата `id text primary key, payload text`. В Neo4j создаются узлы `ReportRecord` с отдельными labels для каждого отчета. В Valkey создаются ключи `report:<report_name>:<id>` и счетчики `report:<report_name>:count`.

Проверка MongoDB:

```bash
docker exec mongodb_lab mongosh -u root -p root --authenticationDatabase admin --quiet --eval "
const db = db.getSiblingDB('reports');
['product_sales_report','customer_sales_report','time_sales_report','store_sales_report','supplier_sales_report','product_quality_report']
  .forEach(c => print(c + '\t' + db.getCollection(c).countDocuments()))
"
```

Проверка Cassandra:

```bash
docker exec cassandra_lab cqlsh -e "
select count(*) from reports.product_sales_report;
select count(*) from reports.customer_sales_report;
select count(*) from reports.time_sales_report;
select count(*) from reports.store_sales_report;
select count(*) from reports.supplier_sales_report;
select count(*) from reports.product_quality_report;
"
```

Проверка Neo4j:

```bash
docker exec neo4j_lab cypher-shell -u neo4j -p password "
MATCH (n:ReportRecord)
RETURN n.report AS report, count(n) AS rows
ORDER BY report
"
```

Проверка Valkey:

```bash
docker exec valkey_lab valkey-cli MGET \
  report:product_sales_report:count \
  report:customer_sales_report:count \
  report:time_sales_report:count \
  report:store_sales_report:count \
  report:supplier_sales_report:count \
  report:product_quality_report:count
```

Примеры проверки одной записи в бонусных БД:

```bash
docker exec mongodb_lab mongosh -u root -p root --authenticationDatabase admin --quiet --eval "
db.getSiblingDB('reports').product_sales_report.findOne()
"

docker exec cassandra_lab cqlsh -e "
select * from reports.product_sales_report limit 1;
"

docker exec neo4j_lab cypher-shell -u neo4j -p password "
MATCH (n:ProductSalesReport)
RETURN n.record_id AS id, n.payload AS payload
LIMIT 1
"

docker exec valkey_lab valkey-cli GET report:product_sales_report:1
```

Ожидаемые количества для всех БД:

```text
customer_sales_report   10000
product_quality_report  10000
product_sales_report    10000
store_sales_report      10000
supplier_sales_report   10000
time_sales_report          12
```

### 7. Служебные файлы

Файл `.gitignore` нужен, чтобы не добавлять в репозиторий локальные и сгенерированные файлы: Python-кэш `__pycache__`, Jupyter checkpoints, `.DS_Store`, `.env`, Spark warehouse/metastore и логи.

Что необходимо сделать? 

Необходимо реализовать ETL-пайплайн с помощью Spark, который трансформирует данные из источника (файлы mock_data.csv с номерами) в модель данных звезда в PostgreSQL, а затем на основе модели данных звезда создать ряд отчетов по данным в одной из NoSQL базах данных обязательно и в нескольких других опционально (будет бонусом). Каждый отчет представляет собой отдельную таблицу в NoSQL БД.

Какие отчеты надо создать?
1. Витрина продаж по продуктам
Цель: Анализ выручки, количества продаж и популярности продуктов.
 - Топ-10 самых продаваемых продуктов.
 - Общая выручка по категориям продуктов.
 - Средний рейтинг и количество отзывов для каждого продукта.
2. Витрина продаж по клиентам
Цель: Анализ покупательского поведения и сегментация клиентов.
 - Топ-10 клиентов с наибольшей общей суммой покупок.
 - Распределение клиентов по странам.
 - Средний чек для каждого клиента.
3. Витрина продаж по времени
Цель: Анализ сезонности и трендов продаж.
 - Месячные и годовые тренды продаж.
 - Сравнение выручки за разные периоды.
 - Средний размер заказа по месяцам.
4. Витрина продаж по магазинам
Цель: Анализ эффективности магазинов.
 - Топ-5 магазинов с наибольшей выручкой.
 - Распределение продаж по городам и странам.
 - Средний чек для каждого магазина.
5. Витрина продаж по поставщикам
Цель: Анализ эффективности поставщиков.
 - Топ-5 поставщиков с наибольшей выручкой.
 - Средняя цена товаров от каждого поставщика.
 - Распределение продаж по странам поставщиков.
6. Витрина качества продукции
Цель: Анализ отзывов и рейтингов товаров.
 - Продукты с наивысшим и наименьшим рейтингом.
 - Корреляция между рейтингом и объемом продаж.
 - Продукты с наибольшим количеством отзывов.

В каких NoSQL БД должны быть эти отчеты:
1. **Clickhouse** **(обязательно)**
2. Cassandra (опционально, если будет реализация, то это бонус)
3. Neo4J (опционально, если будет реализация, то это бонус)
4. MongoDB (опционально, если будет реализация, то это бонус)
5. Valkey (опционально, если будет реализация, то это бонус)

![Лабораторная работа №2](https://github.com/user-attachments/assets/2b854382-4c36-4542-a7fb-04fe82a6f6fa)


Алгоритм:

1. Клонируете к себе этот репозиторий.
2. Устанавливаете себе инструмент для работы с запросами SQL (рекомендую DBeaver).
3. Устанавливаете базу данных PostgreSQL (рекомендую установку через docker).
4. Устанавливаете Apache Spark (рекомендую установку через Docker. Для удобства написания кода на Python можно запустить вместе со JupyterNotebook. Для Java - подключить volume и собрать образ Docker, который будет запускать команду spark-submit с java jar-файлом при старте контейнера, сам jar файл собирается отдельно и кладется в подключенный volume)
5. Скачиваете файлы с исходными данными mock_data( * ).csv, где ( * ) номера файлов. Всего 10 файлов, каждый по 1000 строк.
6. Импортируете данные в БД PostgreSQL (например, через механизм импорта csv в DBeaver). Всего в таблице mock_data должно находиться 10000 строк из 10 файлов.
7. Анализируете исходные данные с помощью запросов.
8. Выявляете сущности фактов и измерений.
9. Реализуете приложение на Spark, которое по аналогии с первой лабораторной работой перекладывает исходные данные из PostgreSQL в модель снежинку/звезда в PostgreSQL. (Убедитесь в коннективности Spark и PostgreSQL, настройте сеть между Spark и PostgreSQL, если используете Docker).
10. Устанавливаете ClickHouse (рекомендую установку через Docker. Убедитесь в коннективности Spark и Clickhouse, настройте сеть между Spark и ClickHouse). **(обязательно)**
11. Реализуете приложение на Spark, которое создаёт все 6 перечисленных выше отчетов в виде 6 отдельных таблиц в ClickHouse. **(обязательно)**
12. Устанавливаете Cassandra (рекомендую установку через Docker. Убедитесь в коннективности Spark и Cassandra, настройте сеть между Spark и Cassandra). (опционально)
13. Реализуете приложение на Spark, которое создаёт все 6 перечисленных выше отчетов в виде 6 отдельных таблиц в Cassandra. (опционально)
14. Устанавливаете Neo4j (рекомендую установку через Docker. Убедитесь в коннективности Spark и Neo4j, настройте сеть между Spark и Neo4j). (опционально)
15. Реализуете приложение на Spark, которое создаёт все 6 перечисленных выше отчетов в виде отдельных сущностей в Neo4j. (опционально)
16. Устанавливаете MongoDB (рекомендую установку через Docker. Убедитесь в коннективности Spark и MongoDB, настройте сеть между Spark и MongoDB). (опционально)
17. Реализуете приложение на Spark, которое создаёт все 6 перечисленных выше отчетов в виде 6 отдельных коллекций в MongoDB. (опционально)
18. Устанавливаете Valkey (рекомендую установку через Docker. Убедитесь в коннективности Spark и Valkey, настройте сеть между Spark и Valkey). (опционально)
19. Реализуете приложение на Spark, которое создаёт все 6 перечисленных выше отчетов в виде отдельных записей в Valkey. (опционально)
20. Проверяете отчеты в каждой базе данных средствами языка самой БД (ClickHouse - SQL (DBeaver), Cassandra - CQL (DBeaver), Neo4J - Cipher (DBeaver), MongoDB - MQL (Compass), Valkey - redis-cli).
21. Отправляете работу на проверку лаборантам.

Что должно быть результатом работы?

1. Репозиторий, в котором есть исходные данные mock_data().csv, где () номера файлов. Всего 10 файлов, каждый по 1000 строк.
2. Файл docker-compose.yml с установкой PostgreSQL, Spark, ClickHouse **(обязательно)**, Cassandra (опционально), Neo4j (опционально), MongoDB (опционально), Valkey (опционально) и заполненными данными в PostgreSQL из файлов mock_data(*).csv.
3. Инструкция, как запускать Spark-джобы для проверки лабораторной работы.
4. Код Apache Spark трансформации данных из исходной модели в снежинку/звезду в PostgreSQL.
5. Код Apache Spark трансформации данных из снежинки/звезды в отчеты в ClickHouse.
6. Код Apache Spark трансформации данных из снежинки/звезды в отчеты в Cassandra.
7. Код Apache Spark трансформации данных из снежинки/звезды в отчеты в Neo4j.
8. Код Apache Spark трансформации данных из снежинки/звезды в отчеты в MongoDB.
9. Код Apache Spark трансформации данных из снежинки/звезды в отчеты в Valkey.
