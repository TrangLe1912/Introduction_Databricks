# Databricks notebook source
# MAGIC %md
# MAGIC # Phân tích dữ liệu sử dụng Pyspark trên Databricks

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC ## Tổng quan
# MAGIC PySpark là API Python của Apache Spark, một framework để xử lý dữ liệu quy mô lớn một cách hiệu quả trong môi trường phân tán. Apache Spark là một framework mã nguồn mở cho cluster computing, có thể thực hiện nhanh chóng việc xử lý dữ liệu, xử lý streaming, machine learning, xử lý đồ thị, v.v.
# MAGIC
# MAGIC ## Tại sao sử dụng PySpark?
# MAGIC - **Xử lý dữ liệu quy mô lớn**: PySpark xử lý phân tán khối lượng dữ liệu lớn, tốc độ xử lý nhanh và có thể mở rộng trên cluster.
# MAGIC - **API linh hoạt**: PySpark cung cấp API Python thân thiện, có thể đáp ứng từ các thao tác đơn giản như Pandas đến xử lý phân tán phức tạp.
# MAGIC - **Ứng dụng rộng rãi**: Đáp ứng nhiều mục đích khác nhau như ETL (Extract, Transform, Load), phân tích dữ liệu, machine learning, real-time streaming, v.v.
# MAGIC
# MAGIC ## Các thành phần chính của PySpark
# MAGIC 1. **RDD (Resilient Distributed Dataset)**: Cấu trúc dữ liệu cơ bản, là tập hợp dữ liệu bất biến được phân tán. Thao tác bằng transformation và action.
# MAGIC 2. **DataFrame**: API cấp cao để xử lý dữ liệu có cấu trúc, có thể thực hiện các thao tác giống SQL. Có cách sử dụng tương tự DataFrame của Pandas và có thể thực thi SQL query.
# MAGIC 3. **Spark SQL**: Module có thể thao tác DataFrame bằng SQL query. Có thể xử lý dữ liệu giống như database.
# MAGIC 4. **MLlib**: Thư viện cung cấp các thuật toán machine learning, có thể thực hiện distributed learning trong cluster.
# MAGIC 5. **Spark Streaming**: Module để xử lý dữ liệu real-time. Xử lý dữ liệu real-time bằng micro-batch.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup và Import Libraries

# COMMAND ----------

from datetime import datetime, timedelta

import pyspark.sql.functions as F
import pyspark.sql.types as T
import pyspark.sql.window as W
from pyspark.sql import SparkSession

# Tạo hoặc lấy Spark session (thường đã có sẵn trên Databricks)
# spark = SparkSession.builder.appName("PySpark_Tutorial").getOrCreate()

print("PySpark setup hoàn tất!")
print(f"Spark version: {spark.version}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Tạo và hiển thị DataFrame
# MAGIC
# MAGIC DataFrame là cấu trúc dữ liệu chính trong PySpark, tương tự như bảng trong SQL.

# COMMAND ----------

# === TẠO DATAFRAME TỪ DỮ LIỆU MẪU ===

# Dữ liệu bán hàng mẫu
sales_data = [
    ("TXN001", "C001", "P001", 2, 1000.0, "2024-01-15", "S001"),
    ("TXN002", "C002", "P002", 1, 5000.0, "2024-01-15", "S002"),
    ("TXN003", "C003", "P003", 3, 3000.0, "2024-01-16", "S001"),
    ("TXN004", "C001", "P004", 1, 2000.0, "2024-01-17", "S003"),
    ("TXN005", "C004", "P001", 5, 1000.0, "2024-01-17", "S002"),
    ("TXN006", "C002", "P005", 2, 1200.0, "2024-01-18", "S001"),
    ("TXN007", "C005", "P002", 1, 5000.0, "2024-01-18", "S003"),
    ("TXN008", "C003", "P003", 4, 3000.0, "2024-01-19", "S002"),
]

# Định nghĩa schema (cấu trúc dữ liệu)
sales_schema = T.StructType(
    [
        T.StructField("transaction_id", T.StringType(), True),
        T.StructField("customer_id", T.StringType(), True),
        T.StructField("product_id", T.StringType(), True),
        T.StructField("quantity", T.IntegerType(), True),
        T.StructField("unit_price", T.DoubleType(), True),
        T.StructField("sale_date", T.StringType(), True),
        T.StructField("store_id", T.StringType(), True),
    ]
)

# Tạo DataFrame
sdf_sales = spark.createDataFrame(sales_data, sales_schema)

print("THÔNG TIN DATAFRAME:")
print(f"Số dòng: {sdf_sales.count()}")
print(f"Số cột: {len(sdf_sales.columns)}")
print(f"Tên các cột: {sdf_sales.columns}")

# COMMAND ----------

# === HIỂN THỊ DATAFRAME ===

# Cách 1: show() - hiển thị dạng text
print("Hiển thị với .show():")
sdf_sales.show(5)  # Hiển thị 5 dòng đầu

# Cách 2: display() - hiển thị đẹp mắt (chỉ có trên Databricks)
print("Hiển thị với .display():")
sdf_sales.display()

# Xem schema (cấu trúc dữ liệu)
print("Schema của DataFrame:")
sdf_sales.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Thao tác với cột
# MAGIC
# MAGIC Các thao tác phổ biến: select, add, drop, rename columns

# COMMAND ----------

# === CHỌN CỘT ===

# Chọn một vài cột cụ thể
print("Chọn cột customer_id và product_id:")
sdf_sales.select("customer_id", "product_id").display(3)

# Chọn tất cả cột
sdf_sales.select("*").display(3)

# Lấy danh sách tên cột
column_names = sdf_sales.columns
print(f"Danh sách cột: {column_names}")

# COMMAND ----------

# === THÊM CỘT MỚI ===

# Thêm cột với giá trị cố định
sdf_sales_with_country = sdf_sales.withColumn("country", F.lit("Vietnam"))

# Thêm cột tính toán từ các cột khác
sdf_sales_enhanced = sdf_sales_with_country.withColumn(
    "total_amount", F.col("quantity") * F.col("unit_price")
)

# Thêm cột với logic điều kiện
sdf_sales_enhanced = sdf_sales_enhanced.withColumn(
    "order_size",
    F.when(F.col("quantity") >= 5, "Large")
    .when(F.col("quantity") >= 3, "Medium")
    .otherwise("Small"),
)

print("DataFrame sau khi thêm cột:")
sdf_sales_enhanced.display()

# COMMAND ----------

# === ĐỔI TÊN VÀ XÓA CỘT ===

# Đổi tên cột
sdf_sales_renamed = sdf_sales_enhanced.withColumnRenamed("sale_date", "purchase_date")

# Xóa cột không cần thiết
sdf_sales_cleaned = sdf_sales_renamed.drop("country")  # Xóa cột country vừa tạo

print("DataFrame sau khi đổi tên và xóa cột:")
sdf_sales_cleaned.display()

# COMMAND ----------

# === SỬ DỤNG FUNCTIONS VỚI CỘT ===

# Các hàm xử lý chuỗi
sdf_string_ops = sdf_sales.withColumn("product_upper", F.upper("product_id"))
sdf_string_ops = sdf_string_ops.withColumn(
    "customer_product", F.concat("customer_id", F.lit("_"), "product_id")
)

# Các hàm xử lý ngày tháng (chuyển string thành date trước)
sdf_date_ops = sdf_sales.withColumn("sale_date", F.to_date("sale_date", "yyyy-MM-dd"))
sdf_date_ops = sdf_date_ops.withColumn("year", F.year("sale_date"))
sdf_date_ops = sdf_date_ops.withColumn("month", F.month("sale_date"))
sdf_date_ops = sdf_date_ops.withColumn("day_of_week", F.dayofweek("sale_date"))

print("Thao tác với chuỗi và ngày tháng:")
sdf_date_ops.select("sale_date", "year", "month", "day_of_week").display()

# COMMAND ----------

# === USER DEFINED FUNCTION (UDF) ===


# Định nghĩa hàm Python tùy chỉnh
def calculate_discount(quantity, unit_price):
    """Tính discount dựa trên quantity"""
    total = quantity * unit_price
    if total >= 10000:
        return total * 0.1  # 10% discount
    elif total >= 5000:
        return total * 0.05  # 5% discount
    else:
        return 0


# Chuyển thành UDF để dùng trong PySpark
discount_udf = F.udf(calculate_discount, T.DoubleType())

# Áp dụng UDF
sdf_sales_with_discount = sdf_sales.withColumn(
    "discount", discount_udf(F.col("quantity"), F.col("unit_price"))
)

print("DataFrame với discount tính bằng UDF:")
sdf_sales_with_discount.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Thao tác với dòng
# MAGIC
# MAGIC Filter, sort, và xử lý missing values

# COMMAND ----------

# === LỌC DỮ LIỆU (FILTER) ===

# Lọc với điều kiện đơn giản
sdf_high_quantity = sdf_sales.filter(F.col("quantity") >= 3)
print("Đơn hàng có quantity >= 3:")
sdf_high_quantity.display()

# Lọc với nhiều điều kiện (AND)
sdf_expensive_large_orders = sdf_sales.filter(
    (F.col("quantity") >= 2) & (F.col("unit_price") >= 2000)
)
print("Đơn hàng lớn và đắt tiền:")
sdf_expensive_large_orders.display()

# Lọc với điều kiện OR
sdf_specific_customers = sdf_sales.filter(
    (F.col("customer_id") == "C001") | (F.col("customer_id") == "C002")
)
print("Đơn hàng của khách hàng C001 hoặc C002:")
sdf_specific_customers.display()

# Lọc với danh sách giá trị
sdf_selected_products = sdf_sales.filter(
    F.col("product_id").isin(["P001", "P002", "P003"])
)
print("Đơn hàng của sản phẩm P001, P002, P003:")
sdf_selected_products.display()

# COMMAND ----------

# === SẮP XẾP DỮ LIỆU ===

# Sắp xếp tăng dần
sdf_sorted_asc = sdf_sales.orderBy("unit_price")
print("Sắp xếp theo unit_price tăng dần:")
sdf_sorted_asc.display()

# Sắp xếp giảm dần
sdf_sorted_desc = sdf_sales.orderBy(F.desc("unit_price"))
print("Sắp xếp theo unit_price giảm dần:")
sdf_sorted_desc.display()

# Sắp xếp nhiều cột
sdf_multi_sort = sdf_sales.orderBy(["customer_id", F.desc("unit_price")])
print("Sắp xếp theo customer_id tăng, unit_price giảm:")
sdf_multi_sort.display()

# COMMAND ----------

# === XỬ LÝ MISSING VALUES ===

# Tạo DataFrame có giá trị null để demo
sdf_with_nulls = sdf_sales.withColumn(
    "discount_rate",
    F.when(F.col("product_id") == "P001", 0.1)
    .when(F.col("product_id") == "P002", None)  # Null value
    .when(F.col("product_id") == "P003", 0.15)
    .otherwise(None),
)

print("DataFrame có giá trị null:")
sdf_with_nulls.select("product_id", "discount_rate").display()

# Đếm giá trị null
null_count = sdf_with_nulls.filter(F.col("discount_rate").isNull()).count()
print(f"Số dòng có discount_rate null: {null_count}")

# Bổ sung giá trị null
sdf_filled = sdf_with_nulls.fillna(0.05, subset=["discount_rate"])
print("Sau khi bổ sung null bằng 0.05:")
sdf_filled.select("product_id", "discount_rate").display()

# Xóa dòng có giá trị null
sdf_dropped = sdf_with_nulls.dropna(subset=["discount_rate"])
print("Sau khi xóa dòng có null:")
sdf_dropped.select("product_id", "discount_rate").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Nhóm hóa và tổng hợp (GroupBy & Aggregation)
# MAGIC
# MAGIC Tính toán các chỉ số thống kê theo nhóm

# COMMAND ----------

# === NHÓM HÓA CƠ BẢN ===

# Tính toán cơ bản theo từng nhóm
sdf_customer_stats = sdf_sales.groupBy("customer_id").agg(
    F.sum("quantity").alias("total_quantity"),
    F.avg("unit_price").alias("avg_price"),
    F.count("transaction_id").alias("transaction_count"),
    F.min("unit_price").alias("min_price"),
    F.max("unit_price").alias("max_price"),
)

print("Thống kê theo khách hàng:")
sdf_customer_stats.display()

# COMMAND ----------

# === NHÓM HÓA NÂNG CAO ===

# Tính tổng doanh thu và thống kê chi tiết theo cửa hàng
sdf_store_performance = (
    sdf_sales.withColumn("total_amount", F.col("quantity") * F.col("unit_price"))
    .groupBy("store_id")
    .agg(
        F.sum("total_amount").alias("total_revenue"),
        F.avg("total_amount").alias("avg_order_value"),
        F.count("transaction_id").alias("total_orders"),
        F.countDistinct("customer_id").alias("unique_customers"),
        F.countDistinct("product_id").alias("unique_products"),
        F.stddev("total_amount").alias("revenue_std_dev"),
        F.first("store_id").alias("store_code"),  # Just for demo
    )
    .orderBy(F.desc("total_revenue"))
)

print("Hiệu suất theo cửa hàng:")
sdf_store_performance.display()

# COMMAND ----------

# === TỔNG HỢP CÓ ĐIỀU KIỆN ===

# Tính tổng có điều kiện sử dụng CASE WHEN
sdf_conditional_aggregation = (
    sdf_sales.withColumn("total_amount", F.col("quantity") * F.col("unit_price"))
    .groupBy("customer_id")
    .agg(
        # Tổng số đơn hàng lớn (>= 5000)
        F.sum(F.when(F.col("total_amount") >= 5000, 1).otherwise(0)).alias(
            "large_orders"
        ),
        # Tổng doanh thu từ đơn hàng lớn
        F.sum(
            F.when(F.col("total_amount") >= 5000, F.col("total_amount")).otherwise(0)
        ).alias("large_order_revenue"),
        # Tổng doanh thu
        F.sum("total_amount").alias("total_revenue"),
    )
    .withColumn(
        "large_order_percentage",
        F.round((F.col("large_order_revenue") / F.col("total_revenue")) * 100, 2),
    )
)

print("Phân tích đơn hàng lớn theo khách hàng:")
sdf_conditional_aggregation.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Window Functions
# MAGIC
# MAGIC Thực hiện tính toán trên "cửa sổ" dữ liệu mà không cần groupby

# COMMAND ----------

# === RANKING FUNCTIONS ===

# Tạo cột total_amount để ranking
sdf_sales_with_total = sdf_sales.withColumn(
    "total_amount", F.col("quantity") * F.col("unit_price")
)

# Định nghĩa Window specification
customer_window = W.Window.partitionBy("customer_id").orderBy(F.desc("total_amount"))
overall_window = W.Window.orderBy(F.desc("total_amount"))

# Áp dụng các hàm ranking
sdf_ranked = (
    sdf_sales_with_total.withColumn("rank_in_customer", F.rank().over(customer_window))
    .withColumn("dense_rank_in_customer", F.dense_rank().over(customer_window))
    .withColumn("row_number_in_customer", F.row_number().over(customer_window))
    .withColumn("overall_rank", F.rank().over(overall_window))
)

print("Ranking các giao dịch:")
sdf_ranked.select(
    "customer_id",
    "total_amount",
    "rank_in_customer",
    "dense_rank_in_customer",
    "overall_rank",
).display()

# COMMAND ----------

# === WINDOW AGGREGATE FUNCTIONS ===

# Tính tổng running totals và moving averages
date_window = W.Window.orderBy("sale_date")
customer_window_unbounded = W.Window.partitionBy("customer_id")

sdf_window_agg = (
    sdf_sales_with_total.withColumn("sale_date", F.to_date("sale_date", "yyyy-MM-dd"))
    .withColumn("running_total", F.sum("total_amount").over(date_window))
    .withColumn("customer_total", F.sum("total_amount").over(customer_window_unbounded))
    .withColumn("customer_avg", F.avg("total_amount").over(customer_window_unbounded))
)

print("Window aggregations:")
sdf_window_agg.select(
    "customer_id", "sale_date", "total_amount", "running_total", "customer_total"
).orderBy("sale_date").display()

# COMMAND ----------

# === MOVING AVERAGES ===

# Moving average 3 giao dịch gần nhất
moving_window = W.Window.orderBy("sale_date").rowsBetween(-2, 0)

sdf_moving_avg = (
    sdf_sales_with_total.withColumn("sale_date", F.to_date("sale_date", "yyyy-MM-dd"))
    .withColumn("moving_avg_3", F.avg("total_amount").over(moving_window))
    .withColumn("moving_sum_3", F.sum("total_amount").over(moving_window))
)

print("Moving averages (3 giao dịch gần nhất):")
sdf_moving_avg.select(
    "sale_date", "total_amount", "moving_avg_3", "moving_sum_3"
).orderBy("sale_date").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Kết hợp DataFrame (Joins)
# MAGIC
# MAGIC Kết hợp dữ liệu từ nhiều bảng khác nhau

# COMMAND ----------

# === CHUẨN BỊ DỮ LIỆU CHO JOIN ===

# Tạo bảng thông tin khách hàng
customers_data = [
    ("C001", "Nguyen Van A", "VIP", "Ha Noi"),
    ("C002", "Tran Thi B", "Gold", "Ho Chi Minh"),
    ("C003", "Le Van C", "Silver", "Da Nang"),
    ("C004", "Pham Thi D", "VIP", "Ha Noi"),
    ("C005", "Hoang Van E", "Gold", "Hai Phong"),
    ("C006", "Mai Van F", "Silver", "Can Tho"),  # Khách hàng không có giao dịch
]

customers_schema = T.StructType(
    [
        T.StructField("customer_id", T.StringType(), True),
        T.StructField("customer_name", T.StringType(), True),
        T.StructField("tier", T.StringType(), True),
        T.StructField("city", T.StringType(), True),
    ]
)

sdf_customers = spark.createDataFrame(customers_data, customers_schema)

# Tạo bảng thông tin sản phẩm
products_data = [
    ("P001", "iPhone 15", "Electronics", 15000.0),
    ("P002", "Samsung S24", "Electronics", 25000.0),
    ("P003", "Nike Shoes", "Fashion", 8000.0),
    ("P004", "MacBook Pro", "Electronics", 45000.0),
    ("P005", "Adidas Shirt", "Fashion", 1200.0),
]

products_schema = T.StructType(
    [
        T.StructField("product_id", T.StringType(), True),
        T.StructField("product_name", T.StringType(), True),
        T.StructField("category", T.StringType(), True),
        T.StructField("list_price", T.DoubleType(), True),
    ]
)

sdf_products = spark.createDataFrame(products_data, products_schema)

print("Bảng khách hàng:")
sdf_customers.display()
print("Bảng sản phẩm:")
sdf_products.display()

# COMMAND ----------

# === CÁC LOẠI JOIN ===

# 1. INNER JOIN - Chỉ lấy dữ liệu có match ở cả 2 bên
sdf_inner = sdf_sales.join(sdf_customers, on="customer_id", how="inner")
print("INNER JOIN (Sales + Customers):")
sdf_inner.select(
    "transaction_id", "customer_id", "customer_name", "tier", "product_id"
).display()

# 2. LEFT JOIN - Giữ tất cả dữ liệu bên trái
sdf_left = sdf_sales.join(sdf_customers, on="customer_id", how="left")
print("LEFT JOIN (giữ tất cả sales):")
print(f"Số dòng sales gốc: {sdf_sales.count()}, sau left join: {sdf_left.count()}")
sdf_left.display()

# 3. RIGHT JOIN - Giữ tất cả dữ liệu bên phải
sdf_right = sdf_sales.join(sdf_customers, on="customer_id", how="right")
print("RIGHT JOIN (giữ tất cả customers):")
print(
    f"Số dòng customers: {sdf_customers.count()}, sau right join: {sdf_right.count()}"
)
# sdf_right.filter(F.col("transaction_id").isNull()).display()  # Customers không có giao dịch
sdf_right.display()

# 4. FULL OUTER JOIN - Giữ tất cả dữ liệu từ cả 2 bên
sdf_full = sdf_sales.join(sdf_customers, on="customer_id", how="outer")
print("FULL OUTER JOIN:")
print(f"Số dòng sau full join: {sdf_full.count()}")
sdf_full.display()

# COMMAND ----------

# === JOIN NHIỀU BẢNG ===

# Kết hợp cả 3 bảng: Sales + Customers + Products
sdf_complete = sdf_sales.join(sdf_customers, "customer_id", "left").join(
    sdf_products, "product_id", "left"
)

print("Dữ liệu hoàn chỉnh (3 bảng):")
sdf_complete.select(
    "transaction_id",
    "customer_name",
    "product_name",
    "category",
    "quantity",
    "unit_price",
    "tier",
).display()

# Tính toán trên dữ liệu đã join
sdf_analysis = sdf_complete.withColumn(
    "total_amount", F.col("quantity") * F.col("unit_price")
).withColumn(
    "discount_amount",
    F.when(F.col("tier") == "VIP", F.col("total_amount") * 0.1)
    .when(F.col("tier") == "Gold", F.col("total_amount") * 0.05)
    .otherwise(0),
)

print("Phân tích với discount theo tier:")
sdf_analysis.select(
    "customer_name", "tier", "total_amount", "discount_amount"
).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Lưu trữ

# COMMAND ----------

# Lưu dưới dạng CSV
# output_path_csv = "/path/to/save/sdf_output.csv"
# sdf_sales.write.csv(output_path_csv, header=True, mode="overwrite")
# sdf_sales.write.csv(output_path_csv, header=True, mode="append")

# COMMAND ----------

# MAGIC %md
# MAGIC Khi dữ liệu đã tồn tại, có thể ghi đè dữ liệu bằng cách chỉ định `mode` thành `overwrite`

# COMMAND ----------

# # Lưu dưới dạng Parquet
# output_path_parquet = "/path/to/save/sdf_output.parquet"
# sdf_sales.write.parquet(output_path_parquet, mode="overwrite")

# COMMAND ----------

# # Lưu dưới dạng JSON
# output_path_json = "/path/to/save/sdf_output.json"
# sdf_sales.write.json(output_path_json, mode="overwrite")

# COMMAND ----------

# Lưu dưới dạng table
sdf_left.write.saveAsTable("sales_table", mode="overwrite")

# COMMAND ----------

# MAGIC %md
# MAGIC * Lưu DataFrame thành nhiều file. Được sử dụng để quản lý kích thước file dễ dàng hơn khi xử lý DataFrame quy mô lớn.
# MAGIC   * Sử dụng `repartition(10)` để chia DataFrame thành 10 partition.
# MAGIC   * File CSV riêng biệt được lưu cho mỗi partition được chia.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM workspace.default.sales_table

# COMMAND ----------

# # Lưu dưới dạng CSV được chia thành nhiều file
# output_path_split = "/path/to/save/sdf_output_split"
# sdf_sales.repartition(10).write.csv(output_path_split, header=True, mode="overwrite")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Chuyển đổi đối tượng

# COMMAND ----------

# MAGIC %md
# MAGIC * `pyspark.sql.dataframe.DataFrame` -> `pandas.DataFrame`

# COMMAND ----------

# MAGIC %md
# MAGIC Sử dụng `toPandas` của DataFrame có thể chuyển đổi DataFrame của PySpark thành DataFrame của Pandas.
# MAGIC Phương thức `toPandas` có hiệu năng khá kém, nên khuyến nghị phương pháp lưu `pyspark.sql.dataframe.DataFrame` vào file một lần rồi đọc file đã lưu bằng `pandas`.

# COMMAND ----------

pdf = sdf_sales.toPandas()
pdf

# COMMAND ----------

# MAGIC %md
# MAGIC * `pyspark.sql.dataframe.DataFrame` -> `List`

# COMMAND ----------

rows = sdf_sales.select("*").collect()
rows

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Lazy Evaluation (Đánh giá trì hoãn)

# COMMAND ----------

# MAGIC %md
# MAGIC Các phương thức của DataFrame được phân loại lớn thành hai hệ thống xử lý: `transformation` và `action`. `transformation` tương ứng với xử lý lọc, định dạng, kết hợp dữ liệu (: `join`, `groupBy`), `action` tương ứng với xử lý hiển thị và lưu dữ liệu (: `show`, `write`).
# MAGIC
# MAGIC Trong lazy evaluation của Spark, khi thực hiện `transformation`, chỉ đăng ký nội dung xử lý vào scheduler mà không thực hiện xử lý thực tế, khi `action` được thực hiện, xử lý được thực hiện trên DataFrame dựa trên nội dung của scheduler.
# MAGIC
# MAGIC Ưu điểm của lazy evaluation là
# MAGIC
# MAGIC * Có thể kiểm soát để dữ liệu không cần thiết không được tải lên memory nhiều nhất có thể
# MAGIC * Có thể tối ưu hóa một loạt xử lý từ transformation đến action
# MAGIC
# MAGIC v.v.

# COMMAND ----------

# Định nghĩa dữ liệu mẫu
data = [
    ("Alice", 25, "F"),
    ("Bob", 30, "M"),
    ("Charlie", 35, "M"),
    ("David", 40, "M"),
    ("Eve", 45, "F"),
]

# Định nghĩa schema
schema = T.StructType(
    [
        T.StructField("name", T.StringType(), True),
        T.StructField("age", T.IntegerType(), True),
        T.StructField("gender", T.StringType(), True),
    ]
)

# Tạo DataFrame
sdf_demo = spark.createDataFrame(data, schema)

sdf_demo.display()

# COMMAND ----------

# MAGIC %md
# MAGIC **Ví dụ**

# COMMAND ----------

# MAGIC %md
# MAGIC Dưới đây thực hiện xử lý thuộc về transformation trên DataFrame.
# MAGIC Trước khi gọi xử lý thuộc về action, xử lý thực tế không được thực hiện mà chỉ tạo scheduler xử lý.

# COMMAND ----------

# Ví dụ về lazy evaluation - Thao tác transformation
# 1. Thêm cột nhân đôi tuổi
sdf_transformed = sdf_demo.withColumn("age_double", F.col("age") * 2)


# 2. Lọc chỉ nam giới
sdf_filtered = sdf_transformed.filter(F.col("gender") == "M")


# 3. Chuyển đổi tên thành chữ hoa
sdf_final = sdf_filtered.withColumn("name_upper", F.upper(F.col("name")))

# Chưa được đánh giá cho đến khi action được gọi
print("Các thao tác transformation chưa được thực hiện.")


# Thực sự thực hiện action (ví dụ: hiển thị)
# sdf_final.display()

# COMMAND ----------

# MAGIC %md
# MAGIC Tại thời điểm gọi action, tất cả transformer được thực hiện dựa trên nội dung của scheduler

# COMMAND ----------

# MAGIC %md
# MAGIC Nếu không xem xét lazy evaluation mà tiếp tục thêm transformer vào scheduler, tại thời điểm thực hiện action, xử lý rất nặng sẽ được thực hiện trên dữ liệu khổng lồ.
# MAGIC Khuyến nghị lưu vào disk sau khi kết hợp bảng: `join` hoặc lọc dữ liệu khổng lồ, để có thể tham chiếu kết quả trung gian với tốc độ cao.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bài tập Hands-on: Phân tích dữ liệu bán hàng
# MAGIC
# MAGIC **Mục tiêu**: Xây dựng dashboard phân tích kinh doanh từ dữ liệu thô

# COMMAND ----------

# === CHUẨN BỊ DỮ LIỆU CHO BÀI TẬP ===

# Tạo dữ liệu bán hàng (1 tháng)
extended_sales_data = [
    # Tuần 1
    ("TXN001", "C001", "P001", 2, 15000, "2024-01-01", "S001"),
    ("TXN002", "C002", "P002", 1, 25000, "2024-01-01", "S002"),
    ("TXN003", "C003", "P003", 3, 8000, "2024-01-02", "S001"),
    ("TXN004", "C001", "P004", 1, 45000, "2024-01-03", "S003"),
    ("TXN005", "C004", "P001", 5, 15000, "2024-01-04", "S002"),
    # Tuần 2
    ("TXN006", "C002", "P005", 2, 12000, "2024-01-08", "S001"),
    ("TXN007", "C005", "P002", 1, 25000, "2024-01-09", "S003"),
    ("TXN008", "C003", "P003", 4, 8000, "2024-01-10", "S002"),
    ("TXN009", "C006", "P001", 3, 15000, "2024-01-11", "S001"),
    # Tuần 3
    ("TXN010", "C001", "P002", 2, 25000, "2024-01-15", "S002"),
    ("TXN011", "C004", "P004", 1, 45000, "2024-01-16", "S003"),
    ("TXN012", "C002", "P003", 3, 8000, "2024-01-17", "S001"),
    ("TXN013", "C005", "P005", 4, 12000, "2024-01-18", "S002"),
    # Tuần 4
    ("TXN014", "C003", "P001", 6, 15000, "2024-01-22", "S001"),
    ("TXN015", "C006", "P002", 1, 25000, "2024-01-23", "S003"),
    ("TXN016", "C001", "P004", 2, 45000, "2024-01-24", "S002"),
    ("TXN017", "C004", "P003", 5, 8000, "2024-01-25", "S001"),
    ("TXN018", "C002", "P005", 3, 12000, "2024-01-26", "S003"),
]

# Schema giữ nguyên
sdf_sales_extended = spark.createDataFrame(extended_sales_data, sales_schema)

# Thông tin chi tiết khách hàng
customers_detailed = [
    ("C001", "Nguyen Van A", "VIP", "M", "1990-05-15", "Ha Noi"),
    ("C002", "Tran Thi B", "Gold", "F", "1985-08-22", "Ho Chi Minh"),
    ("C003", "Le Van C", "Silver", "M", "1992-03-10", "Da Nang"),
    ("C004", "Pham Thi D", "VIP", "F", "1988-12-05", "Ha Noi"),
    ("C005", "Hoang Van E", "Gold", "M", "1995-07-18", "Hai Phong"),
    ("C006", "Mai Van F", "Silver", "F", "1987-11-30", "Can Tho"),
]

customers_detailed_schema = T.StructType(
    [
        T.StructField("customer_id", T.StringType(), True),
        T.StructField("customer_name", T.StringType(), True),
        T.StructField("tier", T.StringType(), True),
        T.StructField("gender", T.StringType(), True),
        T.StructField("birth_date", T.StringType(), True),
        T.StructField("city", T.StringType(), True),
    ]
)

sdf_customers_detailed = spark.createDataFrame(
    customers_detailed, customers_detailed_schema
)

# Thông tin sản phẩm
products_extended = [
    ("P001", "iPhone 15 Pro", "Electronics", "Apple", 15000),
    ("P002", "Samsung Galaxy S24", "Electronics", "Samsung", 25000),
    ("P003", "Nike Air Max 270", "Fashion", "Nike", 8000),
    ("P004", "MacBook Pro M3", "Electronics", "Apple", 45000),
    ("P005", "Adidas Ultraboost", "Fashion", "Adidas", 12000),
]

products_extended_schema = T.StructType(
    [
        T.StructField("product_id", T.StringType(), True),
        T.StructField("product_name", T.StringType(), True),
        T.StructField("category", T.StringType(), True),
        T.StructField("brand", T.StringType(), True),
        T.StructField("list_price", T.IntegerType(), True),
    ]
)

sdf_products_extended = spark.createDataFrame(
    products_extended, products_extended_schema
)

print("DỮ LIỆU CHO BÀI TẬP HANDS-ON")
print("=" * 50)
print(f"Sales: {sdf_sales_extended.count()} giao dịch")
print(f"Customers: {sdf_customers_detailed.count()} khách hàng")
print(f"Products: {sdf_products_extended.count()} sản phẩm")

# COMMAND ----------

# === BÀI TẬP 1: DATA PREPARATION & CLEANING ===

print("BÀI TẬP 1: DATA PREPARATION & CLEANING")
print("=" * 50)

# Bước 1: Tạo master dataset
sdf_master = sdf_sales_extended.join(
    sdf_customers_detailed, "customer_id", "left"
).join(sdf_products_extended, "product_id", "left")

# Bước 2: Làm sạch và enriching data
sdf_clean = (
    sdf_master.withColumn("sale_date", F.to_date("sale_date", "yyyy-MM-dd"))
    .withColumn("total_amount", F.col("quantity") * F.col("unit_price"))
    .withColumn("birth_date", F.to_date("birth_date", "yyyy-MM-dd"))
    .withColumn("age", F.floor(F.datediff(F.current_date(), "birth_date") / 365.25))
    .withColumn("week_of_year", F.weekofyear("sale_date"))
    .withColumn("day_name", F.date_format("sale_date", "EEEE"))
    .withColumn(
        "is_weekend",
        F.when(F.dayofweek("sale_date").isin([1, 7]), "Yes").otherwise("No"),
    )
)

# Bước 3: Thêm business logic
sdf_enriched = (
    sdf_clean.withColumn(
        "discount_rate",
        F.when(F.col("tier") == "VIP", 0.10)
        .when(F.col("tier") == "Gold", 0.05)
        .otherwise(0.02),
    )
    .withColumn("discount_amount", F.col("total_amount") * F.col("discount_rate"))
    .withColumn("net_amount", F.col("total_amount") - F.col("discount_amount"))
)

print("Data cleaned and enriched!")
print("🔍 Sample của master dataset:")
sdf_enriched.select(
    "transaction_id",
    "customer_name",
    "product_name",
    "total_amount",
    "discount_amount",
    "net_amount",
    "week_of_year",
).display()

# COMMAND ----------

# === BÀI TẬP 2: SALES PERFORMANCE ANALYSIS ===

print("BÀI TẬP 2: SALES PERFORMANCE ANALYSIS")
print("=" * 50)

# 2.1 Tổng quan doanh thu
total_metrics = sdf_enriched.agg(
    F.sum("net_amount").alias("total_revenue"),
    F.avg("net_amount").alias("avg_order_value"),
    F.count("transaction_id").alias("total_orders"),
    F.countDistinct("customer_id").alias("unique_customers"),
).collect()[0]

print("TỔNG QUAN KINH DOANH:")
print(f"Tổng doanh thu: {total_metrics['total_revenue']:,.0f} VND")
print(f"Giá trị đơn hàng TB: {total_metrics['avg_order_value']:,.0f} VND")
print(f"Tổng số đơn hàng: {total_metrics['total_orders']}")
print(f"Khách hàng unique: {total_metrics['unique_customers']}")

# 2.2 Phân tích theo sản phẩm
sdf_product_performance = (
    sdf_enriched.groupBy("product_name", "category", "brand")
    .agg(
        F.sum("net_amount").alias("revenue"),
        F.sum("quantity").alias("quantity_sold"),
        F.count("transaction_id").alias("orders"),
        F.avg("net_amount").alias("avg_order_value"),
    )
    .withColumn("rank", F.rank().over(W.Window.orderBy(F.desc("revenue"))))
    .orderBy("rank")
)

print("\n TOP SẢN PHẨM THEO DOANH THU:")
sdf_product_performance.display()

# 2.3 Phân tích theo tuần
sdf_weekly_trend = (
    sdf_enriched.groupBy("week_of_year")
    .agg(
        F.sum("net_amount").alias("weekly_revenue"),
        F.count("transaction_id").alias("weekly_orders"),
        F.countDistinct("customer_id").alias("weekly_customers"),
    )
    .orderBy("week_of_year")
)

print("XU HƯỚNG THEO TUẦN:")
sdf_weekly_trend.display()

# COMMAND ----------

# === BÀI TẬP 3: CUSTOMER ANALYSIS ===

print("BÀI TẬP 3: CUSTOMER ANALYSIS")
print("=" * 50)

# 3.1 Phân tích khách hàng chi tiết
sdf_customer_analysis = (
    sdf_enriched.groupBy(
        "customer_id", "customer_name", "tier", "gender", "age", "city"
    )
    .agg(
        F.sum("net_amount").alias("total_spent"),
        F.count("transaction_id").alias("order_frequency"),
        F.avg("net_amount").alias("avg_order_value"),
        F.countDistinct("product_id").alias("product_variety"),
        F.max("sale_date").alias("last_purchase"),
        F.min("sale_date").alias("first_purchase"),
    )
    .withColumn("customer_lifetime_days", F.datediff("last_purchase", "first_purchase"))
    .withColumn(
        "rank_by_revenue", F.rank().over(W.Window.orderBy(F.desc("total_spent")))
    )
)

print("TOP KHÁCH HÀNG:")
sdf_customer_analysis.orderBy("rank_by_revenue").display(10)

# 3.2 Phân tích theo demographics
sdf_demographic_analysis = (
    sdf_enriched.groupBy("gender", "tier")
    .agg(
        F.sum("net_amount").alias("revenue"),
        F.countDistinct("customer_id").alias("customers"),
        F.avg("net_amount").alias("avg_order_value"),
    )
    .withColumn(
        "revenue_per_customer", F.round(F.col("revenue") / F.col("customers"), 0)
    )
    .orderBy("gender", "tier")
)

print("PHÂN TÍCH THEO GIỚI TÍNH & TIER:")
sdf_demographic_analysis.display()

# 3.3 RFM Analysis cơ bản
current_date = F.lit("2024-01-31")
sdf_rfm_analysis = sdf_enriched.groupBy("customer_id", "customer_name").agg(
    F.datediff(current_date, F.max("sale_date")).alias("recency"),
    F.count("transaction_id").alias("frequency"),
    F.sum("net_amount").alias("monetary"),
)

print("RFM ANALYSIS:")
sdf_rfm_analysis.orderBy(F.desc("monetary")).display()

# COMMAND ----------

# === BÀI TẬP 4: ADVANCED ANALYTICS WITH WINDOW FUNCTIONS ===

print("BÀI TẬP 4: ADVANCED ANALYTICS")
print("=" * 50)

# 4.1 Customer purchase patterns
customer_pattern_window = W.Window.partitionBy("customer_id").orderBy("sale_date")

sdf_customer_patterns = (
    sdf_enriched.withColumn(
        "days_between_purchases",
        F.datediff(
            F.col("sale_date"), F.lag("sale_date", 1).over(customer_pattern_window)
        ),
    )
    .withColumn("purchase_sequence", F.row_number().over(customer_pattern_window))
    .withColumn(
        "running_customer_total", F.sum("net_amount").over(customer_pattern_window)
    )
)

print("CUSTOMER PURCHASE PATTERNS:")
sdf_customer_patterns.filter(F.col("purchase_sequence") > 1).select(
    "customer_name",
    "sale_date",
    "net_amount",
    "days_between_purchases",
    "running_customer_total",
).orderBy("customer_name", "sale_date").display()

# 4.2 Product trend analysis
product_trend_window = W.Window.partitionBy("product_name").orderBy("week_of_year")

sdf_product_trends = (
    sdf_enriched.groupBy("product_name", "week_of_year")
    .agg(
        F.sum("quantity").alias("weekly_quantity"),
        F.sum("net_amount").alias("weekly_revenue"),
    )
    .withColumn(
        "quantity_growth",
        (
            F.col("weekly_quantity")
            - F.lag("weekly_quantity", 1).over(product_trend_window)
        )
        / F.lag("weekly_quantity", 1).over(product_trend_window)
        * 100,
    )
    .withColumn(
        "revenue_growth",
        (
            F.col("weekly_revenue")
            - F.lag("weekly_revenue", 1).over(product_trend_window)
        )
        / F.lag("weekly_revenue", 1).over(product_trend_window)
        * 100,
    )
)

print("PRODUCT WEEKLY TRENDS:")
sdf_product_trends.filter(F.col("quantity_growth").isNotNull()).select(
    "product_name", "week_of_year", "weekly_quantity", "quantity_growth"
).orderBy("product_name", "week_of_year").display()



