# Introduction_Databricks

Giới thiệu
Repository này cung cấp một hướng dẫn chi tiết về cách sử dụng Databricks và PySpark để phân tích dữ liệu. Bạn sẽ học được từ những khái niệm cơ bản đến các kỹ thuật nâng cao trong xử lý dữ liệu lớn.

Mục tiêu học tập
Sau khi hoàn thành tutorial này, bạn sẽ:

✅ Hiểu được vai trò và lợi ích của Databricks trong hệ sinh thái dữ liệu hiện đại

✅ Hiểu kiến trúc Lakehouse và cách hoạt động của Delta 

✅ Thành thạo PySpark DataFrame API cho xử lý dữ liệu có cấu trúc

✅ Áp dụng các kỹ thuật phân tích dữ liệu

Cấu trúc nội dung
1. Giới thiệu về Databricks
Bối cảnh lịch sử: Sự phát triển của Big Data (1970-nay)
Thách thức truyền thống: Hạ tầng phức tạp, Data silos, chi phí cao
Giải pháp Databricks: Platform thống nhất, cloud-native, auto-scaling
Kiến trúc Lakehouse: Kết hợp Data Warehouse và Data Lake

2. Các thành phần cốt lõi của Databricks
Databricks Workspace & Notebooks: Môi trường cộng tác
Databricks SQL: Kho dữ liệu serverless hiệu suất cao
Delta Lake: Lớp lưu trữ đáng tin cậy với ACID transactions
Unity Catalog: Quản trị dữ liệu tập trung
Clusters: Tài nguyên tính toán linh hoạt
Workflows: Tự động hóa pipeline dữ liệu
Machine Learning: Nền tảng ML/AI end-to-end

3. PySpark căn bản
Kiến trúc Spark: Driver-Executor model
SparkSession: Entry point cho ứng dụng Spark
DataFrame API: Làm việc với dữ liệu có cấu trúc
Lazy Evaluation: Tối ưu hóa kế hoạch thực thi

4. Thao tác với PySpark DataFrame
Thao tác cơ bản
Tạo và hiển thị DataFrame
Chọn, thêm, xóa và đổi tên cột
Lọc và sắp xếp dữ liệu
Xử lý missing values
Thao tác nâng cao
Aggregations: GroupBy và các hàm tổng hợp
Window Functions: Ranking, running totals, moving averages
Joins: Inner, outer, left, right joins
UDFs: User Defined Functions tùy chỉnh
Bài tập thực hành
Dự án: Phân tích dữ liệu bán hàng
Một bài tập hands-on hoàn chỉnh bao gồm:

Data Preparation & Cleaning

Tạo master dataset từ multiple tables
Làm sạch và enriching data
Thêm business logic
Sales Performance Analysis

Tổng quan doanh thu và metrics
Phân tích theo sản phẩm và thời gian
Trend analysis theo tuần
Customer Analysis

Customer segmentation chi tiết
Demographic analysis
RFM Analysis cơ bản
Advanced Analytics

Customer purchase patterns
Product trend analysis với Window Functions
Predictive insights
Yêu cầu hệ thống
Databricks Workspace (Community Edition hoặc Trial)
Python 3.7+
PySpark (được cài đặt sẵn trên Databricks)

Cách sử dụng
Clone repository
Import notebook vào Databricks:

Đăng nhập vào Databricks Workspace
Import file pyspark_databricks_tutorial.py
Attach notebook vào cluster
Chạy từng cell theo thứ tự:

Đọc kỹ comments và markdown cells
Thực thi code cells và quan sát kết quả
Thử modify code để hiểu rõ hơn
Thực hành với dữ liệu thật:

Áp dụng các kỹ thuật đã học với dataset của bạn
Thử nghiệm với các use cases khác nhau
Các khái niệm quan trọng
Databricks Lakehouse
Unified Platform: Tích hợp data engineering, ML, và analytics
Delta Lake: ACID transactions, schema evolution, time travel
Medallion Architecture: Bronze → Silver → Gold data layers
PySpark Core Concepts
RDD: Resilient Distributed Dataset
DataFrame: Structured data với schema
Transformations vs Actions: Lazy evaluation pattern
Catalyst Optimizer: Query optimization engine
Performance Best Practices
Data Organization: Partitioning, file sizing, Z-ordering
Compute Optimization: Cluster sizing, auto-scaling
Query Optimization: Predicate pushdown, broadcast joins, caching
Memory Management: Serializers, GC tuning, storage levels
Tài liệu tham khảo
Databricks Documentation
Apache Spark Documentation
PySpark API Reference
Delta Lake Documentation
