# Evaluating Apache DataFusion Comet for Accelerating Apache Spark Workloads in an Open Lakehouse Architecture

---

## Mục lục
1. [Tổng quan Đề tài & Mục tiêu Nghiên cứu](#1-tổng-quan-đề-tài--mục-tiêu-nghiên-cứu)
2. [Tài liệu Tham khảo & Nền tảng Học thuật](#2-tài-liệu-tham-khảo--nền-tảng-học-thuật)
3. [Kiến trúc Hệ thống & Cơ chế Hoạt động](#3-kiến-trúc-hệ-thống--cơ-chế-hoạt-động)
4. [Thiết kế Data Pipeline (Medallion Architecture)](#4-thiết-kế-data-pipeline-medallion-architecture)
5. [Thiết kế Workload & Bộ Dữ liệu Thử nghiệm (Benchmark Suite)](#5-thiết-kế-workload--bộ-dữ-liệu-thử-nghiệm-benchmark-suite)
6. [Phương pháp Đo lường & Quy trình Thực nghiệm (Benchmarking Methodology)](#6-phương-pháp-đo-lường--quy-trình-thực-nghiệm-benchmarking-methodology)
7. [Quản lý Bộ nhớ & Tinh chỉnh Hiệu năng (Memory Tuning & Stability)](#7-quản-lý-bộ-nhớ--tinh-chỉnh-hiệu-năng-memory-tuning--stability)
8. [Công cụ Tự động hóa & Phân tích (Benchmark Automation & Tooling)](#8-công-cụ-tự-động-hóa--phân-tích-benchmark-automation--tooling)
9. [Cấu trúc Thư mục Dự án Chuẩn (Repository Layout)](#9-cấu-trúc-thư-mục-dự-án-chuẩn-repository-layout)
10. [Kế hoạch Triển khai 8 Tuần (8-Week Roadmap)](#10-kế-hoạch-triển-khai-8-tuần-8-week-roadmap)
11. [Tiêu chí Đánh giá & Sản phẩm Đầu ra (Deliverables)](#11-tiêu-chí-đánh-giá--sản-phẩm-đầu-ra-deliverables)

---

## 1. Tổng quan Đề tài & Mục tiêu Nghiên cứu

### 1.1. Bối cảnh & Đặt vấn đề
Trong các hệ thống Lakehouse hiện đại, **Apache Spark** là nền tảng xử lý dữ liệu phân tán phổ biến nhất. Tuy nhiên, kiến trúc dựa trên máy ảo Java (JVM) của Spark gặp phải các giới hạn vật lý cố hữu:
- Chi phí quản lý bộ nhớ và Garbage Collection (GC) cao khi xử lý lượng dữ liệu khổng lồ.
- Không tận dụng triệt để kiến trúc phần cứng hiện đại (SIMD vectorization, cache locality, CPU branch prediction).
- Chi phí tuần tự hóa/giải tuần tự hóa (serialization/deserialization) dữ liệu dạng dòng/cột trên JVM.

Để giải quyết nút thắt này, cộng đồng Apache DataFusion đã phát triển **Comet** (Apache DataFusion Comet) — một Spark plugin giúp thay thế Spark physical plan operators bằng các native operators được viết bằng Rust trên nền tảng **Apache DataFusion** và **Apache Arrow**, trong khi vẫn giữ nguyên Spark DataFrame/SQL API và ngữ nghĩa tính toán của Spark.

### 1.2. Mục tiêu Nghiên cứu Cốt lõi
Đề tài tập trung trả lời câu hỏi trung tâm:
> **"Cùng một pipeline dữ liệu trên kiến trúc Open Lakehouse (MinIO + Parquet + Iceberg), khi bật DataFusion Comet thì hiệu năng thực thi (Speedup), tài nguyên tiêu thụ (CPU/RAM/GC/Shuffle) thay đổi như thế nào, trên những workload nào và nguyên nhân sâu xa (Plan / Operator / Fallback) là gì?"**

### 1.3. Ba Câu hỏi Nghiên cứu (Research Questions)
* **RQ1 (Speedup & Throughput)**: DataFusion Comet cải thiện hiệu năng (thời gian thực thi, latency p50/p95) của Spark SQL bao nhiêu % trên các lớp workload khác nhau (Scan-heavy, Join-heavy, Aggregation-heavy, Window-heavy)?
* **RQ2 (Operator Suitability & Coverage)**: Những operator và biểu thức nào đạt hiệu quả gia tốc native cao nhất, và những thành phần nào thường xuyên bị fallback về Spark JVM?
* **RQ3 (Scalability & Fallback Overhead)**: Mức độ gia tốc thay đổi ra sao khi quy mô dữ liệu tăng dần (1 GB, 10 GB, 50 GB, 100 GB), và chi phí chuyển đổi định dạng (Arrow-JVM conversion) khi xảy ra fallback ảnh hưởng như thế nào đến tổng thời gian thực thi?

### 1.4. Phạm vi & Giới hạn Công nghệ
* **Tech Stack Trọng tâm**: Spark 4.1.x (Java 17) + Apache DataFusion Comet 1.0.0 + Apache Iceberg + Apache Parquet + MinIO (S3-compatible Object Storage).
* **Môi trường Thực thi**: Linux x86_64 / ARM64 (chạy qua Docker / WSL2 Ubuntu để đảm bảo tương thích với các pre-built native binaries của Comet).
* **Phạm vi Loại trừ**: Không triển khai các công cụ BI (như Superset, Trino) hoặc hạ tầng Monitoring phức tạp (như Prometheus, Grafana) để tránh phân tán nguồn lực; thay vào đó sử dụng **Spark UI**, **Extended Execution Plan Log** và **Bộ công cụ Benchmark Runner nội tại** để thu thập metrics chính xác.

---

## 2. Tài liệu Tham khảo & Nền tảng Học thuật

### 2.1. Bài báo Khoa học Nền tảng (Academic Papers)
1. **Databricks Photon Paper (SIGMOD 2022)**:
   - *Alexander Behm et al.* — *"Photon: A Fast Query Engine for Lakehouse Systems"*, Proceedings of the 2022 International Conference on Management of Data (SIGMOD '22), pp. 2326–2339.
   - *Giá trị nghiên cứu*: Giải thích chi tiết sự chuyển dịch từ JVM-based query execution sang Native Vectorized Execution Engine (C++/Rust) để tối ưu hóa tận dụng phần cứng hiện đại.
2. **Meta Velox Paper (VLDB 2022)**:
   - *Pedro Pedreira et al.* — *"Velox: A Unified Execution Engine for Data Management"*, Proceedings of the VLDB Endowment, Vol. 15, No. 12.
   - *Giá trị nghiên cứu*: Kiến trúc chuẩn hóa cho execution engine vector hóa dạng module (composable native execution) với Apache Arrow.
3. **Apache Arrow Format Specification**:
   - Đặc tả chuẩn in-memory columnar representation giúp chia sẻ bộ nhớ zero-copy giữa JVM và native code.

### 2.2. Documentation & Repositories Chính thức
1. **Apache DataFusion Comet**:
   - Official Documentation: [https://datafusion.apache.org/comet/](https://datafusion.apache.org/comet/)
   - Architecture & Compatibility: [https://datafusion.apache.org/comet/user-guide/overview.html](https://datafusion.apache.org/comet/user-guide/overview.html)
   - Supported Operators & Expressions: [https://datafusion.apache.org/comet/user-guide/supported-operators.html](https://datafusion.apache.org/comet/user-guide/supported-operators.html)
   - Configuration Parameters: [https://datafusion.apache.org/comet/user-guide/configs.html](https://datafusion.apache.org/comet/user-guide/configs.html)
   - GitHub Repository: [https://github.com/apache/datafusion-comet](https://github.com/apache/datafusion-comet)
   - Official Benchmark Scripts: [https://github.com/apache/datafusion-comet/tree/main/benchmarks](https://github.com/apache/datafusion-comet/tree/main/benchmarks)

2. **Apache DataFusion & Arrow**:
   - Query Engine Architecture: [https://datafusion.apache.org/](https://datafusion.apache.org/)
   - Arrow Columnar Format: [https://arrow.apache.org/docs/format/Columnar.html](https://arrow.apache.org/docs/format/Columnar.html)

3. **Apache Iceberg**:
   - Documentation & Table Spec: [https://iceberg.apache.org/docs/latest/](https://iceberg.apache.org/docs/latest/)
   - Spark with Iceberg Integration: [https://iceberg.apache.org/docs/latest/spark-getting-started/](https://iceberg.apache.org/docs/latest/spark-getting-started/)

4. **Apache Spark**:
   - Spark SQL & Catalyst Optimizer: [https://spark.apache.org/docs/latest/sql-programming-guide.html](https://spark.apache.org/docs/latest/sql-programming-guide.html)
   - Spark Performance Tuning: [https://spark.apache.org/docs/latest/tuning.html](https://spark.apache.org/docs/latest/tuning.html)

5. **TPC Benchmark™ H (TPC-H)**:
   - TPC-H Standard Specification: [https://www.tpc.org/tpch/](https://www.tpc.org/tpch/)

---

## 3. Kiến trúc Hệ thống & Cơ chế Hoạt động

### 3.1. Kiến trúc Tổng thể (Lean Lakehouse Stack)

Kiến trúc được tinh giản tối đa nhằm tập trung tuyệt đối vào quá trình tính toán và đo lường gia tốc:

```
┌────────────────────────────────────────────────────────────────────────┐
│                              DATA SOURCES                              │
│         E-Commerce Generator / PostgreSQL Dump / TPC-H Generator        │
└───────────────────────────────────┬────────────────────────────────────┘
                                    │ Ingestion
                                    ▼
┌────────────────────────────────────────────────────────────────────────┐
│                        BRONZE LAYER (Raw Store)                        │
│             MinIO (S3-compatible) / Parquet Time-Partitioned           │
└───────────────────────────────────┬────────────────────────────────────┘
                                    │
                                    │ Spark SQL Execution
                                    ▼
┌────────────────────────────────────────────────────────────────────────┐
│                 PROCESSING & ACCELERATION ENGINE                       │
│                                                                        │
│   ┌────────────────────────────────────────────────────────────────┐   │
│   │                        APACHE SPARK                            │   │
│   │           DataFrame API / SQL Parser / Catalyst Optimizer      │   │
│   └───────────────────────────────┬────────────────────────────────┘   │
│                                   │ Spark Physical Plan                │
│                                   ▼                                    │
│   ┌────────────────────────────────────────────────────────────────┐   │
│   │                    COMET PLUGIN INTERCEPT                      │   │
│   │         Scans plan, delegates compatible subtrees to Rust      │   │
│   └───────────────────────────────┬────────────────────────────────┘   │
│                   ┌───────────────┴───────────────┐                    │
│                   │ Supported                     │ Unsupported        │
│                   ▼                               ▼                    │
│   ┌───────────────────────────────┐   ┌────────────────────────────┐   │
│   │       DATAFUSION COMET        │   │         SPARK JVM          │   │
│   │  - Rust Native Execution      │   │  - Fallback Execution      │   │
│   │  - Arrow Columnar Format      │   │  - Row/UnsafeRow Format    │   │
│   │  - Native Parquet/Iceberg Scan│   │  - Standard Catalyst Exec  │   │
│   │  - Native Hash Join / Shuffle │   │                            │   │
│   └───────────────────────────────┘   └────────────────────────────┘   │
└───────────────────────────────────┬────────────────────────────────────┘
                                    │
                                    ▼
┌────────────────────────────────────────────────────────────────────────┐
│                    SILVER & GOLD LAYERS (Iceberg)                      │
│        Apache Iceberg Tables (Cleaned, Joined, Aggregated, Marts)      │
└────────────────────────────────────────────────────────────────────────┘
```

### 3.2. Bảng Đối chiếu Vai trò Các Thành phần

| Thành phần | Công nghệ | Vai trò trong Hệ thống |
| :--- | :--- | :--- |
| **Object Storage** | MinIO | Lưu trữ đối tượng tương thích chuẩn S3, chứa raw files và data files của Iceberg |
| **File Format** | Apache Parquet | Định dạng file lưu trữ dạng cột nén cao (Snappy/ZSTD), hỗ trợ pushdown predicate |
| **Table Format** | Apache Iceberg | Quản lý bảng dữ liệu, ACID transactions, schema evolution, hidden partitioning và time travel |
| **Distributed Engine** | Apache Spark 4.1 | Quản lý phân tán, phân chia Task/Stage, lập lịch và tối ưu hóa Logical Plan |
| **Accelerator Plugin** | Apache DataFusion Comet | Chặn (intercept) Physical Plan, chuyển đổi sang DataFusion Plan và gọi Native Engine |
| **Native Query Engine** | Apache DataFusion (Rust) | Thực thi vectorized code trực tiếp trên CPU, quản lý bộ nhớ native bằng Apache Arrow |

### 3.3. Cơ chế Intercept và Fallback của Comet

Comet chèn các rule tối ưu hóa vào giai đoạn lập kế hoạch vật lý (Physical Planning) của Spark:

```
                  Spark SQL / DataFrame
                           │
                           ▼
                  Catalyst Logical Plan
                           │
                           ▼
                  Spark Physical Plan
                           │
                           ▼
              Comet Optimization & Intercept
                           │
        ┌──────────────────┴──────────────────┐
        │                                     │
 [Tất cả Operator hỗ trợ]           [Có Operator chưa hỗ trợ]
        │                                     │
        ▼                                     ▼
Toàn bộ Plan chạy Native            Tách thành các Subtree
  (Rust + Arrow Memory)                       │
        │                                     ├──────────────────────────┐
        │                                     ▼                          ▼
        │                              Native Subtree             Spark JVM Subtree
        │                           (DataFusion Execute)        (Catalyst Java Execute)
        │                                     │                          │
        │                                     └──────────┬───────────────┘
        │                                                │
        │                                      [Chuyển đổi Arrow ↔ JVM]
        │                                       (Chi phí phát sinh / overhead)
        │                                                │
        └──────────────────┬─────────────────────────────┘
                           ▼
                      Final Result
```

* **Fully Native**: Toàn bộ pipeline từ Scan, Filter, Project, HashJoin, Aggregate, Shuffle đến Write đều do DataFusion đảm nhiệm. Không tốn chi phí chuyển đổi dữ liệu qua lại giữa JVM và native memory.
* **Partial Fallback**: Một số biểu thức phức tạp hoặc hàm UDF chưa được Comet hỗ trợ sẽ được thực thi trên Spark JVM. Lúc này dữ liệu phải chuyển đổi giữa định dạng Arrow Columnar và Spark UnsafeRow.

---

## 4. Thiết kế Data Pipeline (Medallion Architecture)

Dự án xây dựng pipeline theo kiến trúc Medallion 3 lớp chuẩn:

### 4.1. Bronze Layer (Raw Data Ingestion)
- **Mục tiêu**: Lưu giữ nguyên vẹn dữ liệu gốc từ nguồn, phục vụ khả năng tái lập (reproducibility) và audit.
- **Định dạng & Lưu trữ**: `s3a://lakehouse/bronze/{table_name}/` dưới định dạng Apache Parquet, phân vùng theo thời gian:
  ```
  s3a://lakehouse/bronze/orders/year=2026/month=08/day=18/*.parquet
  s3a://lakehouse/bronze/customers/*.parquet
  s3a://lakehouse/bronze/products/*.parquet
  s3a://lakehouse/bronze/order_items/*.parquet
  s3a://lakehouse/bronze/events/year=2026/month=08/*.parquet
  ```

### 4.2. Silver Layer (Cleaned & Enriched Iceberg Tables)
- **Mục tiêu**: Chuẩn hóa kiểu dữ liệu, loại bỏ dữ liệu trùng lặp (deduplication), làm giàu dữ liệu qua các phép JOIN nhiều bảng và ghi vào bảng Apache Iceberg.
- **Đặc tả Schema**:
  - `silver_orders`: Kết hợp `orders` + `customers` + `order_items` + `products`.
  - `silver_events`: Chuẩn hóa nhật ký hành vi người dùng (clickstream).
- **Ví dụ Query Xử lý Silver**:
  ```sql
  CREATE OR REPLACE TABLE lakehouse_catalog.silver.sales_enriched
  USING iceberg
  PARTITIONED BY (days(order_time), region)
  AS
  SELECT
      o.order_id,
      o.customer_id,
      c.customer_name,
      c.region,
      c.segment,
      o.order_time,
      oi.product_id,
      p.product_name,
      p.category,
      oi.quantity,
      oi.unit_price,
      oi.discount,
      (oi.quantity * oi.unit_price * (1 - oi.discount)) AS net_revenue
  FROM lakehouse_catalog.bronze.orders o
  JOIN lakehouse_catalog.bronze.customers c
      ON o.customer_id = c.customer_id
  JOIN lakehouse_catalog.bronze.order_items oi
      ON o.order_id = oi.order_id
  JOIN lakehouse_catalog.bronze.products p
      ON oi.product_id = p.product_id
  WHERE o.status = 'COMPLETED';
  ```

### 4.3. Gold Layer (Analytics Data Marts & Aggregations)
- **Mục tiêu**: Tạo các bảng tổng hợp nghiệp vụ, phục vụ truy vấn phân tích hiệu năng cao, kiểm thử các phép tính Aggregation, Window Function và Complex Filtering.
- **Các Bảng Data Mart Chính**:
  1. `gold_daily_revenue`: Doanh thu và số lượng đơn hàng theo ngày và khu vực.
  2. `gold_customer_ltv`: Giá trị vòng đời khách hàng (Customer Lifetime Value), phân khúc tần suất mua sắm.
  3. `gold_product_ranking`: Xếp hạng sản phẩm bán chạy nhất trong từng danh mục sử dụng Window Function (`ROW_NUMBER()`, `DENSE_RANK()`).
  4. `gold_category_growth`: Tỉ lệ tăng trưởng doanh thu theo tháng của từng nhóm hàng (`LAG()`, `LEAD()`).

---

## 5. Thiết kế Workload & Bộ Dữ liệu Thử nghiệm (Benchmark Suite)

Để đánh giá toàn diện năng lực của DataFusion Comet, nghiên cứu thiết kế **3 tầng workload**:

### 5.1. Tầng 1: Micro-benchmarks (M01 – M10)
Mục tiêu là cô lập từng operator riêng biệt để xác định điểm mạnh/yếu của engine:

| Mã | Operator Trọng tâm | Mô tả Query Thử nghiệm |
| :--- | :--- | :--- |
| **M01** | `Scan` | Đọc toàn bộ bảng Parquet dung lượng lớn với nhiều kiểu dữ liệu khác nhau |
| **M02** | `Filter` | Quét bảng kèm điều kiện lọc đơn giản và phức tạp (Selectivity: 1%, 10%, 50%) |
| **M03** | `Projection` | Tính toán biểu thức số học, chuỗi ký tự trên nhiều cột đồng thời |
| **M04** | `Hash Join (1:N)` | Thực hiện phép Broadcast Hash Join và Shuffled Hash Join giữa bảng Fact và Dimension |
| **M05** | `Aggregation (Low Card)` | `GROUP BY` trên cột có độ đa dạng giá trị thấp (Low Cardinality, ví dụ: 5 - 20 groups) |
| **M06** | `Aggregation (High Card)`| `GROUP BY` trên cột có độ đa dạng giá trị cao (High Cardinality, ví dụ: $10^6$ groups) |
| **M07** | `Sort` | `ORDER BY` trên nhiều cột (kết hợp cả số và chuỗi ký tự) có giới hạn `LIMIT` |
| **M08** | `Window Function` | Các phép tính cửa sổ phân tích: `ROW_NUMBER()`, `RANK()`, `SUM() OVER(PARTITION BY ...)` |
| **M09** | `Join + Aggregation` | Kết hợp Hash Join nhiều bảng và thực hiện Aggregation đa cấp độ |
| **M10** | `Shuffle Exchange` | Phép toán gây tải mạng và I/O lớn (`DISTRIBUTE BY`, `REPARTITION`, `CROSS JOIN`) |

### 5.2. Tầng 2: Business Workload (B01 – B10)
Các câu truy vấn phân tích nghiệp vụ thực tế trên bộ dữ liệu E-Commerce:

* **B01 (Daily Regional Revenue)**: Tổng hợp doanh thu, chiết khấu, số đơn hàng theo ngày và vùng miền.
* **B02 (Customer Lifetime Value - LTV)**: Tính tổng chi tiêu, trung bình đơn hàng, ngày mua gần nhất của từng khách hàng.
* **B03 (Category Top Sellers)**: Xếp hạng top 10 sản phẩm đạt doanh thu cao nhất theo từng danh mục.
* **B04 (Conversion Funnel)**: Tỉ lệ chuyển đổi từ sự kiện `view_product` $\to$ `add_to_cart` $\to$ `checkout` $\to$ `purchase`.
* **B05 (Customer RFM Segmentation)**: Phân khúc khách hàng dựa trên Recency, Frequency, Monetary.
* **B06 (Monthly Growth Rate)**: Tỉ lệ tăng trưởng doanh thu theo tháng sử dụng hàm `LAG()`.
* **B07 (Discount Impact Analysis)**: Phân tích độ co giãn của sản lượng bán theo mức chiết khấu.
* **B08 (Repeat Purchase Cohort)**: Phân tích tỷ lệ mua lại của các nhóm khách hàng theo tháng tham gia.
* **B09 (Product Cross-selling Matrix)**: Xác định các cặp sản phẩm thường được mua cùng nhau trong một đơn hàng.
* **B10 (Rolling 7-Day Average Revenue)**: Tính doanh thu trung bình động 7 ngày liên tiếp.

### 5.3. Tầng 3: Standard Benchmark (TPC-H)
Sử dụng bộ benchmark chuẩn công nghiệp TPC-H để đảm bảo tính khách quan và khả năng so sánh với các nghiên cứu khác trên thế giới:
* **Scale Factors (SF)**:
  - **SF1** (~1 GB): Kiểm thử tính đúng đắn và baseline latency.
  - **SF10** (~10 GB): Workload tiêu chuẩn cho môi trường single-node / local cluster.
  - **SF50** (~50 GB): Workload kiểm thử áp lực bộ nhớ và GC.
  - **SF100** (~100 GB): Workload kiểm thử tối đa với swap/spill (nếu tài nguyên phần cứng cho phép).
* **TPC-H Query Selection**: Lựa chọn tối thiểu 10 queries đại diện: `Q01` (Scan/Agg), `Q03` (Join/Agg/Sort), `Q05` (Multi-way Join/Group), `Q06` (Scan/Filter), `Q09` (Complex Multi-Join), `Q10` (Join/Agg/Window), `Q12` (Join/Filter/Case), `Q14` (Case/Agg), `Q18` (Large Join/Group), `Q19` (Complex Filter Disjunction).

---

## 6. Phương pháp Đo lường & Quy trình Thực nghiệm (Benchmarking Methodology)

### 6.1. Nguyên tắc Kiểm soát Môi trường (Controlled Environment)
Để kết quả đo đạc mang giá trị khoa học, mọi thực nghiệm so sánh giữa **Spark Baseline (JVM)** và **Comet Accelerated (Rust Native)** phải tuân thủ nguyên tắc cách ly hoàn toàn:
1. **Phần cứng Cố định**: Chạy trên cùng một máy chủ vật lý / VM, cố định số CPU cores, RAM và I/O storage.
2. **Cấu hình Spark Giống hệt nhau**:
   - `spark.executor.memory`, `spark.driver.memory`, `spark.executor.cores`, `spark.sql.shuffle.partitions`.
   - Điểm khác biệt duy nhất giữa 2 pipeline là:
     - Baseline: `spark.plugins = ""` (hoặc không kích hoạt Comet).
     - Experimental: `spark.plugins = org.apache.spark.CometPlugin` cùng cấu hình `spark.comet.enabled = true`.
3. **Cơ sở Dữ liệu & Phân vùng Đồng nhất**: Đọc cùng một tập tin Parquet / Iceberg trên MinIO.

### 6.2. Quy trình Thực hiện Đo đạc (Warm-up & Repetition)
Mỗi truy vấn trong bộ benchmark được thực thi theo chu trình:

```
[Khởi động Spark Session]
        │
        ▼
[Warm-up Run 1] ─── (Loại bỏ chi phí khởi tạo JIT, JVM Class Loading, OS Page Cache)
        │
        ▼
[Warm-up Run 2] ─── (Đưa Cache và Memory state về trạng thái ổn định)
        │
        ▼
[Measurement Runs: Lặp lại 5 lần độc lập]
        ├── Run 1 ──> Thu thập: Time, CPU, RAM Peak, Shuffle IO, Fallback Log
        ├── Run 2 ──> Thu thập: Time, CPU, RAM Peak, Shuffle IO, Fallback Log
        ├── Run 3 ──> Thu thập: Time, CPU, RAM Peak, Shuffle IO, Fallback Log
        ├── Run 4 ──> Thu thập: Time, CPU, RAM Peak, Shuffle IO, Fallback Log
        └── Run 5 ──> Thu thập: Time, CPU, RAM Peak, Shuffle IO, Fallback Log
        │
        ▼
[Tính toán Thống kê] ───> Min, Mean, Median (Giá trị đại diện), P95, Std Dev
```

### 6.3. Hệ thống Chỉ số Đo lường (Metrics System)

| Nhóm Chỉ số | Tên Metric | Đơn vị | Phương thức Thu thập |
| :--- | :--- | :--- | :--- |
| **Hiệu năng Thời gian** | `Execution Time (Elapsed)` | Milliseconds (ms) | `SparkListenerJobEnd` / Timer |
| | `Median Latency` | ms | Trung vị của 5 lượt đo |
| | `P95 Latency` | ms | Phân vị 95% của các lượt đo |
| **Tài nguyên CPU & Bộ nhớ** | `CPU Average / Peak Usage` | % | OS Process Sampling / cgroups |
| | `Memory Peak (Resident)` | Megabytes (MB) | JVM Runtime Memory + Native Memory RSS |
| | `JVM GC Time` | ms | JVM GarbageCollectorMXBean |
| **I/O & Mạng** | `Shuffle Read / Write Size` | MB | Spark Task Metrics |
| | `Disk Spill (Memory / Disk)`| MB | Spark Stage Spill Metrics |
| | `Scan Throughput` | MB/s | (Input Size / Scan Time) |
| **Độ bao phủ Native** | `Native Operator Ratio` | % | Comet Extended Explain Parser |
| | `Fallback Count` | Lần | Đếm các operator bị fallback về JVM |

### 6.4. Công thức Tính toán

* **Tỉ lệ Tăng tốc (Speedup Factor)**:
  $$Speedup = \frac{T_{Spark}}{T_{Comet}}$$

* **Tỉ lệ Cải thiện Hiệu năng (% Improvement)**:
  $$Improvement (\%) = \left(\frac{T_{Spark} - T_{Comet}}{T_{Spark}}\right) \times 100\%$$

* **Tỉ lệ Tiết kiệm Tài nguyên CPU / GC**:
  $$Resource\_Saved (\%) = \left(\frac{Metric_{Spark} - Metric_{Comet}}{Metric_{Spark}}\right) \times 100\%$$

* **Độ bao phủ Native (Native Coverage)**:
  $$Native\_Coverage = \frac{N_{native\_operators}}{N_{total\_physical\_operators}} \times 100\%$$

---

## 7. Quản lý Bộ nhớ & Tinh chỉnh Hiệu năng (Memory Tuning & Stability)

Khi tích hợp DataFusion Comet vào Spark, bộ nhớ được chia thành **JVM On-Heap Memory** (dành cho Spark Catalyst & Fallback operators) và **Native Off-Heap Memory** (dành cho DataFusion Execution & Arrow Columnar Allocation).

### 7.1. Cấu hình Phân bổ Bộ nhớ Khuyến nghị

```properties
# Kích hoạt Comet Plugin
spark.plugins=org.apache.spark.CometPlugin
spark.comet.enabled=true
spark.comet.exec.enabled=true
spark.comet.exec.all.enabled=true

# Phân bổ Memory Overhead cho Native Rust / Arrow (Tối thiểu 30% - 40% Executor Memory)
spark.executor.memory=4g
spark.driver.memory=4g
spark.executor.memoryOverhead=2g
spark.comet.memoryOverhead=2g

# Cấu hình Native Columnar Shuffle
spark.comet.columnar.shuffle.enabled=true
spark.comet.columnar.shuffle.async.enabled=true

# Debug & Trích xuất Extended Plan
spark.comet.explainVerbose.enabled=true
```

### 7.2. Phòng tránh Lỗi Thường gặp
- **JVM Crash (`hs_err_pid.log`)**: Thường xảy ra khi Native memory vượt quá giới hạn cgroups hoặc container RAM. Cần đảm bảo `spark.executor.memoryOverhead` đủ lớn.
- **OOM do Data Skew**: Khi shuffle các key bị lệch, cần bật Adaptive Query Execution (`spark.sql.adaptive.enabled=true`).

---

## 8. Công cụ Tự động hóa & Phân tích (Benchmark Automation & Tooling)

Để đảm bảo toàn bộ quá trình thực nghiệm có tính **tự động, lặp lại được (reproducible) và không có sai số do thao tác thủ công**, dự án xây dựng 4 module công cụ chuyên biệt:

### 8.1. Benchmark Runner
Module Python/Shell tự động đọc cấu hình thử nghiệm từ file YAML, khởi chạy Spark application, kích hoạt cấu hình tương ứng, quản lý warm-up và thu thập log kết quả.

**Ví dụ `experiment_config.yaml`**:
```yaml
experiment:
  id: "EXP-COMM-TPCH-SF10-Q05"
  name: "TPC-H Q05 SF10 Comparison"
  description: "Comparing Spark JVM vs DataFusion Comet on TPC-H Query 5 at Scale Factor 10"
  iterations: 5
  warmup_runs: 2

workload:
  suite: "tpch"
  scale_factor: 10
  query_id: "Q05"
  data_path: "s3a://lakehouse/tpch/sf10/"

spark:
  master: "local[*]"
  driver_memory: "4g"
  executor_memory: "8g"
  executor_cores: 4
  sql_shuffle_partitions: 16

matrix:
  engines:
    - name: "spark_baseline"
      enabled_comet: false
      spark_conf:
        "spark.comet.enabled": "false"
    - name: "comet_accelerated"
      enabled_comet: true
      spark_conf:
        "spark.comet.enabled": "true"
        "spark.comet.exec.enabled": "true"
        "spark.comet.exec.all.enabled": "true"
        "spark.comet.columnar.shuffle.enabled": "true"
```

### 8.2. Plan Analyzer
Module tự động trích xuất kế hoạch thực thi mở rộng (`EXPLAIN EXTENDED` hoặc `EXPLAIN FORMATTED`) từ Spark SQL, phân tích cây thực thi để bóc tách:
1. Các nút thực thi Native (`CometScanExec`, `CometFilterExec`, `CometProjectExec`, `CometHashJoinExec`, `CometHashAggregateExec`).
2. Các nút thực thi JVM Fallback (`FileScan`, `Filter`, `Project`, `SortMergeJoin`, `ObjectHashAggregate`).
3. Các điểm chuyển đổi dữ liệu (`CometSparkToColumnarExec`, `CometColumnarToRowExec`).

### 8.3. Result Store & Cấu trúc Dữ liệu Kết quả
Mỗi lần chạy sẽ xuất một bản ghi JSON có cấu trúc chuẩn vào thư mục `results/raw/`:

```json
{
  "experiment_id": "EXP-COMM-TPCH-SF10-Q05",
  "timestamp": "2026-08-22T09:00:00Z",
  "engine": "comet_accelerated",
  "workload": "tpch",
  "scale_factor": 10,
  "query_id": "Q05",
  "hardware": {
    "cpu_model": "AMD Ryzen 7 / Intel Core i7",
    "allocated_cores": 4,
    "allocated_ram_gb": 8
  },
  "metrics": {
    "execution_time_ms_runs": [14200, 13850, 13910, 14050, 13800],
    "execution_time_median_ms": 13850,
    "execution_time_p95_ms": 14170,
    "cpu_peak_percent": 84.5,
    "memory_peak_mb": 4210,
    "jvm_gc_time_ms": 420,
    "shuffle_read_mb": 450.2,
    "shuffle_write_mb": 450.2,
    "disk_spill_mb": 0
  },
  "plan_analysis": {
    "total_operators": 14,
    "comet_native_operators": 13,
    "spark_fallback_operators": 1,
    "native_coverage_ratio": 0.928,
    "fallback_reasons": ["Unsupported custom UDF in projection"]
  }
}
```

### 8.4. Research Notebooks (Phân tích & Biểu đồ)
Tập hợp các Jupyter Notebook chuyên biệt trong thư mục `analysis/notebooks/` để xử lý dữ liệu và vẽ biểu đồ khoa học:
- `01_data_generator_validation.ipynb`: Kiểm tra phân phối dữ liệu, tính toàn vẹn của Bronze/Silver/Gold.
- `02_micro_benchmark_analysis.ipynb`: Phân tích speedup trên từng operator riêng rẽ (M01 – M10).
- `03_business_workload_analysis.ipynb`: Đánh giá hiệu quả trên các bài toán nghiệp vụ E-Commerce (B01 – B10).
- `04_tpch_scalability_analysis.ipynb`: Phân tích khả năng mở rộng trên TPC-H từ SF1 $\to$ SF100.
- `05_plan_fallback_deepdive.ipynb`: Mổ xẻ chi tiết các trường hợp query bị nghẽn do Arrow-JVM fallback.
- `06_final_synthesis_report.ipynb`: Tổng hợp toàn bộ ma trận kết quả, xuất bảng số liệu và đồ thị phục vụ báo cáo.

---

## 9. Cấu trúc Thư mục Dự án Chuẩn (Repository Layout)

Cấu trúc repository được thiết kế chuyên nghiệp, module hóa rõ ràng:

```
high-perf-lakehouse-comet/
├── README.md                           # Giới thiệu đề tài, hướng dẫn cài đặt & chạy nhanh
├── LICENSE                             # Giấy phép mã nguồn mở (Apache 2.0)
├── Makefile                            # Các lệnh tắt: make setup, make bench, make report
├── docker-compose.yml                  # Môi trường chạy MinIO, Spark Cluster trên Linux
├── .env.example                        # Mẫu biến môi trường (MinIO credentials, Spark paths)
│
├── docs/                               # Tài liệu thiết kế chi tiết
│   ├── project_specification.md        # Toàn văn bản đặc tả kỹ thuật & thực nghiệm đề tài
│   ├── architecture.md                 # Đặc tả kiến trúc hệ thống và luồng dữ liệu
│   ├── research-questions.md           # Chi tiết các câu hỏi và giả thuyết nghiên cứu
│   ├── benchmark-methodology.md        # Hướng dẫn phương pháp đo và kiểm soát môi trường
│   └── comet-configuration-guide.md    # Hướng dẫn tinh chỉnh các tham số Comet
│
├── infrastructure/                     # Cấu hình hạ tầng
│   ├── docker/                         # Dockerfiles cho Spark + Comet pre-built runtime
│   ├── minio/                          # Script khởi tạo bucket S3 (lakehouse/bronze, silver, gold)
│   └── spark/                          # Cấu hình spark-defaults.conf (Comet configs, Iceberg catalog)
│
├── data/                               # Quản lý dữ liệu và sinh dữ liệu
│   ├── generator/                      # Code Python/PySpark sinh dữ liệu E-Commerce giả lập
│   ├── tpch/                           # Script sinh dữ liệu TPC-H (dbgen) theo SF1/SF10/SF50
│   └── schemas/                        # Định nghĩa schema JSON/Avro cho các bảng
│
├── pipeline/                           # Mã nguồn Lakehouse Data Pipeline
│   ├── bronze/                         # Ingestion scripts (Source -> MinIO Parquet)
│   ├── silver/                         # Cleaning & Iceberg Merging scripts
│   └── gold/                           # Business Aggregation & Data Marts scripts
│
├── workloads/                          # Tập hợp các SQL query thử nghiệm
│   ├── micro/                          # M01_scan.sql -> M10_shuffle.sql
│   ├── business/                       # B01_revenue.sql -> B10_rolling_avg.sql
│   └── tpch/                           # Q01.sql -> Q22.sql
│
├── benchmark/                          # Hệ thống tự động hóa Benchmark
│   ├── runner/                         # Benchmark Runner engine (Python)
│   ├── collectors/                     # Trình thu thập CPU, RAM, GC, Spark Event Logs
│   ├── parsers/                        # Parser phân tích Spark execution plan & metrics
│   └── configs/                        # Các file cấu hình YAML cho từng đợt thử nghiệm
│
├── analysis/                           # Phân tích kết quả & Trực quan hóa
│   ├── notebooks/                      # 01 -> 06 Research Jupyter Notebooks
│   ├── scripts/                        # Script tự động vẽ đồ thị từ kết quả tổng hợp
│   └── plots/                          # Hình ảnh đồ thị xuất bản (PNG/SVG)
│
├── results/                            # Lưu trữ dữ liệu thực nghiệm
│   ├── raw/                            # Log JSON thô từng lượt chạy của Spark & Comet
│   ├── normalized/                     # Dữ liệu kết quả đã qua làm sạch và gom nhóm
│   └── reports/                        # Báo cáo tổng hợp số liệu markdown/csv
│
├── tests/                              # Unit test & Data validation test
│   ├── test_data_integrity.py          # Kiểm tra tính đúng đắn của dữ liệu giữa Spark & Comet
│   └── test_pipeline.py                # Kiểm thử pipeline hoạt động trơn tru
│
└── scripts/                            # Shell scripts tiện ích
    ├── setup_env.sh                    # Cài đặt môi trường, tải jar dependencies
    ├── run_pipeline.sh                 # Thực thi toàn bộ pipeline Medallion
    └── run_all_benchmarks.sh           # Kích hoạt toàn bộ bộ benchmark
```

---

## 10. Kế hoạch Triển khai 8 Tuần (8-Week Roadmap)

| Tuần | Trọng tâm Công việc | Chi tiết Nhiệm vụ & Deliverables |
| :---: | :--- | :--- |
| **Tuần 1** | **Nền tảng Spark & Columnar Execution** | - Nghiên cứu cơ chế Catalyst Optimizer, Physical Planning, UnsafeRow và Vectorized Execution trong Spark.<br>- Setup môi trường Linux / WSL2, MinIO và Spark 4.1.x.<br>- Viết tài liệu tổng quan kiến trúc. |
| **Tuần 2** | **Apache DataFusion, Arrow & Comet Internals** | - Nghiên cứu Apache Arrow memory layout và kiến trúc execution engine DataFusion (Rust).<br>- Tích hợp Comet JAR vào Spark, chạy thử nghiệm Hello World với `spark.comet.enabled=true`.<br>- Phân tích cơ chế Intercept và Fallback qua `EXPLAIN EXTENDED`. |
| **Tuần 3** | **Xây dựng Data Pipeline Medallion (Iceberg + MinIO)** | - Thiết lập Apache Iceberg REST Catalog kết nối MinIO.<br>- Xây dựng pipeline Bronze $\to$ Silver $\to$ Gold cho tập dữ liệu E-Commerce.<br>- Hoàn thiện các query nghiệp vụ từ B01 đến B10. |
| **Tuần 4** | **Chuẩn bị Dữ liệu & Xây dựng Bộ Sinh Dữ liệu** | - Viết module sinh dữ liệu E-Commerce có kiểm soát (cardinality, skewness).<br>- Sinh tập dữ liệu TPC-H ở các kích thước SF1, SF10, SF50.<br>- Kiểm tra tính đúng đắn dữ liệu (Data consistency check). |
| **Tuần 5** | **Xây dựng Công cụ Tự động hóa Benchmark** | - Phát triển `Benchmark Runner` tự động đọc cấu hình YAML.<br>- Xây dựng module `Plan Analyzer` phân tích tỉ lệ Native Coverage.<br>- Xây dựng bộ đo đạc tài nguyên (CPU, RAM, JVM GC, Shuffle I/O). |
| **Tuần 6** | **Thực nghiệm Benchmark Toàn diện** | - Chạy benchmark Micro-benchmark (M01 – M10) trên cả Spark JVM và Comet.<br>- Chạy benchmark Business Workload (B01 – B10).<br>- Chạy benchmark TPC-H SF1, SF10, SF50 theo đúng quy trình 2 warm-up + 5 runs.<br>- Thu thập toàn bộ log và kết quả JSON vào `results/raw/`. |
| **Tuần 7** | **Phân tích Sâu Dữ liệu Thực nghiệm (Deep Dive)** | - Phân tích chi tiết Speedup, CPU saving, GC reduction và Shuffle efficiency.<br>- Nghiên cứu các case study bị nghẽn (bottleneck) do Fallback và chuyển đổi Arrow-JVM.<br>- Xây dựng toàn bộ biểu đồ khoa học trên Jupyter Notebooks. |
| **Tuần 8** | **Hoàn thiện Báo cáo, Đóng gói & Trình bày** | - Tổng hợp kết quả, trả lời đầy đủ 3 câu hỏi nghiên cứu (RQ1, RQ2, RQ3).<br>- Hoàn thiện tài liệu kỹ thuật cuối cùng và slide thuyết trình.<br>- Đóng gói mã nguồn, chuẩn bị video demo thực tế. |

---

## 11. Tiêu chí Đánh giá & Sản phẩm Đầu ra (Deliverables)

Khi hoàn thành đề tài, các sản phẩm bàn giao bắt buộc phải đạt được:

1. **Mã nguồn Hoàn chỉnh (Clean & Reproducible Repository)**:
   - Toàn bộ source code pipeline Medallion, benchmark automation runner, và kịch bản phân tích có thể tái lập 100% bằng 1 dòng lệnh `make run-all`.
2. **Bộ Dữ liệu Thực nghiệm Đầy đủ (Empirical Dataset)**:
   - Toàn bộ kết quả đo lường thô (JSON/CSV) của hàng trăm lần chạy thử nghiệm có lưu vết thời gian, cấu hình phần cứng, execution plan logs.
3. **Báo cáo Phân tích Kỹ thuật Chuyên sâu (Comprehensive Research Report)**:
   - Báo cáo định lượng cụ thể:
     - Biểu đồ so sánh thời gian thực thi (Latency comparison bar charts).
     - Biểu đồ phân tích tài nguyên (CPU & Memory profiles over time).
     - Ma trận Native Operator Coverage và phân tích nguyên nhân Fallback.
   - Kết luận rõ ràng cho 3 câu hỏi nghiên cứu RQ1, RQ2, RQ3 dựa trên số liệu thực tế.
4. **Slide Thuyết trình & Video Demo Trực quan**:
   - Slide báo cáo chuẩn mực dành cho hội đồng chuyên môn / mentor.
   - Video demo so sánh trực tiếp Spark Web UI giữa Spark thuần và Spark + Comet.
