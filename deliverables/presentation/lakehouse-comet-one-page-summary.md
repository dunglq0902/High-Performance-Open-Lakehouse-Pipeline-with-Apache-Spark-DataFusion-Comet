# TÓM TẮT ĐỀ TÀI

## Đường ống Open Lakehouse hiệu năng cao với Apache Spark và DataFusion Comet

### Bài toán và mục tiêu

Apache Spark là nền tảng xử lý dữ liệu phổ biến nhưng vẫn chịu chi phí của JVM, quản lý bộ nhớ và chuyển đổi dữ liệu. Đề tài xây dựng một Open Lakehouse dựa trên Apache Spark 4.1.3, DataFusion Comet 1.0.0, Apache Iceberg 1.11.0, Parquet và MinIO; sau đó đánh giá định lượng việc bật Comet trên cùng dữ liệu, câu lệnh SQL, cấu hình và giới hạn tài nguyên. Ba câu hỏi nghiên cứu là: Comet cải thiện độ trễ đến mức nào; operator nào chạy native hoặc fallback; và lợi ích thay đổi ra sao theo fallback và quy mô dữ liệu.

### Kiến trúc và phương pháp

Dữ liệu được tổ chức theo Bronze–Silver–Gold trên Iceberg. Spark thuần và Spark + Comet đọc cùng snapshot bất biến. Mỗi engine chạy trong một Spark application độc lập, cùng một worker 2 core và giới hạn cgroup 5 GiB. Ma trận chính gồm 10 workload: một business query, năm microbenchmark và bốn truy vấn TPC-H-derived ở SF1. Mỗi workload có 2 warm-up cho từng engine và 10 cặp đo Spark/Comet theo lịch AB/BA ngẫu nhiên có seed cố định, tương ứng 24 execution attempts; toàn bộ campaign ghi nhận 0 attempt lỗi và 0 measurement lỗi.

Kết quả được kiểm tra đúng đắn trước khi nhận mẫu. Mỗi run lưu wall time, Spark event log, physical plan, native/fallback operators và mẫu tài nguyên cgroups v2. Chỉ số chính là median của speedup theo từng cặp, `Spark wall time / Comet wall time`; khoảng tin cậy 95% được bootstrap 10.000 lần với seed 20260824. P95 không được công bố vì mỗi engine chỉ có 10 measurement.

### Kết quả ma trận chính (SF1 và e-commerce)

- Comet cải thiện median latency ở cả 10/10 workload; toàn bộ khoảng tin cậy 95% có cận dưới lớn hơn 1. Geometric mean của median paired speedup là **1,508×**.
- Q01 có mức tăng cao nhất: **3,509×**, CI 95% **[3,284; 3,990]**. M08 thấp nhất nhưng vẫn đạt **1,115×**, CI 95% **[1,069; 1,148]**.
- Median CPU core-seconds giảm ở 10/10 workload, từ **28,9% đến 99,2%**. Peak cgroup memory không cho thấy mức giảm ở median trong cả 10 workload; vì vậy đề tài không tuyên bố Comet giảm RAM đỉnh.
- Native coverage đạt 100% ở 9/10 workload. M08 chỉ đạt 40%, gồm 4 native operators, 6 fallback operators và 2 transitions; đây cũng là workload có speedup thấp nhất.
- Phân tích H2 chỉ mang tính mô tả: Spearman rho là +0,522 giữa native coverage và log-speedup, và −0,522 đối với fallback/transitions. Kết quả không có p-value và không chứng minh quan hệ nhân quả.

### Mở rộng SF10: vòng 2, 10 cặp mỗi truy vấn

| Truy vấn | Paired speedup | CI 95% |
|---|---:|---:|
| Q01 | **4,51×** | 4,15–4,79× |
| Q03 | **0,99×** | 0,94–1,03× |
| Q06 | **1,46×** | 1,36–1,57× |
| Q12 | **1,64×** | 1,53–1,68× |

80 lượt đo và 16 record correctness/plan đều thành công. Mỗi application đo có 2 warm-up không tính giờ. Cả 192 cửa sổ tài nguyên đầy đủ, swap bằng 0. Q03 chưa có khác biệt chắc chắn dù native coverage đạt 100%. Vòng 1 có 5 cặp được giữ riêng. Geometric mean 1,508× phía trên không gồm SF10.

### Đóng góp, giới hạn và kết luận

Đóng góp chính không chỉ là một con số speedup, mà là quy trình benchmark tái lập: khóa runtime và dữ liệu, ghép cặp công bằng, kiểm tra correctness, giữ nguyên raw evidence, phân tích plan và áp dụng publication gate. Kết quả cho thấy Comet có tiềm năng giảm đáng kể độ trễ và CPU trên cấu hình đã đo; đồng thời M08 minh họa rằng fallback có thể làm suy giảm lợi ích. Phạm vi kết luận chỉ áp dụng cho laptop single-worker, 2 core, 5 GiB, bộ workload và phiên bản runtime đã khóa. Các truy vấn TPC-H chỉ là TPC-H-derived, không phải kết quả TPC-H được kiểm toán. Đã có đối chiếu SF1–SF10 cho bốn truy vấn, nhưng hai bộ khác commit, image và warm-up nên chưa tách được tác động riêng của scale. SF10 vòng 2 trải qua hai phiên máy; ba lỗi launcher Q01 trước thực thi được lưu riêng. Hướng tiếp theo là đo hai scale trên cùng giao thức, tăng số cặp và mở rộng nhiều worker.

Gluten + Velox là một lựa chọn native cạnh tranh: Apache Gluten công bố 3,34× overall trên TPCH-like, nhưng môi trường 3 TB, Xeon 8592+ và Spark 3.3.1 khác đề tài nên không thể xếp hạng trực tiếp. Khi workload production khác nguồn công khai, cần A/B benchmark các engine trên cùng dữ liệu, tài nguyên, correctness gate, p95, chi phí và plan coverage.

Nguồn SF10 và kết luận cập nhật: [báo cáo nghiên cứu](../../docs/research-report.md). Nguồn ma trận chính: `results/reports/technical-report.md`, `results/reports/research-findings.json`, `results/reports/report-publishability.json`; [Apache Gluten public performance](https://github.com/apache/gluten-site/blob/main/index.md#5-performance).
