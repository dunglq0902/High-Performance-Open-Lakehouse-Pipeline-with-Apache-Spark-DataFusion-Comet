# Đánh giá readiness trên laptop — 2026-08-26

> **Historical snapshot:** tài liệu này giữ nguyên đánh giá tại ngày 26/08. Nhiều khoảng trống nêu
> dưới đây đã được triển khai sau đó; trạng thái hiện hành nằm tại
> [`implementation-status.md`](implementation-status.md).

## Kết luận

Dự án **đã sẵn sàng cho phát triển và native correctness smoke** trên laptop hiện tại. Dự án
**chưa sẵn sàng để sinh benchmark nghiên cứu chính thức** vì profile `benchmark-laptop`, tập dữ
liệu SF1, workload core và pipeline thu thập/phân tích metric chưa được triển khai đầy đủ.

Phương thức vận hành đã chốt là **chạy thủ công theo từng batch**. Hệ thống không phải dịch vụ
real-time và không được giữ Spark, MinIO, Iceberg REST chạy liên tục.

## Bằng chứng chạy thật

Lệnh `make smoke` đã hoàn thành với exit code 0 trên Docker Desktop + Ubuntu/WSL2:

- Ruff lint và format check đạt.
- Mypy đạt trên 38 source files.
- Pytest đạt 65/65 tests.
- Docker Compose build và health checks đạt cho MinIO, Iceberg REST, Spark master và worker.
- Spark baseline và Comet dùng cùng Iceberg snapshot, cho cùng schema, row count và canonical
  result hash.
- Comet 1.0.0 native library được nạp; final plan M02 có 6/6 operator native, native Iceberg scan,
  không fallback và không có node chưa phân loại.
- Stack được tự động dừng sau khi chạy; không còn container dự án chạy nền.

Artifact kiểm chứng mới nhất:
`.artifacts/smoke/smoke-20260826T152147Z-5944/verification.json`.

Smoke dùng fixture 96 orders và có nhãn `readiness-smoke-not-benchmark-data`. Thời gian đo trong
artifact smoke không được dùng làm kết quả hiệu năng nghiên cứu.

## Phần cứng và tài nguyên quan sát được

| Thành phần | Giá trị quan sát | Đánh giá |
|---|---:|---|
| Laptop | Dell Precision 5550 | Phù hợp nghiên cứu single-node giới hạn |
| CPU | Intel Core i7-10750H, 6 cores / 12 threads, AVX2 | Đủ; Spark chỉ dùng 2 executor cores |
| RAM vật lý | 15.72 GiB | Đủ cho smoke và có điều kiện cho SF1 |
| RAM Windows khả dụng khi kiểm tra | 2.29 GiB; khoảng 85.4% đang dùng | Rủi ro cao cho benchmark nếu vẫn mở nhiều tác vụ |
| RAM thấy bởi Docker/WSL | Khoảng 7.61 GiB | Phù hợp envelope hiện tại; không nên tăng thêm |
| WSL swap sau smoke | 72 KiB / 2 GiB | Không có paging đáng kể trong smoke |
| Dung lượng trống ổ D | 467.6 GiB | Không phải nút thắt hiện tại |

Envelope Spark đã giảm còn một worker, một executor, 2 cores, executor heap 2 GiB, memory overhead
1 GiB, off-heap 1 GiB và driver heap 1 GiB. Docker worker công bố tối đa 5 GiB để đủ nhận executor
này.

## Mức độ phù hợp theo loại chạy

| Loại chạy | Trạng thái | Điều kiện |
|---|---|---|
| Unit/static validation | Đủ và ổn định | Có thể chạy khi đang làm việc bình thường |
| Native smoke M02 | Đã chứng minh chạy đúng | Chạy thủ công; script tự dọn stack |
| Benchmark SF1 | Có khả năng phù hợp nhưng chưa được chứng minh | Phải triển khai runner/data/workload còn thiếu và chạy trong cửa sổ tài nguyên sạch |
| Benchmark SF10 | Chỉ là mở rộng tùy chọn, chưa xác nhận capacity | Chỉ thử sau khi SF1 ổn định; được phép bỏ khỏi báo cáo |
| Chạy liên tục hoặc scale-out | Ngoài phạm vi | Không triển khai trên laptop này |

## Rủi ro và phần còn thiếu

### Mức cao — chặn benchmark chính thức

- `benchmark-laptop` mới là contract/profile dự kiến; `make run-all` hiện chỉ chấp nhận
  `smoke-local` và sẽ fail closed với profile nghiên cứu.
- Fixture smoke không phải dữ liệu SF1. Chưa có campaign SF1 hoàn chỉnh qua correctness, capacity
  và stability gates.
- Mới có vertical slice M02. Tập workload core Micro/Business và bốn truy vấn TPC-H-derived chưa
  có đủ SQL, manifest, golden/correctness evidence và orchestration.
- Resource sampling, Spark event-log metric attribution, resume/timeout campaign và bootstrap
  confidence interval chưa được triển khai.
- Mức RAM Windows hiện tại quá sát. Chạy benchmark cùng nhiều tab web/tác vụ khác có thể tạo
  paging, thermal throttling hoặc contention, làm kết quả không hợp lệ dù job vẫn hoàn thành.

### Mức vừa

- Native integration mới được chứng minh cục bộ trên Docker Desktop/WSL2, chưa có Linux CI runner
  chuyên dụng.
- Image nghiên cứu hiện khóa cho `x86_64`; đây là chủ ý phù hợp laptop hiện tại, không phải image
  đa kiến trúc.
- Build đầu tiên phụ thuộc mạng và có thể lâu. PyArrow đã được checksum-pin và tách cache để giảm
  timeout; các lần build sau dùng cache.
- Docker Desktop/WSL đôi khi có lỗi CLI tạm thời ngay sau khi khởi động. Health probe đã được đổi
  sang retry và phần xử lý log dùng interpreter tuyệt đối để tránh abort giả.

## Cấu hình vận hành khuyến nghị

- Giữ chế độ manual batch; sau mỗi campaign dùng `docker compose down` hoặc để smoke tự cleanup.
- Giữ Spark ở 2 executor cores và envelope bộ nhớ hiện tại. Không tăng executor/driver memory trên
  laptop 16 GiB này.
- Giữ Docker/WSL quanh mức 7.5–8 GiB; không tăng vì sẽ ép Windows, và chưa giảm trước khi có capacity
  test xác nhận.
- Với smoke phát triển, có thể giữ các tác vụ nhẹ. Với measurement chính thức, đóng browser/tab
  nặng, IDE phụ và tiến trình nền; cắm nguồn, dùng chế độ hiệu năng ổn định và để máy nguội.
- Chạy SF1 trước với tối thiểu 10 paired measurements. Chỉ thử SF10 sau khi không có swap, OOM,
  throttling hoặc timeout ở SF1; SF10 không phải điều kiện hoàn thành dự án.
- Không công bố latency/speedup từ smoke. Chỉ artifact của campaign nghiên cứu vượt toàn bộ gate mới
  được đưa vào báo cáo.

## Quyết định cho kế hoạch một người / tám tuần

Phạm vi khả thi là: hoàn thiện runner `benchmark-laptop`, một tập workload core nhỏ có chất lượng,
SF1 là kết quả chính, correctness/native-coverage trước performance, và báo cáo giới hạn single-node.
Nếu giữ phạm vi này, phần cứng đủ để hoàn thành nghiên cứu mức vừa. Nếu yêu cầu full catalog,
campaign liên tục, scale-out hoặc bắt buộc SF10, kế hoạch và phần cứng hiện tại không đủ an toàn.
