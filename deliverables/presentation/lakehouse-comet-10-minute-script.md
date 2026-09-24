# Lời thuyết trình 10 phút

> Bản cập nhật 24/09/2026 cho 15 slide trong `lakehouse-comet-research-sf10-20260924.pptx`. Các mốc dưới đây cộng lại đúng 10 phút. Số liệu ma trận chính và phần mở rộng SF10 được gọi tên riêng trong bài nói.

## Slide 1 — Đường ống Open Lakehouse hiệu năng cao (0:00–0:30)

Kính thưa thầy cô và các bạn. Đề tài đánh giá việc bật DataFusion Comet trên Apache Spark trong một pipeline Open Lakehouse dùng Iceberg. Em trình bày ma trận chính gồm 10 workload và phần mở rộng SF10 vừa hoàn tất. Mỗi workload chính và mỗi truy vấn SF10 vòng 2 đều có 10 cặp đo Spark–Comet. Mục tiêu là xác định lợi ích thực tế và giới hạn của từng kết luận.

## Slide 2 — Câu hỏi nghiên cứu và phạm vi (0:30–1:00)

RQ1 đo độ trễ và độ ổn định. RQ2 phân tích operator native và fallback. RQ3 xem xét quy mô dữ liệu và chi phí fallback. Ma trận chính gồm một business query, năm microbenchmark và bốn truy vấn TPC-H-derived SF1. Phần mở rộng chạy cùng bốn query ID ở SF10. Môi trường là laptop với một Spark worker, 2 core và giới hạn cgroup 5 GiB.

## Slide 3 — Cơ sở bằng chứng của ma trận chính (1:00–1:30)

Bảng này chỉ thuộc ma trận chính: 10 campaign, 200 measurement record và 240 record gồm cả warm-up, không có attempt lỗi. Phần SF10 mới có 80 measurement và 16 record correctness/plan, được trình bày riêng ở slide 12. Các lần lỗi launcher SF10 cũng được ghi riêng. Không cộng các bộ này vào cùng một chỉ số tổng hợp.

## Slide 4 — Kiến trúc đo lường (1:30–2:10)

Dữ liệu đi qua Bronze, Silver và Gold trên Iceberg. Trong từng campaign, Spark thuần và Spark với Comet đọc cùng snapshot, chạy ở application độc lập với tài nguyên đã khóa. Runner ghép cặp theo lịch AB/BA có seed cố định. Correctness là điều kiện bắt buộc trước khi nhận mẫu. Event log, physical plan và dữ liệu cgroups cho phép truy vết mỗi kết quả đến SQL, cấu hình, dataset manifest và commit cụ thể. SF10 còn thực hiện hai warm-up không tính giờ trong mỗi application đo.

## Slide 5 — Paired speedup của ma trận chính (2:10–3:00)

Chỉ số chính là trung vị tỷ số thời gian Spark chia cho thời gian Comet trong từng cặp. Lớn hơn 1 nghĩa là Comet nhanh hơn. Trong ma trận chính, cả 10 workload có trung vị và khoảng tin cậy 95% nằm trên 1. Geometric mean của 10 trung vị là 1,508 lần. Q01 ở SF1 cao nhất, 3,509 lần; M08 thấp nhất, 1,115 lần. Con số 1,508 không bao gồm SF10. Paired speedup giữ quan hệ giữa hai phép đo trong từng cặp và không nhất thiết bằng tỷ số của hai thời gian trung vị.

## Slide 6 — Median latency theo workload (3:00–3:35)

Đơn vị ở đây là giây. Q01 tại SF1 giảm từ khoảng 5,44 giây xuống 1,51 giây. B01 giảm từ khoảng 9,11 xuống 5,98 giây. M08 giảm ít hơn, từ khoảng 13,20 xuống 11,70 giây. Lợi ích vì thế thay đổi theo workload. Các cột là trung vị; báo cáo kỹ thuật giữ IQR, min, max và khoảng tin cậy để người đọc thấy độ phân tán.

## Slide 7 — CPU và bộ nhớ của ma trận chính (3:35–4:05)

Median CPU core-seconds giảm ở cả 10 workload chính, từ 28,9% đến 99,2%. Tuy nhiên median saving của peak cgroup memory bằng 0 ở cả 10 workload. Vì vậy dữ liệu hỗ trợ kết luận giảm CPU trong bộ này, còn chưa hỗ trợ tuyên bố giảm RAM đỉnh. Các tỷ lệ tài nguyên này thuộc ma trận chính, không được tự động áp dụng cho SF10.

## Slide 8 — Hồ sơ tài nguyên Q01 tại SF1 (4:05–4:30)

Biểu đồ chuẩn hóa tiến độ từ 0 đến 100% và lấy trung vị của 10 run. Nó giúp so hình dạng CPU và bộ nhớ, nhưng không thay thế wall time tuyệt đối. Việc kết hợp latency, profile tài nguyên và physical plan giúp đánh giá lợi ích từ nhiều phép đo.

## Slide 9 — Native coverage và fallback (4:30–5:10)

Chín workload chính đạt native coverage 100%. M08 đạt 40%, gồm 4 native operator, 6 fallback operator và 2 transition, đồng thời có speedup thấp nhất. Đây là quan hệ mô tả. Coverage đếm operator đủ điều kiện trong plan, không phải phần trăm CPU hay wall time chạy native. Phần SF10 sẽ cho thấy ngay cả coverage 100% cũng có thể đi cùng lợi ích tốc độ rất nhỏ.

## Slide 10 — Plan ổn định và H2 mang tính mô tả (5:10–5:45)

Initial và final semantic plan ổn định ở cả 10 workload chính. Tương quan Spearman giữa native coverage và log-speedup là dương 0,522; với fallback và transition là âm 0,522. Mẫu chỉ có 10 workload và nhiều giá trị coverage trùng nhau. Vì vậy phân tích này không có p-value và không chứng minh quan hệ nhân quả. Muốn xác định tác động riêng của operator cần thí nghiệm ablation.

## Slide 11 — So sánh cạnh tranh với Gluten + Velox (5:45–6:20)

Apache Gluten với Velox công bố 3,34 lần overall và tối đa 23,45 lần ở một query trên TPCH-like. Cấu hình đó dùng 3 terabyte, Xeon 8592+ và Spark 3.3.1, khác môi trường của đề tài. Đây là tham chiếu về một lựa chọn native khác, không dùng để xếp hạng trực tiếp. Lựa chọn production cần A/B benchmark trên cùng dữ liệu và tài nguyên, kèm correctness, latency, chi phí và plan coverage.

## Slide 12 — SF10: Comet nhanh hơn ở 3/4 truy vấn (6:20–7:35)

Phần mở rộng vừa hoàn tất có 10 cặp cho mỗi Q01, Q03, Q06 và Q12. Tổng cộng 80 lượt đo cùng 16 record correctness/plan đều thành công. Q01 đạt 4,51 lần, khoảng tin cậy 4,15 đến 4,79. Q06 đạt 1,46 lần, khoảng 1,36 đến 1,57. Q12 đạt 1,64 lần, khoảng 1,53 đến 1,68. Ba khoảng này đều trên 1.

Q03 đạt 0,99 lần, khoảng tin cậy 0,94 đến 1,03, nên chưa thấy khác biệt tốc độ chắc chắn. Không nên nói Comet nhanh hơn ở cả bốn truy vấn, cũng chưa đủ để kết luận Q03 chậm hơn chắc chắn. Thời gian trung vị Q01 giảm từ 42,206 xuống 9,318 giây. Cả 192 cửa sổ tài nguyên đầy đủ và không dùng swap. Đây vẫn là kết quả TPC-H-derived thăm dò trên laptop, chưa được kiểm toán.

## Slide 13 — Đối chiếu SF1 và SF10 theo từng truy vấn (7:35–8:40)

Đã có số liệu ở hai scale cho cùng bốn query ID. Q01 có trung vị speedup 3,509 ở SF1 và 4,51 ở SF10, trong khi Q03 là 1,484 và 0,99. Bảng đối chiếu này mang tính mô tả. Hai bộ khác commit, image và giao thức warm-up nên chưa thể quy chênh lệch cho riêng quy mô dữ liệu.

Vòng SF10 mới cũng trải qua hai phiên máy: Q03, Q06, Q12 hoàn tất trước khi WSL khởi động lại, Q01 hoàn tất sau đó với cùng commit, image và tài nguyên. Ba lần launcher Q01 lỗi trước thực thi được lưu riêng, không tạo measurement. SF10 vòng 1 có 5 cặp được giữ độc lập, không gộp thành 15 cặp. Cả bốn truy vấn vòng 2 có 100% native coverage, nhưng Q03 gần 1 lần, nên coverage không thay thế phép đo latency.

## Slide 14 — Kết luận RQ1, RQ2 và RQ3 (8:40–9:20)

RQ1 có bằng chứng tăng tốc trong ma trận chính và ở Q01, Q06, Q12 của SF10; Q03 SF10 chưa rõ khác biệt. RQ2 cho thấy lợi ích phụ thuộc workload, coverage cao chưa bảo đảm tăng tốc lớn. RQ3 đã có đối chiếu mô tả SF1–SF10, nhưng chưa cô lập ảnh hưởng của scale hay overhead nhân quả. Các kết luận đều gắn với bộ đo cụ thể, không trở thành khẳng định rằng Comet luôn nhanh hơn.

## Slide 15 — Phạm vi kết luận và hướng tiếp theo (9:20–10:00)

Đóng góp gồm pipeline Lakehouse, runner ghép cặp, correctness gate và bằng chứng truy vết cho kết quả SF1 cùng phần mở rộng SF10. Phạm vi vẫn là laptop, một worker và mẫu 10 cặp, chưa báo cáo P95. Bước tiếp theo là đo hai scale trên cùng commit và warm-up, tăng số cặp, lặp nhiều phiên máy rồi thử nhiều worker. Có thể đánh giá thêm Gluten với Velox bằng cùng phương pháp. Em xin cảm ơn thầy cô và sẵn sàng trả lời câu hỏi.

## Bốn câu cần nhớ nếu phải rút ngắn

1. Ma trận chính đạt geometric mean 1,508×; con số này không gồm SF10.
2. SF10 vòng 2 đạt Q01 4,51×, Q06 1,46× và Q12 1,64×; Q03 0,99× với CI chứa 1.
3. SF10 có 96 record thành công, 80 lượt đo và 192 cửa sổ tài nguyên không swap; ba lỗi launcher trước truy vấn được lưu riêng.
4. Đối chiếu SF1–SF10 mang tính mô tả do khác commit, warm-up và phiên máy, chưa chứng minh quy luật scalability.

Nguồn: [báo cáo nghiên cứu cập nhật](../../docs/research-report.md), [thống kê SF10 vòng 2](../../docs/benchmarks/sf10/sf10-r2-benchmark-summary.json), [báo cáo ma trận chính](../../results/reports/technical-report.md).
