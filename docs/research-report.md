# Báo cáo nghiên cứu Spark và DataFusion Comet

Cập nhật ngày 24/09/2026, bổ sung benchmark SF10 vòng 2 với 10 cặp đo cho mỗi truy vấn Q01, Q03, Q06 và Q12.

## Kết quả chính

Trong bộ SF10 mới, Comet đạt trung vị tăng tốc theo cặp **4,51× ở Q01, 1,46× ở Q06 và 1,64× ở Q12**. Khoảng tin cậy 95% của ba truy vấn này đều nằm trên 1×. **Q03 đạt 0,99× với khoảng tin cậy 0,94–1,03×**, nên chưa có bằng chứng về khác biệt tốc độ chắc chắn trong điều kiện đo.

Ma trận chính trước đó gồm 10 workload e-commerce/micro và TPC-H-derived SF1, đạt geometric mean của median paired speedup **1,508×**. Con số này chỉ thuộc ma trận chính, không bao gồm SF10. Các kết quả SF10 được trình bày riêng theo truy vấn và theo vòng chạy.

## Phạm vi bằng chứng

| Bộ đo | Phạm vi | Cặp đo mỗi truy vấn/workload | Lượt đo | Tổng record thành công |
|---|---|---:|---:|---:|
| Ma trận chính | 6 e-commerce/micro, 4 TPC-H-derived SF1 | 10 | 200 | 240 |
| SF10 vòng 1 | Q01, Q03, Q06, Q12 | 5 | 40 | 56 |
| SF10 vòng 2 | Q01, Q03, Q06, Q12 | 10 | 80 | 96 |

Ma trận chính có báo cáo và publication receipt lịch sử tại commit `d839d28257b35bb794aa57e5954e6b8b97528ede`. SF10 vòng 1 có commit `412e949e76de838d58385ccc135dce579be94df9`; vòng 2 có commit `e8c0c5c834b6ff2fd792f2220f79a5ae9db8728e`. Các receipt này xác nhận bộ bằng chứng tại thời điểm chạy, không xác nhận một lần chạy mới tại commit tài liệu hiện tại.

SF10 là phần mở rộng thăm dò trên laptop, sử dụng TPC-H-derived và warm storage cache. Đây không phải kết quả TPC-H được kiểm toán. Không gộp SF10 vào ma trận publication chính và không gộp mẫu của hai vòng SF10.

## Thiết kế thực nghiệm SF10 vòng 2

| Thuộc tính | Giá trị |
|---|---|
| Dataset | `tpch-derived-sf10-852ad0a5ee31-v1` |
| Quy mô | 8 bảng, 86.586.082 dòng, 35 tệp Parquet, 4.005.253.732 byte |
| CPU máy | Intel Core i7-10750H, 2,60 GHz |
| Tài nguyên thực thi | 1 Spark worker, 2 core, cgroup 5 GiB, executor heap 2 GiB, off-heap 1 GiB |
| Runtime | Spark 4.1.3, Comet 1.0.0, Iceberg 1.11.0, Scala 2.13.17, Java 17.0.19 |
| Lịch chạy | 10 cặp/truy vấn, cân bằng 5 AB và 5 BA, seed `20260923` |
| Warm-up | 2 lượt không tính giờ bên trong mỗi application đo |
| Đại lượng đo | `query_wall_time_ms`, không bao gồm khởi động application hoặc nạp dữ liệu |
| Thống kê chính | Trung vị tỷ số thời gian Spark/Comet theo từng cặp |
| Khoảng tin cậy | Paired percentile bootstrap 95%, 10.000 resamples, seed `20260824` |

Hai engine đọc cùng trạng thái dữ liệu và dùng cấu hình tài nguyên đã khóa trong từng campaign. Correctness và physical plan được kiểm tra trước khi nhận phép đo. Vòng 2 có 80 measurement record và 16 record kiểm tra correctness/plan; các warm-up trong application không phải 16 record này và không được cộng vào 80 lượt đo.

Tăng tốc lớn hơn 1 nghĩa là Comet nhanh hơn. Trung vị tỷ số theo cặp không nhất thiết bằng tỷ số của hai thời gian trung vị. Vì mỗi engine chỉ có 10 measurement cho mỗi truy vấn, P95 chưa được báo cáo.

## Kết quả SF10 vòng 2

| Truy vấn | Spark trung vị (giây) | Comet trung vị (giây) | Trung vị tăng tốc theo cặp | CI 95% | Diễn giải |
|---|---:|---:|---:|---:|---|
| Q01 | 42,206 | 9,318 | **4,51×** | 4,15–4,79× | Comet nhanh hơn trong điều kiện đo |
| Q03 | 36,697 | 37,374 | **0,99×** | 0,94–1,03× | Chưa rõ khác biệt |
| Q06 | 3,462 | 2,352 | **1,46×** | 1,36–1,57× | Comet nhanh hơn trong điều kiện đo |
| Q12 | 15,672 | 9,599 | **1,64×** | 1,53–1,68× | Comet nhanh hơn trong điều kiện đo |

Nguồn: [tổng hợp SF10 vòng 2](benchmarks/sf10/sf10-r2-benchmark-summary.json), [thống kê Q01](benchmarks/sf10/sf10-r2-Q01-summary.json), [Q03](benchmarks/sf10/sf10-r2-Q03-summary.json), [Q06](benchmarks/sf10/sf10-r2-Q06-summary.json), [Q12](benchmarks/sf10/sf10-r2-Q12-summary.json).

Cả bốn truy vấn SF10 có native operator coverage bằng 100% trong các plan được ghi nhận và không có fallback reason. Q03 vẫn gần 1×, cho thấy tỷ lệ native operator cao không đủ để bảo đảm tăng tốc lớn. Coverage là tỷ lệ đếm operator, không phải phần trăm wall time hay CPU chạy native. Chưa có thí nghiệm ablation để kết luận nguyên nhân riêng của kết quả Q03.

## Đối chiếu SF1 và SF10

| Truy vấn | SF1, 10 cặp: tăng tốc [CI 95%] | SF10 vòng 2, 10 cặp: tăng tốc [CI 95%] |
|---|---:|---:|
| Q01 | 3,509× [3,284; 3,990] | 4,51× [4,15; 4,79] |
| Q03 | 1,484× [1,422; 1,540] | 0,99× [0,94; 1,03] |
| Q06 | 1,337× [1,109; 1,511] | 1,46× [1,36; 1,57] |
| Q12 | 1,421× [1,196; 1,540] | 1,64× [1,53; 1,68] |

Nguồn SF1: [báo cáo kỹ thuật của ma trận chính](../results/reports/technical-report.md). Nguồn SF10: [tổng hợp vòng 2](benchmarks/sf10/sf10-r2-benchmark-summary.json).

Đã có số liệu cho cùng bốn query ID ở hai scale, nên nhận xét “chưa có SF10” không còn mô tả đầy đủ nghiên cứu hiện tại. Tuy nhiên, đây là đối chiếu mô tả giữa các bộ đo độc lập. SF1 và SF10 khác commit, image và giao thức warm-up: SF10 thực hiện hai warm-up ngay bên trong mỗi application đo. SF10 vòng 2 cũng trải qua hai phiên máy. Vì thế không thể quy mọi thay đổi trong bảng cho riêng scale factor, không gộp mẫu, không tính CI cho chênh lệch giữa scale và không suy ra quy luật scalability tổng quát.

## Đối chiếu hai vòng SF10

| Truy vấn | Vòng 1, 5 cặp: tăng tốc [CI 95%] | Vòng 2, 10 cặp: tăng tốc [CI 95%] |
|---|---:|---:|
| Q01 | 4,85× [3,52; 5,30] | 4,51× [4,15; 4,79] |
| Q03 | 1,03× [0,83; 1,04] | 0,99× [0,94; 1,03] |
| Q06 | 1,57× [1,12; 2,62] | 1,46× [1,36; 1,57] |
| Q12 | 1,63× [1,36; 1,67] | 1,64× [1,53; 1,68] |

Nguồn: [vòng 1](benchmarks/sf10/sf10-benchmark-summary.json) và [vòng 2](benchmarks/sf10/sf10-r2-benchmark-summary.json). Hai vòng độc lập cùng cho thấy Q03 chưa có khác biệt tốc độ chắc chắn. Vòng 2 có nhiều cặp đo hơn và khoảng tin cậy hẹp hơn trong các kết quả này. Không dùng bảng để khẳng định thay đổi giữa hai vòng có ý nghĩa thống kê.

## Kiểm tra tài nguyên và xử lý gián đoạn

[Receipt cuối vòng 2](benchmarks/sf10/sf10-r2-final-verification.json) xác nhận:

- 96 record thành công, trong đó có 80 measurement record.
- 192 cửa sổ thu thập tài nguyên đầy đủ, 21.443 mẫu và swap bằng 0 trong các cửa sổ được kiểm tra.
- Mã băm raw/control evidence khớp và kết quả vòng 1 được giữ nguyên, kiểm tra lại.

Q03, Q06 và Q12 hoàn tất trước khi WSL khởi động lại. Sau đó Q01 có ba lần launcher thất bại vì thư mục chia sẻ của container, trước khi truy vấn thực thi. Các lần lỗi tạo **0 canonical query record**, được lưu riêng trong archive gồm 17 tệp và không nằm trong 96 record thành công. Sau khi tạo lại container, kiểm tra đọc/ghi thư mục, sampler và hiệu chuẩn đạt trước khi chạy Q01. Q01 hoàn tất trong phiên máy tiếp theo với cùng commit, image và tài nguyên của vòng 2.

Việc không dùng swap và thu đủ tài nguyên hỗ trợ tính hợp lệ của phép đo, nhưng không loại bỏ mọi ảnh hưởng của cache, nhiệt độ máy hoặc tải nền. Không dùng các số CPU/RAM của SF1 để suy ra mức tiết kiệm tài nguyên ở SF10.

## Cập nhật câu trả lời nghiên cứu

**RQ1:** Trong ma trận chính, 10/10 workload có CI của paired speedup nằm trên 1. Trong phần mở rộng SF10 vòng 2, Q01, Q06 và Q12 có lợi ích rõ, còn Q03 chưa cho thấy khác biệt chắc chắn. Vì vậy không kết luận Comet luôn nhanh hơn Spark.

**RQ2:** Kết quả ma trận chính về M08 và fallback được giữ nguyên. SF10 bổ sung bốn truy vấn có native coverage 100%, nhưng Q03 cho thấy coverage không thay thế phép đo latency. Chưa có chứng minh nhân quả về overhead của từng operator.

**RQ3/H3:** Đã bổ sung đối chiếu mô tả SF1–SF10 cho bốn truy vấn. Chưa thực hiện thí nghiệm chỉ thay scale trong khi giữ cố định toàn bộ commit, image và warm-up, nên chưa tách được tác động riêng của quy mô. Findings tự động của ma trận chính vẫn mô tả đúng phạm vi SF1 của chính bộ đó.

Hướng tiếp theo là đo SF1 và SF10 trên cùng commit và giao thức warm-up, tăng số cặp lên ít nhất 20, lặp nhiều phiên máy và mở rộng nhiều worker. Đây là công việc tiếp theo, chưa phải kết quả của đợt SF10 hiện tại.

## Truy vết và tài liệu trình bày

- [Bản sao báo cáo SF10 vòng 2](benchmarks/sf10/sf10-r2-benchmark-report.md) và [vòng 1](benchmarks/sf10/sf10-benchmark-report.md).
- [Danh mục bản sao cùng SHA-256](benchmarks/sf10/evidence-index.json), [phạm vi sử dụng bằng chứng](benchmarks/sf10/README.md).
- Dataset manifest SHA-256: `d2743e81a4055bc8f07f94036749cb89243c49844ce3716f3f41c00281558582`.
- Image vòng 2: `sha256:d1112e50ee3b26f1e8129143fcaf14ef2000ae2db12bb88166a578bee86d51cd`.
- Summary vòng 2 SHA-256: `9916c859c22c5aef04b1ee0283dc20f29ddc10b883a81e2f17fe8103775589e5`.
- [Slide cập nhật](../deliverables/presentation/lakehouse-comet-research-sf10-20260924.pptx), [lời thuyết trình](../deliverables/presentation/lakehouse-comet-10-minute-script.md), [tóm tắt đề tài](../deliverables/presentation/lakehouse-comet-one-page-summary.md), [hỏi đáp bảo vệ](../deliverables/presentation/lakehouse-comet-defense-qa.md).

Đợt cập nhật này bổ sung tài liệu và slide. Video demo và các artifact báo cáo chính đã được tạo trước đó giữ nguyên.
