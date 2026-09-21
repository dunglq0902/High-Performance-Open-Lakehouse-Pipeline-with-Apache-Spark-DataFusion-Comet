# Lời thuyết trình 10 phút

> Nhịp đề xuất: nói rõ ràng khoảng 130–145 từ/phút. Các mốc thời gian dưới đây cộng lại đúng 10 phút và bám theo 12 slide trong `lakehouse-comet-research.pptx`.

## Slide 1 — Đường ống Open Lakehouse hiệu năng cao (0:00–0:35)

Kính thưa thầy cô và các bạn. Đề tài của em là “Đường ống Open Lakehouse hiệu năng cao với Apache Spark, DataFusion Comet và Apache Iceberg”. Mục tiêu của đề tài không chỉ là xây dựng một pipeline chạy được, mà còn trả lời bằng số liệu rằng: khi giữ nguyên dữ liệu, câu lệnh SQL và tài nguyên, việc bật Comet thay đổi hiệu năng Spark như thế nào. Kết quả em trình bày hôm nay được xây dựng từ 10 workload, mỗi workload có 10 cặp đo, và toàn bộ báo cáo đã vượt qua cổng kiểm tra khả năng công bố.

## Slide 2 — Câu hỏi nghiên cứu và phạm vi (0:35–1:15)

Đề tài có ba câu hỏi nghiên cứu. RQ1 đo mức thay đổi về độ trễ và độ ổn định của từng workload. RQ2 phân tích physical plan để xác định operator nào chạy native và operator nào fallback về Spark. RQ3 xem xét ảnh hưởng của fallback và quy mô dữ liệu đến lợi ích. Phạm vi thực nghiệm gồm một business query, năm microbenchmark và bốn truy vấn TPC-H-derived ở SF1. Hệ thống chạy trên một Spark worker, 2 core và giới hạn cgroup 5 GiB. Đây là phạm vi có chủ đích để bảo đảm phép đo kiểm soát được và có thể tái lập trên laptop.

## Slide 3 — Cơ sở bằng chứng (1:15–1:55)

Mỗi workload có 2 warm-up cho từng engine và 10 cặp measurement Spark–Comet, tổng cộng 24 execution attempts cho một campaign. Với 10 campaign, hệ thống tạo 200 measurement records và không có measurement hay execution attempt nào thất bại. Điểm quan trọng là lỗi đo và lỗi attempt được báo cáo riêng, không bị âm thầm loại khỏi tập dữ liệu. Toàn bộ raw record, log, plan và thông tin tài nguyên đều được giữ lại, nhờ đó các con số trên slide có thể truy ngược về lần chạy cụ thể.

## Slide 4 — Kiến trúc đo lường (1:55–2:40)

Về đường dữ liệu, dữ liệu Parquet được đưa qua các lớp Bronze, Silver và Gold trên Apache Iceberg. Spark thuần và Spark + Comet đọc đúng cùng một snapshot Iceberg. Hai engine chạy trong application độc lập nhưng sử dụng cùng resource envelope và cấu hình chung. Về đường bằng chứng, runner tạo lịch AB/BA ngẫu nhiên có seed cố định để cân bằng ảnh hưởng của thứ tự và cache. Correctness là hard gate trước khi nhận mẫu. Trong mỗi run, hệ thống thu wall time, Spark event log, final AQE plan và dữ liệu CPU, bộ nhớ từ cgroups v2. Mọi kết luận vì vậy đều gắn với commit, dataset manifest, SQL, SparkConf và snapshot cụ thể.

## Slide 5 — Paired speedup (2:40–3:35)

Đây là kết quả trung tâm cho RQ1. Chỉ số chính là median của speedup theo từng cặp, được tính bằng thời gian Spark chia cho thời gian Comet; lớn hơn 1 nghĩa là Comet nhanh hơn. Cả 10 trên 10 workload đều có median speedup lớn hơn 1, và quan trọng hơn, cận dưới của khoảng tin cậy 95% cũng lớn hơn 1. Geometric mean trên 10 median workload là 1,508 lần. Q01 cao nhất với 3,509 lần và khoảng tin cậy từ 3,284 đến 3,990. M08 thấp nhất với 1,115 lần, khoảng tin cậy từ 1,069 đến 1,148. Vì đây là paired design, kết quả phản ánh trực tiếp chênh lệch trong từng cặp chạy thay vì so hai tập trung vị độc lập.

## Slide 6 — Median latency theo workload (3:35–4:20)

Biểu đồ này chuyển speedup về đơn vị dễ quan sát hơn là giây. Ví dụ Q01 giảm từ khoảng 5,44 giây ở Spark xuống 1,51 giây khi bật Comet. B01 giảm từ khoảng 9,11 xuống 5,98 giây. Với M08, độ trễ giảm ít hơn, từ khoảng 13,20 xuống 11,70 giây. Như vậy Comet không tạo ra một hệ số tăng tốc cố định cho mọi truy vấn. Lợi ích phụ thuộc vào hình dạng plan và mức độ operator được thực thi native. Các cột là median; độ phân tán, IQR, min, max và confidence interval vẫn được giữ đầy đủ trong báo cáo kỹ thuật.

## Slide 7 — CPU và bộ nhớ (4:20–5:05)

Về tài nguyên, median CPU core-seconds giảm ở cả 10 workload, từ 28,9% ở M08 đến 99,2% ở Q01. Kết quả này cho thấy lợi ích không chỉ nằm ở thời gian hoàn thành mà còn ở lượng CPU tích lũy trong cửa sổ thực thi. Tuy nhiên, peak cgroup memory có median saving bằng 0 ở cả 10 workload. Vì vậy, kết luận đúng là Comet giảm CPU trong phạm vi đã đo, còn dữ liệu hiện tại không hỗ trợ tuyên bố giảm đỉnh RAM. Sự tách biệt này cũng cho thấy vì sao cần đo nhiều chiều thay vì chỉ nhìn wall time.

## Slide 8 — Hồ sơ tài nguyên Q01 (5:05–5:50)

Slide này đi sâu vào Q01, workload có speedup lớn nhất. Hai đường CPU và bộ nhớ được chuẩn hóa theo tiến độ từ 0 đến 100%, rồi lấy median của 10 run. Comet hoàn thành nhanh hơn và dùng ít CPU core-seconds hơn rõ rệt, trong khi đường bộ nhớ không cho thấy mức giảm peak tương ứng. Cần lưu ý rằng biểu đồ đã chuẩn hóa trục thời gian để so hình dạng sử dụng tài nguyên; nó không thay thế số liệu wall time tuyệt đối ở slide trước. Sự kết hợp giữa latency, CPU profile và plan evidence giúp tránh giải thích kết quả chỉ từ một metric đơn lẻ.

## Slide 9 — Native coverage và fallback (5:50–6:40)

RQ2 được trả lời bằng final physical plan. Chín trên mười workload đạt 100% native coverage theo số lượng operator đủ điều kiện. Ngoại lệ là M08, chỉ đạt 40%, với 4 native operators, 6 fallback operators và 2 transitions giữa miền native và Spark. Các node liên quan đến Window và WindowGroupLimit nằm trong phần fallback của plan. M08 đồng thời là workload có speedup thấp nhất, 1,115 lần. Native coverage ở đây là tỷ lệ theo số operator trong plan, không phải phần trăm thời gian hay CPU chạy native. Do đó nó là chỉ dấu cấu trúc hữu ích, nhưng không nên được diễn giải như một tỷ lệ thời gian thực thi.

## Slide 10 — Plan ổn định và H2 mang tính mô tả (6:40–7:35)

Initial và final semantic plan ổn định ở cả 10 workload, nên khác biệt đo được không đến từ việc plan liên tục thay đổi giữa các lần chạy. Trên 10 workload, tương quan Spearman giữa native coverage và log-speedup là dương 0,522; với số fallback operator và số transition, tương quan là âm 0,522. Chiều tương quan phù hợp với kỳ vọng rằng native coverage cao thường đi cùng lợi ích lớn hơn. Tuy nhiên phân tích này chỉ mang tính mô tả: mẫu chỉ có 10 workload, phần lớn coverage bằng 100%, không tính p-value và không cho phép suy luận nhân quả. Nói cách khác, đây là tín hiệu để giải thích và đặt giả thuyết, không phải bằng chứng rằng coverage là nguyên nhân duy nhất tạo speedup.

## Slide 11 — Kết luận RQ1, RQ2 và RQ3 (7:35–8:45)

Tổng hợp lại: với RQ1, Comet giảm median latency ở 10 trên 10 workload và geometric mean speedup là 1,508 lần. Với RQ2, 9 workload chạy hoàn toàn native theo metric operator-count; M08 có fallback rõ ràng và cũng có lợi ích thấp nhất. Với RQ3, phần liên hệ giữa fallback và speedup đã có kết quả mô tả, nhưng ảnh hưởng của quy mô dữ liệu chưa thể ước lượng. Lý do là tập bằng chứng TPC-H hiện chỉ có SF1, chưa có SF10 để tạo so sánh matched-query. Vì vậy đề tài chủ động không đưa ra tuyên bố tổng quát về scalability. Việc nói rõ phần nào trả lời được và phần nào chưa trả lời được là một kết quả nghiên cứu quan trọng, không phải thiếu sót bị che giấu.

## Slide 12 — Phạm vi kết luận và quy trình phát hành (8:45–10:00)

Kết luận cuối cùng của đề tài là: trên cấu hình laptop single-worker đã khóa, DataFusion Comet mang lại cải thiện latency có ý nghĩa và giảm CPU core-seconds trên toàn bộ 10 workload được đo. Lợi ích lớn nhất xuất hiện khi plan được native hóa tốt; fallback như ở M08 làm mức tăng thấp hơn. Tuy nhiên, kết quả chỉ áp dụng cho 2 core, giới hạn 5 GiB, 10 cặp đo và các phiên bản Spark 4.1.3, Comet 1.0.0, Iceberg 1.11.0. P95 không được ước lượng vì n nhỏ hơn 20. Bốn truy vấn TPC-H là TPC-H-derived và không phải kết quả benchmark TPC-H được kiểm toán.

Điểm đóng góp của đề tài gồm cả hệ thống lẫn phương pháp: một pipeline Open Lakehouse hoàn chỉnh, benchmark runner ghép cặp, correctness gate, thu thập event log và cgroups, phân tích native/fallback plan, báo cáo tái tạo được và bộ bằng chứng có provenance. Hướng phát triển tiếp theo là chạy SF10 sau capacity gate, mở rộng sang multi-node và tăng số lần lặp để đánh giá tail latency. Em xin cảm ơn thầy cô và các bạn đã lắng nghe, và em sẵn sàng trả lời câu hỏi.

## Ba câu cần nhớ nếu phải rút ngắn

1. Comet đạt geometric mean speedup 1,508× và 10/10 workload có CI 95% nằm trên 1.
2. Chín workload đạt 100% native coverage; M08 chỉ 40% và có speedup thấp nhất 1,115×.
3. Kết quả đáng tin trong phạm vi single-worker SF1 đã khóa, nhưng chưa đủ để kết luận scalability vì chưa có SF10.
