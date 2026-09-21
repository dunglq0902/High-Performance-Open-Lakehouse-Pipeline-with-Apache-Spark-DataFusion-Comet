# Bộ câu hỏi phản biện và trả lời gợi ý

> Cách sử dụng: trả lời trực tiếp kết luận trước, sau đó nêu bằng chứng và cuối cùng nhắc giới hạn. Không cần đọc nguyên văn; mỗi câu trả lời nên kéo dài khoảng 20–45 giây.

## A. Bài toán và đóng góp

### 1. Đóng góp mới của đề tài là gì nếu Spark, Comet và Iceberg đều đã có sẵn?

Đóng góp chính là tích hợp chúng thành một pipeline Open Lakehouse có thể đo và tái lập, thay vì chỉ dựng một demo chức năng. Đề tài xây dựng benchmark runner ghép cặp, correctness gate, kiểm soát provenance, thu event log và cgroups, phân tích native/fallback plan, rồi tự động tạo báo cáo từ raw evidence. Vì vậy giá trị nằm ở cả hệ thống, phương pháp đo và kết quả thực nghiệm có thể truy vết.

### 2. Vì sao chọn DataFusion Comet thay vì một công cụ tăng tốc khác?

Comet phù hợp với câu hỏi nghiên cứu vì nó gắn trực tiếp vào Spark, chuyển một phần physical plan sang engine native dựa trên Apache DataFusion và Arrow. Điều này cho phép so sánh cùng pipeline và SQL khi chỉ thay execution path. Đề tài không khẳng định Comet tốt hơn mọi accelerator; kết luận chỉ là hiệu quả của Comet trong cấu hình và workload đã khóa.

### 3. Apache Iceberg đóng vai trò gì trong nghiên cứu hiệu năng này?

Iceberg tạo lớp bảng có snapshot và metadata rõ ràng, giúp hai engine đọc đúng cùng một trạng thái dữ liệu. Dataset manifest và snapshot ID là một phần của provenance, giảm nguy cơ hai phía benchmark trên dữ liệu khác nhau. Benchmark chính tập trung vào đọc và query execution; Iceberg write vẫn do Spark thực hiện.

## B. Thiết kế thực nghiệm

### 4. Làm sao bảo đảm so sánh Spark và Comet là công bằng?

Hai engine dùng cùng dữ liệu, Iceberg snapshot, SQL, SparkConf chung, CPU, memory limit và storage. Mỗi engine chạy trong application độc lập vì plugin và shuffle manager không thể đổi an toàn trong cùng SparkSession. Chỉ các cấu hình cần thiết để bật Comet nằm trong allowlist; mọi khác biệt còn lại được kiểm tra và lưu vào bằng chứng.

### 5. Tại sao dùng lịch AB/BA ngẫu nhiên?

Nếu luôn chạy Spark trước rồi Comet sau, engine chạy sau có thể hưởng lợi từ cache hoặc chịu ảnh hưởng nhiệt độ máy. Lịch paired randomized AB/BA cân bằng hiệu ứng thứ tự giữa các cặp. Seed được cố định và lịch được lưu trong manifest nên vẫn tái lập được.

### 6. Mỗi workload có bao nhiêu lần chạy và vì sao báo cáo ghi 24 attempts?

Mỗi engine có 2 warm-up và 10 measurement, nên một campaign có 12 lần cho Spark và 12 lần cho Comet, tổng cộng 24 execution attempts. Chỉ 10 cặp measurement được đưa vào thống kê; warm-up không được trộn vào kết quả. Cả 10 campaign đều có 0 failed attempt và 0 measurement failure.

### 7. Vì sao chỉ dùng 10 cặp đo, liệu có quá ít không?

Mười cặp là mức tối thiểu đã khóa trước cho ma trận chính trên laptop, đủ để báo cáo median, IQR, min/max và bootstrap CI, nhưng chưa đủ để ước lượng tail latency đáng tin cậy. Vì vậy P95 được chủ động bỏ qua khi n nhỏ hơn 20. Nếu mở rộng nghiên cứu, ưu tiên đầu tiên là tăng số cặp và lặp trên nhiều máy hoặc nhiều ngày.

### 8. Tại sao dùng median paired speedup thay vì ratio of medians?

Paired speedup tính tỷ lệ Spark/Comet trong từng cặp chạy cùng điều kiện gần nhau, sau đó lấy median. Cách này bảo toàn cấu trúc ghép cặp và giảm ảnh hưởng của drift theo thời gian. Ratio of medians vẫn được báo cáo như phép kiểm tra chéo, nhưng không phải estimand chính vì nó bỏ mất quan hệ từng cặp.

### 9. Bootstrap 10.000 lần với n=10 có đáng tin không?

Bootstrap không tạo thêm thông tin mới; nó lượng hóa độ bất định dựa trên 10 cặp quan sát. Đề tài dùng 10.000 resamples, percentile method và seed cố định để kết quả tái lập, đồng thời luôn công bố IQR, min và max. Vì n nhỏ, kết luận được giới hạn trong workload và môi trường đã đo, không được mở rộng thành khẳng định tổng quát.

### 10. Correctness được kiểm tra như thế nào?

Mỗi workload phải qua Spark–Comet correctness gate trước khi mẫu được chấp nhận. Hệ thống kiểm tra schema và kết quả theo hợp đồng workload; mismatch là hard failure, không được retry để biến thành kết quả hợp lệ. Dataset fingerprint, SQL, configuration và raw output liên quan đều được lưu để truy vết.

### 11. Làm sao biết không có lỗi bị loại bỏ có chọn lọc?

Measurement failures và execution-attempt failures là hai trường riêng trong báo cáo. Attempt lỗi phải có record và log bất biến; runner không được âm thầm bỏ qua. Trong bộ bằng chứng hiện tại, cả hai con số đều bằng 0 trên toàn bộ 10 campaign.

## C. Diễn giải kết quả

### 12. Kết quả quan trọng nhất là gì?

Median paired speedup lớn hơn 1 ở cả 10 workload và toàn bộ CI 95% có cận dưới trên 1. Geometric mean của 10 median speedup là 1,508×. Q01 cao nhất 3,509×; M08 thấp nhất 1,115× nhưng CI vẫn hoàn toàn trên 1.

### 13. Vì sao Q01 nhanh hơn đến 3,509×?

Trong bằng chứng hiện tại, Q01 có 100% native operator coverage, không có fallback operator và giảm 99,2% median CPU core-seconds. Plan và resource profile nhất quán với một execution path native hiệu quả. Tuy nhiên, đề tài không quy toàn bộ speedup cho một operator riêng lẻ vì chưa có ablation experiment tách từng yếu tố.

### 14. Vì sao M08 chỉ nhanh hơn 1,115×?

M08 là ngoại lệ có native coverage 40%, gồm 4 native operators, 6 fallback operators và 2 transitions. Final plan còn các node như Window và WindowGroupLimit ngoài miền native, làm phát sinh phần việc Spark và biên chuyển đổi. Đây là bằng chứng cơ chế hợp lý cho lợi ích thấp hơn, nhưng vẫn là quan hệ mô tả chứ chưa phải chứng minh nhân quả.

### 15. Native coverage 100% có nghĩa toàn bộ thời gian chạy trong native code không?

Không. Coverage là tỷ lệ đếm các physical-plan operators đủ điều kiện, loại trừ một số wrapper hoặc planning nodes theo định nghĩa. Nó không phải phần trăm CPU time hay wall time chạy native. Vì vậy coverage phải được đọc cùng latency, CPU profile và transition/fallback evidence.

### 16. Vì sao CPU giảm mạnh nhưng peak memory không giảm?

CPU core-seconds là tích phân mức CPU theo thời gian, nên giảm latency và thực thi vector hóa có thể làm metric này giảm mạnh. Peak cgroup memory lại ghi mức cao nhất của toàn container trong cửa sổ chạy và có thể bị chi phối bởi heap/off-heap budget, baseline runtime hoặc một đỉnh ngắn. Dữ liệu hiện tại cho median peak-memory saving bằng 0 ở 10/10 workload, nên đề tài không tuyên bố Comet tiết kiệm RAM đỉnh.

### 17. Tương quan H2 là bao nhiêu và có ý nghĩa thống kê không?

Trên 10 workload, Spearman rho là +0,522 cho native coverage với log-speedup, và −0,522 cho fallback operator count cũng như transition count. Dấu của các hệ số phù hợp với giả thuyết, nhưng đây chỉ là phân tích exploratory: không có p-value, mẫu nhỏ và dữ liệu coverage có nhiều ties. Không được diễn giải thành quan hệ nhân quả.

### 18. Initial và final AQE plan có thay đổi giữa các run không?

Không có drift semantic được quan sát: cả 10 workload đều có initial và final semantic plan ổn định trong tập đo. Điều này làm giảm khả năng speedup bị giải thích bởi một chiến lược plan thay đổi ngẫu nhiên giữa các lần chạy. Hash và operator inventory được lưu trong `plan-insights.json` để kiểm tra lại.

## D. Giới hạn và khả năng khái quát

### 19. Tại sao không có SF10, và như vậy RQ3 đã hoàn thành chưa?

SF10 là phạm vi tùy chọn và chỉ được chạy khi capacity gate trên laptop đạt. Bộ bằng chứng được chấp nhận hiện chỉ có SF1, nên không có matched query giữa SF1 và SF10. Vì vậy phần scalability của RQ3 được ghi đúng là “not estimable”; đề tài không tuyên bố đã chứng minh khả năng mở rộng theo quy mô dữ liệu.

### 20. Có thể gọi đây là kết quả benchmark TPC-H không?

Không nên. Bốn truy vấn sử dụng dữ liệu và logic TPC-H-derived để tạo workload phân tích có kiểm soát, nhưng quy trình không phải benchmark TPC-H được kiểm toán và không bao phủ đầy đủ chuẩn. Cách diễn đạt đúng là “TPC-H-derived ở SF1”.

### 21. Kết quả single-node có áp dụng cho cluster production không?

Không thể suy rộng trực tiếp. Single-worker giúp cô lập execution engine nhưng chưa phản ánh network shuffle, nhiều executor, autoscaling, contention và failure mode của production. Kết quả là bằng chứng mạnh cho cấu hình đã đo và là cơ sở để thiết kế thí nghiệm multi-node tiếp theo.

### 22. Vì sao không báo cáo P95?

Chính sách của đề tài chỉ công bố P95 khi mỗi engine có ít nhất 20 measurement thành công. Ở đây n bằng 10, nên P95 có thể dao động mạnh và tạo cảm giác chính xác giả. Thay vào đó báo cáo median, IQR, min/max và CI 95% của estimand chính.

### 23. Có thể khẳng định Comet luôn nhanh hơn Spark không?

Không. Có thể khẳng định rằng trong 10 workload, dữ liệu, runtime và resource envelope đã khóa, Comet có median paired speedup trên 1 và CI 95% cũng trên 1. Không thể chuyển kết luận đó thành “mọi workload” hoặc “mọi cluster”, vì operator không được hỗ trợ, conversion overhead và cấu hình khác có thể làm kết quả thay đổi.

## E. Tái lập và hướng phát triển

### 24. Người khác có thể tái lập kết quả như thế nào?

Repository khóa phiên bản runtime, container image, cấu hình benchmark, SQL, dataset manifest và seed. Mỗi kết quả có raw record, event log, plan, resource sample và provenance gắn với commit bằng chứng. Quy trình chuẩn là dựng môi trường, chạy smoke/correctness, chạy benchmark, tạo report và kiểm tra `publishable: true` trước khi sử dụng kết quả.

### 25. Nếu có thêm thời gian, bạn sẽ làm gì trước?

Ưu tiên một là chạy SF10 sau khi đạt capacity gate để trả lời phần scalability của RQ3. Ưu tiên hai là tăng số cặp lên ít nhất 20 để có thể phân tích P95. Sau đó mở rộng sang nhiều worker và thực hiện ablation theo nhóm operator để phân biệt tác động của native scan, shuffle, aggregation, join và chi phí transition.

## Câu trả lời kết thúc khi gặp câu hỏi ngoài phạm vi

“Dữ liệu hiện tại chưa đủ để khẳng định điều đó. Trong phạm vi đã đo, em có thể kết luận …; để trả lời rộng hơn, em sẽ bổ sung thí nghiệm … và giữ nguyên cùng quy trình correctness, pairing và provenance.”
