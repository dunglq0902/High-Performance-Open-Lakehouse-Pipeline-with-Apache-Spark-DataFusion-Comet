# Kết quả benchmark SF10

Đã hoàn tất bốn truy vấn Q01, Q03, Q06 và Q12: 56 lượt chạy thành công, gồm 40 lượt đo và 16 lượt kiểm tra kết quả/kế hoạch thực thi.

| Truy vấn | Spark trung vị (giây) | Comet trung vị (giây) | Tăng tốc trung vị theo cặp | Khoảng tin cậy 95% |
|---|---:|---:|---:|---:|
| Q01 | 42.732 | 8.873 | 4.85× | 3.52–5.30× |
| Q03 | 34.825 | 38.273 | 1.03× | 0.83–1.04× |
| Q06 | 5.802 | 3.608 | 1.57× | 1.12–2.62× |
| Q12 | 19.438 | 13.926 | 1.63× | 1.36–1.67× |

Mỗi truy vấn có 5 cặp đo Spark/Comet, thứ tự ngẫu nhiên có seed cố định. Mỗi ứng dụng đo chạy 2 lượt khởi động không tính giờ trước lượt đo. Thời gian là query_wall_time_ms, không gồm khởi động ứng dụng hay nạp dữ liệu.

Tăng tốc là trung vị của tỷ số Spark/Comet trong từng cặp; lớn hơn 1 nghĩa là Comet nhanh hơn. Khoảng tin cậy được tính bằng paired bootstrap 95% theo bộ tổng hợp của dự án.

Q01, Q06 và Q12 cho thấy Comet nhanh hơn trong lượt chạy này. Q03 chưa cho thấy lợi ích tăng tốc chắc chắn: khoảng tin cậy 95% là 0,83–1,04×, bao gồm mức 1×. Tỷ số trung vị theo cặp không nhất thiết bằng tỷ số của hai thời gian trung vị ở các cột trước.

Đây là kết quả thăm dò trên laptop, warm-storage-cache, TPC-H-derived và non-audited; không phải kết quả TPC-H được kiểm toán. Năm cặp đo chỉ hỗ trợ kết luận trong môi trường và bộ truy vấn này.

Dữ liệu: `tpch-derived-sf10-852ad0a5ee31-v1`, 86,586,082 dòng. Commit chạy: `412e949e76de838d58385ccc135dce579be94df9`.

Các campaign đều vượt qua kiểm tra hoàn tất, tính đúng đắn Spark/Comet và các cổng kiểm soát của runner. Báo cáo SF1 được giữ riêng.

## Tệp kết quả

[Tổng hợp máy đọc](sf10-benchmark-summary.json)

- [Q01: thống kê đầy đủ](sf10-Q01-summary.json)
- [Q03: thống kê đầy đủ](sf10-Q03-summary.json)
- [Q06: thống kê đầy đủ](sf10-Q06-summary.json)
- [Q12: thống kê đầy đủ](sf10-Q12-summary.json)
