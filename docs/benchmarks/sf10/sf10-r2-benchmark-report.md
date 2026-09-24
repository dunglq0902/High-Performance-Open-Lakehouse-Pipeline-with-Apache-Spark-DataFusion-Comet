# Benchmark SF10 — vòng 2, 10 cặp đo mỗi truy vấn

Đã hoàn tất 96/96 lượt chạy: 80 lượt đo và 16 lượt kiểm tra tính đúng đắn/kế hoạch thực thi. Toàn bộ 192 cửa sổ thu thập tài nguyên đều đầy đủ và không dùng swap.

| Truy vấn | Spark trung vị (giây) | Comet trung vị (giây) | Tăng tốc trung vị theo cặp | Khoảng tin cậy 95% |
|---|---:|---:|---:|---:|
| Q01 | 42.206 | 9.318 | 4.51× | 4.15–4.79× |
| Q03 | 36.697 | 37.374 | 0.99× | 0.94–1.03× |
| Q06 | 3.462 | 2.352 | 1.46× | 1.36–1.57× |
| Q12 | 15.672 | 9.599 | 1.64× | 1.53–1.68× |

Mỗi truy vấn có 10 cặp đo, cân bằng 5 cặp Spark trước và 5 cặp Comet trước, seed 20260923. Mỗi ứng dụng đo có 2 lượt khởi động không tính giờ. Thời gian đo là query_wall_time_ms, không gồm khởi động ứng dụng hay nạp dữ liệu.

Tăng tốc là trung vị tỷ số Spark/Comet theo từng cặp; lớn hơn 1 nghĩa là Comet nhanh hơn. Giá trị này không nhất thiết bằng tỷ số của hai thời gian trung vị. Khoảng tin cậy là paired bootstrap 95% theo bộ tổng hợp của dự án.

- Q01: cho thấy Comet nhanh hơn trong điều kiện đo này.
- Q03: chưa cho thấy khác biệt tốc độ chắc chắn vì khoảng tin cậy bao gồm 1× trong điều kiện đo này.
- Q06: cho thấy Comet nhanh hơn trong điều kiện đo này.
- Q12: cho thấy Comet nhanh hơn trong điều kiện đo này.

## Đối chiếu hai vòng độc lập

Kết quả vòng 1 được giữ nguyên và đã được kiểm tra lại mã băm. Bảng sau để tham khảo giữa hai phiên chạy; không gộp mẫu hoặc suy ra kiểm định khác biệt giữa hai vòng.

| Truy vấn | Vòng 1: 5 cặp | Vòng 2: 10 cặp |
|---|---:|---:|
| Q01 | 4.85× | 4.51× |
| Q03 | 1.03× | 0.99× |
| Q06 | 1.57× | 1.46× |
| Q12 | 1.63× | 1.64× |

Đây là kết quả thăm dò trên laptop, warm-storage-cache, TPC-H-derived, non-audited; không phải kết quả TPC-H được kiểm toán.

Dữ liệu: `tpch-derived-sf10-852ad0a5ee31-v1`, 86,586,082 dòng. Commit: `e8c0c5c834b6ff2fd792f2220f79a5ae9db8728e`.

[Tổng hợp JSON](sf10-r2-benchmark-summary.json) · [Báo cáo vòng 1](sf10-benchmark-report.md)

- [Q01: thống kê đầy đủ](sf10-r2-Q01-summary.json)
- [Q03: thống kê đầy đủ](sf10-r2-Q03-summary.json)
- [Q06: thống kê đầy đủ](sf10-r2-Q06-summary.json)
- [Q12: thống kê đầy đủ](sf10-r2-Q12-summary.json)

Phiên chạy bị gián đoạn sau khi Q03, Q06 và Q12 hoàn tất. Q01 được tiếp tục sau khi WSL khởi động lại, với cùng commit, image và cấu hình tài nguyên. Ba bộ kết quả đã hoàn tất được giữ nguyên; các lượt đo vẫn có hai lượt khởi động không tính giờ theo cấu hình. Vì vậy đây là bộ kết quả qua hai phiên máy, cần lưu ý khi đối chiếu giữa các truy vấn.

Ba lần khởi động Q01 đã thất bại trước khi truy vấn chạy do thư mục chia sẻ của container sau khởi động lại. Các lần lỗi được lưu riêng, xác minh mã băm và không tính vào số lượt đo thành công. Sau khi tạo lại container, kiểm tra đọc/ghi thư mục, bộ thu thập và hiệu chuẩn đều đạt trước khi chạy lại Q01.

