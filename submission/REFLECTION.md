# Reflection — Top 5 Lakehouse Anti-Patterns

## Anti-pattern dễ mắc nhất: Small-File Problem (Bỏ qua OPTIMIZE)

Trong hệ thống dữ liệu của team, anti-pattern mà chúng tôi dễ mắc nhất là **Small-File Problem** do streaming ingestion.

### Vì sao dễ mắc?

Khi xây dựng pipeline LLM observability, mỗi micro-batch từ Kafka/streaming trigger tạo ra một file nhỏ. Sau vài giờ, bảng có hàng trăm đến hàng nghìn file, mỗi file chỉ vài KB — trong khi production target là 128–512 MB/file. Điều nguy hiểm là **mỗi commit đều đúng về mặt logic**, không có lỗi nào bị báo, nhưng tích lũy lại tạo ra vấn đề nghiêm trọng.

### Hậu quả đo được (từ NB2 + NB6)

- **Query chậm phi tuyến**: mỗi file tốn một GET request trên object storage. 200 files × 50,000 queries/ngày = 10 triệu GETs/ngày, chi phí request vượt xa chi phí storage.
- **Metadata bloat**: 200 JSON entries trong `_delta_log/` mà cold reader phải replay hết — từ 200ms thành 20s cold start.
- **Z-ORDER mất tác dụng**: min/max stats bị overlap khi dữ liệu phân tán qua nhiều file nhỏ → engine không skip được file nào.

### Giải pháp

Chạy **compaction (Job 1)** theo lịch cron (mỗi giờ hoặc mỗi ngày), kết hợp **checkpoint (Job 5)** để giảm cold-start. NB6 chứng minh compaction giảm ≥10× số file và checkpoint chuyển 200 JSON entries thành 1 Parquet file.
