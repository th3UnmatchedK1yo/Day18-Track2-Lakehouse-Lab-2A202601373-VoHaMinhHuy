5. Kiểm tra kết quả
Chạy các lệnh nghiệm thu chất lượng sau trên Terminal:

# 1. Chạy bộ 22 unit tests của hệ thống
make test
Copy
Kỳ vọng: 22 passed in ~1.0s.
# 2. Chạy tự động toàn bộ 8 notebooks ở chế độ headless (cổng chấm điểm)
make run-all
Copy
Kỳ vọng: Cả 8 notebooks chạy hoàn tất từ đầu đến cuối mà không có lỗi assert nào.
Các lỗi thường gặp & Cách xử lý:
AttributeError: 'DeltaTable' object has no attribute 'files': Bạn đang dùng deltalake 0.x cũ $\rightarrow$ Chạy make clean && make setup để dùng bản 1.x (file_uris()).
No function matches array_cosine_similarity(FLOAT[], ...): Thiếu cast kiểu trong DuckDB $\rightarrow$ Ép kiểu cố định emb::FLOAT[256].
NB2 Speedup < 3×: Bình thường trên một số dòng máy Mac do NVMe SSD/RAM caching quá nhanh $\rightarrow$ Rubric tự động chấp nhận chỉ số thay thế Files-pruned ratio $\ge 10\times$.
