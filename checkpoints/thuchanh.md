. Thực hành
CP0: Setup & Data

CP1: Delta Basics

CP2: OPTIMIZE & Z-ORDER

CP3: Time Travel & MERGE

CP4: Medallion Pipeline

CP5: Iceberg Catalog

CP6: 5 Maintenance Jobs

CP7: Vectors & Quantization

CP8: Agent Provenance

CP9: Test & Submit

Checkpoint 1: Delta Lake Basics & Transaction Log (01_delta_basics.py)
Thao tác:
Ghi bảng Delta đầu tiên với mode="overwrite" vào _lakehouse/scratch/users_delta.
Xem file log JSON tại _delta_log/00000000000000000000.json và in lịch sử bằng dt.history().
Cố tình ghi dữ liệu sai kiểu (age="thirty") để kiểm chứng Schema Enforcement chặn ghi lỗi.
Mở rộng bảng với cột mới tier="premium" bằng cờ schema_mode="merge".
Đăng ký bảng qua Arrow vào DuckDB in-memory và thực thi truy vấn SQL nhóm theo tier.
Kết quả mong đợi: Khối assert cuối notebook in ra NB1 complete., nhận diện đúng 2 nhóm tier.
Checkpoint 2: Small-Files Problem & OPTIMIZE + Z-ORDER (02_optimize_zorder.py)
Thao tác:
Ghi 200 micro-batches liên tục tạo ra $\ge 100$ files Parquet nhỏ.
Benchmark tốc độ truy vấn point-query (user_id = 4242) khi chưa tối ưu (BEFORE).
Chạy dt.optimize.compact(target_size=256*1024) gom file và dt.optimize.z_order(["user_id"]).
Benchmark lại tốc độ sau tối ưu (AFTER), đo Speedup.
Đọc minValues/maxValues của user_id trong file commit log mới nhất để đo Files-pruned ratio.
Kết quả mong đợi: Đạt Speedup $\ge 3\times$ HOẶC Files-pruned ratio $\ge 10\times$, assert NB2 complete..
Checkpoint 3: Time Travel, ACID MERGE & Rollback (03_time_travel.py)
** Link repo: ** https://github.com/VinUni-AI20k/Day18-Track2-Lakehouse-Lab

Thao tác:
Tạo lịch sử gồm v0 (100K rows) $\rightarrow$ v1 (thêm cột tier) $\rightarrow$ v2 (thực thi ACID MERGE INTO 100K rows) $\rightarrow$ v3 (inject dữ liệu lỗi score = -1).
Sử dụng dt.history() kiểm tra Audit trail.
Thực hiện truy vấn Time-travel tại phiên bản v0 và v1.
Khôi phục bảng về trạng thái sạch bằng dt.restore(2) (tạo thành commit mới v4).
Quét bảng kiểm tra số lượng bản ghi lỗi score < 0 quay về chính xác bằng 0.
Kết quả mong đợi: history() có $\ge 5$ phiên bản (bao gồm dòng RESTORE), assert NB3 complete..
Checkpoint 4: Medallion Architecture cho AI Observability (04_medallion.py)
Thao tác:
Đọc dữ liệu thô lượt gọi LLM từ tầng Bronze (llm_calls_raw).
Dùng DuckDB SQL trích xuất JSON, chuẩn hoá kiểu dữ liệu, khử trùng lặp qua ROW_NUMBER() OVER (PARTITION BY request_id ORDER BY ts), ghi xuống tầng silver phân vùng theo date.
Tổng hợp sang tầng gold kết hợp bảng giá token: tính p50_latency_ms, p95_latency_ms, total_prompt_tokens, cost_usd, error_rate, tối ưu z_order(["model"]).
Xác nhận số dòng Silver < Bronze và Gold bao quát $\ge 7$ ngày $\times$ 3 models.
Kết quả mong đợi: Assert pass toàn bộ 4 điều kiện, in ra NB4 complete..
Checkpoint 5: Apache Iceberg & Catalog as Control Plane (05_iceberg_catalog.py)
Thao tác:
Tạo bảng lake.llm_events thông qua Catalog API độc lập (CAT="nb5").
Gán phân vùng ẩn days(ts) qua DayTransform() và append 10 ngày dữ liệu.
Quét bảng lọc trên cột timestamp gốc ts, dùng plan_files() chứng minh Pruning ratio $\ge 5\times$.
Khám phá cây metadata 3 tầng (metadata.json $\rightarrow$ manifest list $\rightarrow$ manifest $\rightarrow$ data files) và tính tỷ lệ dung lượng.
Đổi tên cột latency_ms $\rightarrow$ latency_millis (chứng minh giữ nguyên field_id = 4).
Tiến hoá phân vùng (thêm model vào partition spec) và chứng minh $\ge 2$ partition spec cùng đọc bình thường.
Kết quả mong đợi: Assert kiểm tra đạt đủ 5 tiêu chí, in ra NB5 complete..
Checkpoint 6: Five Table Maintenance Jobs (06_maintenance.py)
Thao tác:
Ghi 200 micro-batches để tái hiện bảng bị phân mảnh.
Job 1 (Compaction): Chạy compact() giảm số lượng file $\ge 10\times$.
Job 2 (Clustering): Chạy z_order(["user_id"]), chứng minh bỏ qua $\ge 50%$ số file khi point-query.
Job 3 (Expiry): Delta chạy vacuum() thu hồi bytes; Iceberg chạy expire_snapshots() về 3 snapshots.
Job 4 (Orphan Removal): Tạo 3 file rác uncommitted, dùng thuật toán hiệu tập hợp ($Disk \setminus Log$) để quét và xoá sạch; dọn manifest list mồ côi trên Iceberg.
Job 5 (Checkpointing): Tạo file *.checkpoint.parquet và cập nhật _last_checkpoint.
Kết quả mong đợi: Toàn bộ 9 kiểm thử bảo trì đều [PASS], in ra NB6 complete..
Checkpoint 7: Vectors, Multimodal & Lifecycle Traps (07_vectors_multimodal.py)
Thao tác:
So sánh lưu trữ 200 frame media: inline blob vs pointer URI, đo đạc Random-access amplification $\ge 5\times$ do Row Group của Parquet gây ra.
Lượng tử hoá mảng embedding sang int8, chứng minh giảm dung lượng $\ge 3\times$ trên đĩa.
Đo lường Recall@10 ($\ge 0.80$) và Topic Fidelity ($\ge 0.95$) của vector int8.
Chạy Semantic Search trực tiếp bằng DuckDB SQL với hàm array_cosine_similarity().
Tái hiện Lifecycle Bug: Xoá user trong Lakehouse nhưng External Vector Index vẫn còn lưu vết (0 hits in-table vs >0 hits in external index).
Sử dụng Change Data Feed (CDF) để bắt trọn gói sự kiện delete.
Kết quả mong đợi: Toàn bộ 7 điều kiện kiểm thử đều đạt, in ra NB7 complete..
Checkpoint 8: Agent Trajectories & EU AI Act Provenance (08_agents_provenance.py)
Thao tác:
Đưa agent traces qua Medallion: Silver phân vùng theo agent_version, Gold tổng hợp theo từng policy.
Ghim phiên bản bảng Delta (table_version) vào metadata của Training Run; replay lại xác nhận khớp $100%$.
Mô phỏng Catalog MCP Protocol: cache tools/list (5 turns $\rightarrow$ 1 catalog read), cơ chế input_required trước destructive calls, và async task polling.
Phân loại toàn bộ dữ liệu huấn luyện vào 4 phân vùng bản quyền theo EU AI Act Art. 10 (licensed, public_domain, scraped_optout_checked, synthetic) và cách ly toàn bộ dòng UNCLASSIFIED.
Thực thi quyền xoá dữ liệu cá nhân (Right-to-erasure) của user_007.
Kết quả mong đợi: Cả 10 kiểm thử đều [PASS], in ra NB8 complete..