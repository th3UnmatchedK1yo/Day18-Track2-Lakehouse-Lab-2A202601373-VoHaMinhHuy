# 4. Mục tiêu đạt ra

## Kiến thức Lakehouse đã nắm được

### Part A — Foundations (NB1–NB4)
- **Delta Lake ACID**: Hiểu transaction log (`_delta_log/`), schema enforcement tự động chặn schema sai, và schema evolution qua `schema_mode="merge"`
- **OPTIMIZE + Z-ORDER**: Hiểu bản chất small-file problem — không phải lỗi code mà là tích lũy từ streaming. Z-ORDER giúp file-skipping qua min/max stats, đạt pruning ≥ 10×
- **Time Travel**: `versionAsOf`, MERGE upsert, RESTORE rollback — mọi thao tác đều là transaction mới, fully auditable qua `history()`
- **Medallion Architecture**: Bronze (raw) → Silver (clean, dedup) → Gold (aggregate) — pipeline LLM observability thực tế

### Part B — Lakehouse 2026 (NB5–NB8)
- **Iceberg + Catalog**: Catalog là control plane, không chỉ name→path lookup. Hidden partitioning loại bỏ hoàn toàn lỗi quên partition predicate. Schema evolution theo field_id, không phải name/position
- **4 Job Maintenance bắt buộc**: Compaction, Clustering, Snapshot Expiry, Orphan Removal — và phát hiện quan trọng: `VACUUM` không dọn orphan chưa commit, `expire_snapshots` chỉ là metadata-only
- **Vectors & Multimodal**: Embedding trong bảng, int8 quantization 4× nhỏ hơn với recall@10 ≥ 0.90. Lifecycle bug: external vector index không sync deletes → compliance violation
- **Agents & Provenance**: Trajectory qua medallion, version pin cho training reproducibility, MCP 2026-07-28 protocol shape, EU AI Act Art. 10 — 4 provenance buckets thành partition key

## Bài học quan trọng nhất
> **Lakehouse anti-patterns không đến từ code sai — chúng đến từ việc thiếu maintenance jobs.** Mỗi commit đều đúng, nhưng tích lũy tạo ra small files, metadata bloat, và orphan files. Giải pháp là cron jobs, không phải refactor.
