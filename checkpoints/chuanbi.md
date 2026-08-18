# 1. Chuẩn bị

## Môi trường
- **OS:** Windows 11 + Python 3.12.5
- **Venv:** Tạo tại `C:\lab18venv` (đường dẫn ngắn để tránh lỗi Windows Long Path)
- **Dependencies:** `pip install -r requirements.txt` — cài thành công toàn bộ packages: deltalake 1.6.2, pyiceberg 0.11.1, duckdb 1.5.5, polars 1.43.2, pyarrow 25.0.1, numpy 2.5.2, jupyterlab 4.6.3, jupytext 1.19.5, pytest 9.1.1

## Smoke Test
- Chạy `scripts/verify_lite.py` → **9/9 checks PASS**:
  - ✓ delta write + read
  - ✓ delta time travel + history
  - ✓ delta maintenance (compact / vacuum)
  - ✓ delta change data feed
  - ✓ iceberg catalog + append
  - ✓ iceberg scan planning prunes 5 → 1 files
  - ✓ iceberg maintenance API present
  - ✓ duckdb vector search (core, offline)
  - ✓ duckdb ↔ delta via arrow (no extension download)

## Data Generation
- `generate_data_lite.py` → 200,000 rows Bronze (190,052 unique + 9,948 duplicates), 7 UTC days
- `generate_ai_data.py` → 2,000 docs (dim=256), 200 blob files (12.5 MB), 1,578 agent trajectory steps / 300 sessions
