6. Nộp bài
Tạo thư mục nộp bài và các artefacts theo yêu cầu của Rubric:

Xuất cấu trúc thư mục và commit log:
mkdir -p submission/screenshots
python3 -c "import os; [print(f'{root}/{f}') for root, dirs, files in os.walk('_lakehouse') for f in files]" > submission/screenshots/lakehouse_tree.txt
cat _lakehouse/scratch/users_delta/_delta_log/00000000000000000000.json > submission/screenshots/delta_log_sample.json
Copy
Soạn thảo submission/REFLECTION.md ($\le 200$ từ): Phân tích trong 5 Anti-Patterns của Lakehouse, hệ thống dữ liệu của team bạn dễ mắc phải lỗi nào nhất (ví dụ: Bỏ qua OPTIMIZE dẫn đến Small-Files Problem do streaming ingestion) và nêu rõ giải pháp khắc phục.
Mở Pull Request: Commit 8 notebooks đã chạy (lưu nguyên output), thư mục submission/ và tạo PR về repo gốc với tiêu đề: [NXX] Lab18 — <Họ Tên>.
