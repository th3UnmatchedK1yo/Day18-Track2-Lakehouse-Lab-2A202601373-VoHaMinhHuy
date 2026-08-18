# Topic C — CDC từ Ride-Hailing Việt Nam → Lakehouse

**Architecture brief**  
**Vai trò:** Architect on-call  
**Deliverable:** `submission/bonus/ARCHITECTURE.md`

---

## 1. Problem statement

Hệ thống production dùng Oracle làm transactional source of truth cho một nền tảng ride-hailing tại Việt Nam, quy mô khoảng **100 triệu chuyến/năm** và **30K writes/giây ở peak**. Lakehouse phải nhận committed changes qua **Debezium CDC**, phục vụ dashboard trong **≤ 60 giây từ source commit** và ad-hoc analytics với **p95 < 1 giây**. Dữ liệu đến muộn là bình thường do thiết bị có thể mất mạng; do đó arrival order không thể được xem là business order.

Phone, CMND và GPS của tài xế/hành khách được đề bài đặt trong phạm vi PII của **Nghị định 13/2023/NĐ-CP**. Kiến trúc vì vậy phải giảm exposure của raw PII ngay tại ingestion boundary, audit privileged access và cung cấp lineage để truy vết downstream impact.

Bài toán khó vì phải tối ưu đồng thời **freshness, correctness, query latency, replayability và privacy**. Kiến trúc không được đánh đổi correctness để giữ dashboard “xanh”: khi downstream chậm, freshness có thể degrade tạm thời nhưng Oracle state và khả năng replay phải được bảo toàn.

---

## 2. Architecture

```text
                                   ┌──────────────────────┐
                                   │   Oracle OLTP        │
                                   │ system of record     │
                                   └──────────┬───────────┘
                                              │ redo/commit log
                                              ▼
                                   ┌──────────────────────┐
                                   │      Debezium        │
                                   └──────────┬───────────┘
                                              │ CDC
                                              ▼
                                   ┌──────────────────────┐
                                   │        Kafka         │
                                   │ buffer + replay      │
                                   └──────────┬───────────┘
                                              │
                                  secure ingestion boundary
                                              │
                                  tokenize phone / CMND
                                  restrict exact GPS
                                              │
                                              ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│ BRONZE — Delta                                                              │
│ append-heavy CDC history; source_ts; source_sequence; op; ingest_ts;        │
│ tokenized identifiers; restricted location representation                   │
└──────────────────────────────────────┬───────────────────────────────────────┘
                                       │ stream / dedup
                                       │ event-time guarded MERGE
                                       ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│ SILVER — Delta                                                              │
│ canonical trip state; SCD Type 2 dimensions; data-quality invariants;       │
│ event_date partition; clustering on frequent query keys                     │
└──────────────────────────────────────┬───────────────────────────────────────┘
                                       │ Delta CDF
                                       │ incremental aggregation
                                       ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│ GOLD — Delta                                                                │
│ city/5-min metrics; driver KPIs; trip-status metrics; dashboard aggregates  │
└──────────────────────────────┬───────────────────────────────┬───────────────┘
                               │                               │
                               ▼                               ▼
                     Dashboard / BI                    Ad-hoc SQL endpoint
                     freshness ≤60s                    p95 target <1s
                               │                               │
                               └───────────────┬───────────────┘
                                               │
                ┌──────────────────────────────┴─────────────────────────────┐
                │ GOVERNANCE / CONTROL PLANE                                │
                │ central catalog + RBAC/column classification              │
                │ lineage: Oracle → Bronze → Silver → Gold → dashboard      │
                │ PII audit: actor / query / columns / purpose / timestamp  │
                │ schema contracts + quality + freshness monitoring         │
                └────────────────────────────────────────────────────────────┘
```

**Layer semantics:**

- **Bronze = replayable ingestion truth.** Giữ technical history cần để replay và forensic, nhưng không cho raw identifiers lan rộng.
- **Silver = canonical business state.** Dedupe, xử lý late events, giữ temporal history cần thiết.
- **Gold = rebuildable serving state.** Tối ưu cho query pattern và dashboard; nếu Gold sai, rebuild từ Silver.
- **Oracle vẫn là transactional source of truth.** Lakehouse không thay Oracle; nó materialize analytical views của committed changes.

---

## 3. Architecture decisions và alternatives bị loại

### Decision 1 — Chọn log-based CDC: Debezium + Kafka

**Tôi chọn** Oracle redo/transaction logs → Debezium → Kafka → streaming ingestion.

**Tôi loại polling Oracle** (`WHERE updated_at > last_seen`) vì polling tạo analytical read load lên OLTP, khó capture delete sạch, dễ gặp timestamp-boundary duplicate/miss và polling interval ăn trực tiếp vào SLA 60 giây.

**Tôi loại application dual-write** Oracle + Kafka vì nó chuyển consistency problem vào từng producer: Oracle commit có thể thành công trong khi publish sang Kafka thất bại hoặc ngược lại.

Kafka là isolation boundary giữa transactional và analytical workload. Khi downstream chậm, Kafka giữ backlog để Lakehouse catch up mà không biến Oracle thành replay engine.

**Trade-off chấp nhận:** thêm Debezium/Kafka operational complexity để đổi lấy source isolation, buffering và replay.

---

### Decision 2 — Chọn Delta Lake cho Bronze, Silver và Gold

**Tôi chọn Delta Lake** vì workload mutation-heavy: CDC có insert/update/delete, Silver cần `MERGE`, late data cần guarded update, SCD2 cần transactional changes và recovery cần table history.

**Tôi loại plain Parquet** vì columnar files đơn thuần không giải quyết transaction/state-management semantics cho frequent updates và replay.

**Tôi loại Iceberg trong hot path này** không phải vì Iceberg “kém”, mà vì constraint hiện tại ưu tiên CDC/MERGE/change propagation và đề bài yêu cầu áp dụng Delta CDF. Nếu engine neutrality trở thành constraint số một, Iceberg đáng được đánh giá lại.

**Trade-off chấp nhận:** table-format neutrality thấp hơn để giảm integration complexity trong CDC pipeline.

---

### Decision 3 — Tokenize PII tại ingestion boundary; không đợi tới Gold

Phone và CMND được chuyển thành **stable deterministic tokens** trước khi vào normal Bronze. Exact GPS chỉ nằm trong restricted domain khi thật sự cần; normal analytics dùng derived geography như city/district/coarse grid.

**Tôi loại Gold-only masking** vì raw PII khi đó đã tồn tại xuyên Bronze và Silver, làm blast radius của ACL/configuration error lớn hơn.

**Tôi loại naïve unsalted hashing** cho identifiers vì predictable domains như phone number không phải use case tốt cho một hash đơn giản; đồng thời hệ thống có thể cần controlled re-identification cho workflow hợp lệ.

Tokenization/key service nằm trong trust boundary riêng. Normal analyst không có quyền resolve token.

**Trade-off chấp nhận:** thêm key/token service và privileged workflow để đổi lấy giảm exposure và vẫn giữ được joinability.

---

### Decision 4 — Xử lý late data bằng source-time/source-sequence guarded MERGE

Arrival time không phải business order. Silver chỉ nhận update nếu incoming change mới hơn current state theo source metadata.

```sql
MERGE INTO silver_trips AS tgt
USING incoming_cdc AS src
ON tgt.trip_id = src.trip_id

WHEN MATCHED
 AND (
      src.source_ts > tgt.source_ts
      OR (
          src.source_ts = tgt.source_ts
          AND src.source_sequence > tgt.source_sequence
      )
 )
THEN UPDATE SET *

WHEN NOT MATCHED
THEN INSERT *;
```

**Tôi loại arrival-order overwrite** vì một `CREATED` event đến muộn có thể overwrite `COMPLETED`.

**Tôi loại watermark chờ rất lâu trước khi publish** vì global waiting để đạt perfect ordering mâu thuẫn với freshness SLA 60 giây.

**Trade-off chấp nhận:** Silver logic phức tạp hơn nhưng state hội tụ deterministic mà không giữ dashboard lại vô thời hạn.

---

### Decision 5 — Dùng SCD Type 2 có chọn lọc cho temporal dimensions

SCD2 áp dụng cho dimensions cần trả lời “business state tại thời điểm X”, ví dụ driver profile, vehicle assignment hoặc pricing-zone attributes.

```text
driver_id | rating | valid_from | valid_to | is_current
D12       | 4.8    | Jan 01     | Mar 10   | false
D12       | 4.6    | Mar 10     | Jun 15   | false
D12       | 4.9    | Jun 15     | NULL     | true
```

**Tôi loại SCD Type 1** cho các attributes cần historical analysis vì overwrite phá point-in-time reproducibility.

**Tôi loại “chỉ query Bronze CDC”** vì technical event history không phải business-friendly temporal model. Bronze trả lời “DB đã đổi gì”; SCD2 trả lời “state hợp lệ tại thời điểm nào”.

Late SCD changes phải split/rewrite validity windows theo effective/source time, không theo arrival time.

**Trade-off chấp nhận:** thêm rows và ETL complexity để có temporal correctness.

---

### Decision 6 — Partition theo time; cluster high-cardinality keys; ưu tiên Snappy trên hot path

Silver trip tables bắt đầu với **`event_date` partitioning**. Các keys như `trip_id`, `driver_id`, `city_id` hoặc event timestamp là clustering/data-skipping candidates tùy workload đo được.

**Tôi loại partition theo `trip_id`, `driver_id`, `passenger_id`** vì cardinality quá cao, dễ tạo fragmented partitions/small files.

**Tôi loại hourly partitioning làm mặc định** vì 8,760 time partitions/năm/table có thể tạo metadata/file overhead không cần thiết; chỉ refine nếu benchmark chứng minh daily partition quá thô.

Về compression, **tôi chọn Parquet + Snappy cho hot Bronze/Silver/Gold path** vì storage ở scale 100M trips/năm rẻ hơn đáng kể so với always-on compute; ưu tiên ingest/decompression CPU và latency hơn maximal compression.

**Tôi loại gzip cho hot path** vì CPU cost và latency không phù hợp streaming/interactive workload. **ZSTD** có thể được benchmark cho colder immutable datasets, nhưng không phải default trước khi đo.

**Trade-off chấp nhận:** trả thêm một ít storage để giữ CPU budget cho CDC và low-latency queries.

---

### Decision 7 — Gold là incremental serving layer; dùng Delta CDF

Dashboard không full-scan Silver mỗi lần refresh. Silver changes được propagate bằng **Delta Change Data Feed** vào workload-specific Gold tables:

- `gold_city_5m_metrics`
- `gold_driver_5m_metrics`
- `gold_trip_status_metrics`

Ví dụ `gold_city_5m_metrics` giữ `window_start`, `city_id`, `trip_count`, `completed_trip_count`, `gross_booking_value`, `avg_wait_time`.

**Tôi loại dashboard query Bronze** vì UI không nên hiểu Debezium envelope, duplicate/update/delete semantics.

**Tôi không dùng Silver cho mọi fixed dashboard** vì repeated `GROUP BY` trên detailed state lặp lại cùng computation. Silver vẫn phục vụ exploration; Gold nhận các query pattern có strict latency/frequency.

**Trade-off chấp nhận:** thêm derived storage và pipeline stages để có predictable query latency. Gold luôn rebuildable từ Silver.

Đối với **ad-hoc p95 <1s**, SLO được đo trên curated Silver/Gold query suite với bounded filters; unbounded full-table scans không được coi là interactive query. SQL endpoint phải có autoscaling/cache và benchmark bằng workload thật trước production sign-off.

---

### Decision 8 — Central catalog + least privilege + lineage + PII access audit

Cost baseline dùng một **managed central metastore/catalog** (ví dụ AWS Glue Data Catalog trong AWS deployment), thay vì local per-cluster metadata. Lineage events được thu ở job/query boundary và nối thành graph Oracle → Bronze → Silver → Gold → dashboard.

Normal analyst chỉ thấy tokenized identifiers và coarse geography. Privileged identity/GPS access bắt buộc ghi immutable audit event:

```text
actor_id
role
query_id
dataset
columns_accessed
purpose_code
approval_reference
access_timestamp
success
```

**Tôi loại standalone Hive Metastore làm governance boundary** vì metadata local/cluster-centric không đủ tốt cho central policy/audit model của bài này.

**Tôi loại table-level permission là cơ chế duy nhất** vì một table có thể chứa cả business-safe columns và restricted location fields.

**Tôi loại “tokenized rồi nên không cần audit”** vì restricted mappings, exact GPS và re-identification vẫn tồn tại.

**Trade-off chấp nhận:** thêm control-plane/operational overhead để có accountability và blast-radius analysis.

---

### Decision 9 — Lifecycle: replay window hữu hạn, không giữ Bronze vô thời hạn

Đề bài không cho retention duration, vì vậy đây là **operational assumption, không phải legal interpretation**:

- Bronze hot/replay: **90 ngày**
- Silver business history: **13 tháng** cho baseline year-over-year analytics
- Gold: **13 tháng**, nhưng rebuildable
- Retention cuối cùng phải được policy/legal owner phê duyệt; khi policy ngắn hơn, policy thắng architecture default.

**Tôi loại infinite Bronze retention** vì tăng PII/security footprint và storage/history maintenance mà không có explicit requirement.

**Tôi loại Bronze 7 ngày làm mặc định** vì quá hẹp cho forensic replay và delayed detection của data-quality incidents.

**Trade-off chấp nhận:** 90 ngày là recovery window đủ rộng cho operations trong khi giới hạn replay/security footprint.

---

## 4. Failure modes: “03:00 sáng thì cái gì hỏng?”

| Failure | Detection | Recovery / rollback |
|---|---|---|
| **1. Debezium/Kafka lag** làm freshness >60s | `current_time - max(source_commit_ts)`; Kafka consumer lag; Bronze freshness | Scale consumers, consume backlog, resume Silver/Gold. Không rollback dữ liệu vì đây là freshness failure, không phải correctness failure. |
| **2. Bad Silver MERGE deployment** cho late event overwrite state mới | Invalid trip-state regressions; source timestamp regression; abnormal late-event rejection ratio | Stop writer → xác định last-known-good Delta version → restore/time travel → deploy guarded MERGE → replay affected Bronze interval → rebuild Gold. |
| **3. Breaking Oracle schema change** | Schema fingerprint/contract violation; deserialization errors; type/drop/rename alerts | Bronze giữ event nếu envelope vẫn parse được; quarantine incompatible records; update transform/schema contract; replay interval; use lineage để tìm downstream impact. |
| **4. Tokenization/key service failure** | Tokenization error rate, key-version mismatch, quarantine growth, unexpected jump in unique tokens | **Fail closed**: không fallback sang raw PII in normal Bronze. Buffer/quarantine encrypted events, restore correct key/version, reprocess, validate, then release downstream. |
| **5. Bad Gold metric deployment** | Reconciliation/control totals giữa Silver và Gold; sudden KPI variance after deploy | Stop Gold job, restore previous transform, replace affected Gold windows, recompute from Silver/CDF, validate totals, resume dashboards. |

### Failure-mode invariant

**Freshness có thể degrade tạm thời; correctness/privacy không được silently degrade.**

Failure mode #2 trực tiếp dùng **Delta time travel**. Failure mode #3 dùng **schema evolution + lineage**. Gold được thiết kế là disposable derived state để giới hạn blast radius của serving bugs.

---

## 5. Back-of-envelope cost estimate

### 5.1 Assumptions

Scale source cho biết **100M trips/năm** nhưng không cho bytes/record hoặc mutations/trip. Để tránh giả vờ có số production, cost model dùng explicit assumptions:

- 100M trips/year
- 15 CDC mutations/trip trung bình
- 2 KB/event trung bình trước compression
- Bronze hot retention: 90 days
- 30% Bronze overhead cho Delta history/file inefficiency
- Silver + SCD2 footprint: 0.5 TB/year logical
- Silver retention: 13 months + 30% overhead
- Gold aggregates: 0.1 TB
- Cost reference: AWS `us-east-1`-style back-of-envelope
- S3 Standard planning rate: **$0.023/GB-month**[^aws-s3]
- EMR Serverless example rates: **$0.052624/vCPU-hour** + **$0.0057785/GB-hour** memory[^aws-emr]
- 730 hours/month

Peak **30K writes/s** dùng để size throughput/headroom, không được giả định là sustained annual average.

### 5.2 Storage

Raw CDC generated/year:

```text
100,000,000 trips
× 15 mutations/trip
× 2 KB/event
≈ 3.0 TB/year raw CDC
```

Bronze 90-day hot footprint:

```text
3.0 TB/year × 90/365 × 1.30 overhead
≈ 0.96 TB
```

Silver:

```text
0.50 TB/year × 13/12 × 1.30
≈ 0.70 TB
```

Gold:

```text
≈ 0.10 TB
```

Total hot object storage:

```text
0.96 + 0.70 + 0.10
≈ 1.76 TB
```

S3-style storage cost:

```text
1.76 TB × 1,000 GB/TB × $0.023/GB-month
≈ $41/month
```

Storage is not the dominant cost in this design; always-on streaming compute is.

### 5.3 Streaming compute

Conservative initial capacity envelope:

```text
64 vCPU + 256 GB RAM, 24×7
```

Compute:

```text
64 × 730 × $0.052624
+ 256 × 730 × $0.0057785
≈ $3,538/month
```

Batch compaction/reconciliation budget:

```text
32 vCPU + 128 GB RAM
× 4 hours/day
≈ 120 hours/month

32 × 120 × $0.052624
+ 128 × 120 × $0.0057785
≈ $291/month
```

### 5.4 Monthly planning envelope

```text
Hot storage                    ≈ $   41
Streaming compute              ≈ $3,538
Compaction/reconciliation      ≈ $  291
---------------------------------------
Storage + compute subtotal     ≈ $3,870/month
```

Add **30% planning contingency** for request overhead, Kafka/control-plane capacity, monitoring, catalog/KMS and uncertainty:

```text
$3,870 × 1.30
≈ $5,031/month planning envelope
```

This is not a vendor quote. The first production action is to sample real CDC payload size, mutation count and worker utilization; the estimate should be recalibrated after one week of measurements. If average CDC bytes double, storage approximately doubles, while compute changes according to serialization/MERGE pressure rather than linearly by storage alone.

[^aws-s3]: AWS documentation cost example for S3 Standard, checked 2026-08-18; example uses $0.023/GB-month for the first 50 TB in US-East-1.
[^aws-emr]: AWS Amazon EMR Pricing, checked 2026-08-18; EMR Serverless example uses $0.052624/vCPU-hour and $0.0057785/GB-hour.

---

## 6. One-week MVP: smallest shippable vertical slice

Mục tiêu tuần đầu **không phải build toàn lakehouse**. MVP chỉ chứng minh bốn risk lớn nhất: CDC freshness, PII boundary, late-event correctness và rebuildability.

### Scope

**Source → Debezium/Kafka → Bronze Delta → Silver Delta → một Gold table → một query/dashboard.**

Dataset dùng synthetic hoặc sanitized sample, đủ lớn để benchmark nhưng không cần production scale.

### Day-by-day

| Day | Deliverable |
|---|---|
| **1** | Oracle-compatible CDC source + Debezium + Kafka; capture `source_ts`, operation và source sequence. |
| **2** | Secure ingestion + deterministic tokenization cho phone/CMND; write append-heavy Bronze Delta; prove raw identifier không xuất hiện trong normal Bronze. |
| **3** | Silver `MERGE` với source-time/source-sequence guard; tests cho out-of-order `CREATED → COMPLETED → late CREATED`. |
| **4** | Một SCD2 dimension nhỏ + schema/data-quality checks; inject late SCD update và verify validity windows. |
| **5** | Silver CDF → `gold_city_5m_metrics`; dashboard/query benchmark; freshness metric từ source commit đến Gold. |
| **6** | Failure drill: intentionally deploy bad MERGE, detect invariant violation, restore Delta version, replay Bronze và rebuild Gold. |
| **7** | Document results: p50/p95 freshness, p95 query latency, replay time, actual bytes/event, compute utilization và updated cost estimate. |

### MVP acceptance criteria

1. **Freshness:** source commit → Gold visible **<60s p95** under test load.
2. **Correctness:** late event không thể regress canonical trip state.
3. **Privacy:** normal Bronze/Silver path chứa token, không chứa raw phone/CMND.
4. **Recovery:** bad Silver write có thể rollback/replay mà không đọc lại full Oracle table.
5. **Serving:** chosen Gold query đạt **p95 <1s** trên declared benchmark dataset/query suite.
6. **Observability:** có ít nhất freshness, consumer lag, late-event count và data-quality failure metrics.

---

## 7. Design review summary

Architecture này ưu tiên một principle xuyên suốt:

> **Capture committed truth → protect sensitive data → preserve replayability → converge deterministic business state → materialize low-latency serving views.**

Tôi không chọn Debezium, Delta, SCD2 hay Gold vì chúng “tốt” một cách tuyệt đối. Mỗi lựa chọn giải một constraint cụ thể:

- **Debezium + Kafka** tách OLTP khỏi analytics và tạo replay buffer.
- **Delta** cung cấp mutation/recovery semantics phù hợp CDC.
- **Tokenization sớm** giảm PII blast radius.
- **Source-time MERGE** giải late-arriving data mà không phá freshness SLA.
- **SCD2** biến technical CDC history thành point-in-time business history.
- **Time partition + clustering** tránh high-cardinality partition explosion.
- **Incremental Gold + CDF** đổi thêm derived state lấy predictable latency.
- **Catalog + audit + lineage** làm privacy/accountability thành platform concern.
- **Finite lifecycle** giữ recovery value mà không giữ technical history vô hạn.

Điểm quan trọng nhất khi hệ thống hỏng lúc 03:00: **Oracle vẫn đúng, Bronze vẫn replayable, Silver có rollback path, Gold rebuildable, và PII boundary không fail open.**
