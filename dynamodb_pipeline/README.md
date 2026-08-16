# DynamoDB → S3 → Glue daily pipeline

Để học và chạy từng bước, mở `dynamodb_pipeline/run.ipynb`. Notebook là giao diện điều
khiển; các file `.py` giữ implementation để dễ test, reuse trong CI/CD và tránh duplicate
logic giữa notebook với runtime Lambda/Glue.

Pipeline này bổ sung luồng riêng cho DynamoDB, không sửa pipeline DMS hiện tại:

```text
EventBridge (daily)
  → Step Functions
    → Lambda: full export lần đầu / incremental export các lần sau
    → chờ DynamoDB export hoàn tất
    → Glue: DynamoDB JSON → Parquet
    → cập nhật watermark khi Glue thành công
  → S3 curated → Glue Data Catalog → Athena
```

## Tại sao không dùng DMS?

DynamoDB có native export từ PITR sang S3. Export chạy bất đồng bộ, không scan bảng và
không tiêu thụ RCU. DMS phù hợp với RDS hiện tại, nhưng thêm DMS cho DynamoDB làm pipeline
phức tạp và tốn tài nguyên hơn mà không có lợi trong bài toán batch hằng ngày.

Step Functions chờ export xong rồi mới gọi Glue. Không nên tạo hai cron độc lập vì thời
gian export không cố định; Glue có thể đọc một export còn dang dở.

## Chuẩn bị

- Bảng DynamoDB đã bật PITR (`database/Dynamo/run.ipynb` hiện đã làm việc này).
- S3 bucket đã tồn tại và ở cùng Region.
- AWS identity dùng để deploy có quyền tạo IAM role, Lambda, Step Functions, EventBridge,
  Glue job/catalog và DynamoDB control table.
- Python 3.11+ và AWS credentials đã cấu hình bằng AWS CLI/profile.

```powershell
cd dynamodb_pipeline
Copy-Item .env.example .env
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
python deploy.py
```

Trên Linux, thay lệnh activate bằng `source .venv/bin/activate`.

`setup()` trong `deploy.py` có thể chạy lại: code cập nhật Lambda, Glue job, state machine,
IAM inline policies và schedule thay vì tạo bản sao.

Teardown pipeline bằng notebook hoặc:

```python
from deploy import destroy
destroy()
```

Hàm này xóa orchestration/compute, control table và Glue Catalog table do pipeline tạo;
không xóa DynamoDB source table, S3 bucket hoặc object raw/curated.

## Chạy thử ngay

Sau khi deploy, mở AWS Console → Step Functions → `<PIPELINE_NAME>-daily` → Start execution.
Lần đầu tạo **full export** tại một point-in-time. Chỉ khi Glue thành công, control table
`<PIPELINE_NAME>-control` mới lưu watermark. Lần sau pipeline dùng incremental export từ
watermark đó đến `now - 5 phút`.

Các prefix:

- Raw: `s3://<bucket>/<RAW_PREFIX>/AWSDynamoDB/<export-id>/data/`
- Curated: `s3://<bucket>/<CURATED_PREFIX>/year=YYYY/month=MM/day=DD/`
- Glue/Athena: `<GLUE_DATABASE>.<GLUE_TABLE>`

Ví dụ Athena:

```sql
SELECT status, COUNT(*) AS total_orders, SUM(amount) AS total_amount
FROM archive.orders
WHERE year = '2025' AND month = '01'
GROUP BY status;
```

## Semantics và giới hạn cần hiểu

Job hiện là **append-only**, phù hợp với order đã đóng và không còn thay đổi. Incremental
export lấy `NewImage`; record delete không được ghi vào curated. Nếu item cũ vẫn bị update
hoặc delete, Parquet append có thể có nhiều version của cùng key. Production nên chuyển
curated table sang Apache Iceberg và dùng `MERGE`/tombstone, thay vì cố deduplicate trong
một batch đơn lẻ.

Schema trong `glue_job.py` đang explicit theo bảng demo (`order_id`, `created_at`, ...).
Đây là chủ ý: schema rõ ràng giúp phát hiện schema drift sớm. Nếu bảng thật có field khác,
sửa `normalize_orders()` và Glue Catalog schema trước khi deploy.

## Lỗi thường gặp

- `PointInTimeRecoveryStatus != ENABLED`: đợi PITR bật hoàn tất rồi deploy/chạy lại.
- `AccessDenied` khi export: kiểm tra Lambda role, bucket policy và KMS policy nếu bucket
  dùng SSE-KMS.
- `The role defined for the function cannot be assumed by Lambda`: setup sẽ repair trust
  principal `lambda.amazonaws.com` và retry tạo function tối đa 120 giây.
- Glue không thấy dữ liệu: kiểm tra state `ExportStatus=COMPLETED` và đúng export-specific
  `SOURCE_PATH`; không trỏ Glue vào cả raw prefix vì sẽ xử lý lại batch cũ.
- Sai giờ chạy: EventBridge cron dùng **UTC**, không dùng Asia/Bangkok. Mặc định `01:00 UTC`
  tương ứng `08:00` tại Bangkok.
- Không cập nhật watermark thủ công sau lỗi. Workflow cố ý chỉ commit sau khi Glue thành công,
  vì commit sớm sẽ tạo khoảng trống dữ liệu khi retry.
