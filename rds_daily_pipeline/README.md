# Daily RDS → S3 pipeline

Để chạy từng bước trong Jupyter, mở `rds_daily_pipeline/run.ipynb`. Notebook gọi các hàm
trong `deploy.py`; code runtime vẫn nằm trong `.py` vì AWS Lambda và Glue không chạy trực
tiếp file notebook.

Pipeline DMS trong `dms/` vẫn giữ nguyên vai trò **initial bootstrap**. Pipeline này xử lý
phần dữ liệu mới đủ tuổi archive mỗi ngày:

```text
EventBridge daily
  → Step Functions
    → Lambda chuẩn bị window (watermark, now - retention]
    → Glue đọc PostgreSQL qua JDBC
       → S3 raw theo year/month/day của DATE_COLUMN
       → normalize/publish S3 curated Parquet (parent + child tables)
    → Glue Crawler cập nhật Data Catalog
    → Glue purge job: verify archive rồi xóa đúng window đó khỏi RDS
    → Lambda commit watermark
```

## Vì sao daily không restart DMS full-load?

`reload-target` chạy lại full-load và không cung cấp transaction exactly-once với S3.
AWS cũng cảnh báo S3 target dùng `TargetTablePrepMode=DO_NOTHING` có thể tạo duplicate khi
task dừng/restart không sạch. Glue JDBC phù hợp hơn với batch archival vì query được chính
xác một window không chồng lấp.

Pipeline dùng điều kiện:

```sql
DATE_COLUMN > watermark
AND DATE_COLUMN <= CURRENT_TIMESTAMP - retention
```

Ranh giới trái là `>` và phải là `<=`; nhờ đó hai ngày liên tiếp không có gap hoặc overlap.
Cutoff được chuẩn hóa về `00:00:00 UTC`, nên mỗi lần ghi chứa trọn ngày. Điều này cần thiết
để dynamic partition overwrite không xóa một phần khác của cùng day partition.

## Bootstrap watermark tự động

Copy `.env.example` thành `.env`, rồi điền các giá trị thật. Không nhập watermark thủ công.
Khai báo prefix của DMS initial:

```text
DMS_PREFIX=orders-initial
```

Trong lần `setup()` đầu tiên, code:

1. Đọc `<DMS_PREFIX>-task` từ AWS DMS.
2. Yêu cầu task đã `stopped` thành công và không có table error.
3. Parse `TableMappings` để lấy cutoff `lte` thật của `DATE_COLUMN`.
4. Đọc Glue bootstrap job `<DMS_PREFIX>-partition-initial`.
5. Xác nhận `--TARGET_PATH` của initial đúng bằng curated path của daily.
6. Seed watermark vào `<PIPELINE_NAME>-control` đúng một lần.

Ví dụ DMS mapping chứa `created_at_utc <= 2026-05-18`, watermark được seed tự động thành
`2026-05-18T00:00:00Z`. Những lần setup sau giữ watermark đang vận hành, không reset nó.

Initial và daily cùng ghi dữ liệu đã xử lý vào:

```text
s3://<S3_BUCKET>/<CURATED_PREFIX>/year=YYYY/month=MM/day=DD/
```

Raw landing vẫn tách riêng có chủ ý: DMS raw và Glue daily raw có lifecycle/format khác nhau.
Không trộn hai loại raw vào một prefix; query/Athena sử dụng curated chung.

Glue chạy trong private subnet. Mặc định setup tự lấy subnet và security groups từ
`<DMS_PREFIX>-instance`, vì network đó đã kết nối được tới RDS. `SUBNET_ID` và
`SECURITY_GROUP_IDS` chỉ là optional overrides; có thể để trống. Security group của RDS
phải cho phép inbound PostgreSQL port `5432` từ security group được chọn. Subnet cũng cần
route tới S3/AWS APIs; S3 Gateway Endpoint do notebook DMS tạo có thể dùng lại nếu cùng
route table.

## Deploy

```powershell
cd rds_daily_pipeline
Copy-Item .env.example .env
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
python deploy.py
```

Linux:

```bash
cd rds_daily_pipeline
cp .env.example .env
python -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
python deploy.py
```

Sau khi deploy, chạy thử thủ công trong Step Functions với state machine
`<PIPELINE_NAME>-workflow`. Schedule mặc định bị tắt bởi `ENABLE_SCHEDULE=false`. Sau khi
execution đầu tiên thành công, đổi thành `true` và chạy lại `python deploy.py`.

Teardown bằng cell cuối notebook hoặc `deploy.destroy()`. Hàm này dừng execution/job đang
chạy và xóa schedule, workflow, Lambda, Glue jobs (archive + purge), crawler, connection,
control table và IAM roles. Nó giữ nguyên RDS, toàn bộ S3 data, Glue database và catalog
tables.

## Purge: xóa dữ liệu khỏi RDS sau khi archive

Archive mà không xóa thì RDS không hề nhỏ lại. Bước purge nằm trong cùng workflow, ngay
giữa crawler và commit watermark, và chạy bởi Glue job `<PIPELINE_NAME>-purge-source`.

Purge dùng lại Glue Connection của archive job, nên nó vào được private subnet và dùng
JDBC driver PostgreSQL có sẵn trong Glue 4.0. Không cần Lambda trong VPC, không cần
`pip install` trong subnet không có NAT.

Thứ tự bắt buộc là **archive → verify → purge**. Job tự fail nếu verify không đạt:

1. Đọc key (và `PURGE_CHECKSUM_COLUMN` nếu có) của window từ RDS.
2. Đọc curated Parquet của đúng các day partition thuộc window đó.
3. Anti-join: mọi key trong RDS phải có trong archive. Thiếu một key là fail.
4. Checksum trong RDS không được lớn hơn trong archive.
5. Mỗi child table trong `PURGE_CHILD_TABLES` phải archive đủ số row của window.
6. Chỉ khi tất cả PASSED mới xóa, child trước parent, mỗi batch một transaction.

Bật purge trong `.env`:

```text
ENABLE_PURGE=false     # true để chèn state PurgeSource vào workflow
PURGE_DRY_RUN=true     # true: chỉ verify và in số row sẽ xóa
PURGE_CHILD_TABLES=order_items:order_id
PURGE_CHECKSUM_COLUMN=amount
PURGE_BATCH_SIZE=500
PURGE_VACUUM=false
```

Quy trình lên production nên đi ba bước, mỗi bước chạy lại `deploy.setup()`:

1. `ENABLE_PURGE=false` — chạy vài ngày, xác nhận curated data đúng.
2. `ENABLE_PURGE=true` + `PURGE_DRY_RUN=true` — log của Glue job in `Verify: PASSED`
   và số row sẽ xóa. RDS chưa mất gì.
3. `PURGE_DRY_RUN=false` — bắt đầu xóa thật.

Child table được archive cùng window với parent, bằng một join trên `PRIMARY_KEY` và
partition theo `DATE_COLUMN` của parent. Nhờ vậy `order_items` nằm ở
`curated/rds/order_items/year=.../month=.../day=.../`, cùng cấp với `curated/rds/orders/`,
và crawler catalog cả hai. Nếu không archive child, purge sẽ xóa child theo foreign key
trong khi S3 chưa có bản sao — đó là lý do hai việc này nằm chung một pipeline.

`PURGE_VACUUM=true` chạy `VACUUM (ANALYZE)` sau khi xóa. Việc này giảm bloat và refresh
statistics, nhưng allocated storage của RDS instance vẫn không tự thu nhỏ.

`DATE_COLUMN` phải có index. Không có index thì mỗi batch DELETE là một sequential scan.

## Retry và idempotency

- Raw và curated có layout `year=YYYY/month=MM/day=DD` theo `DATE_COLUMN`.
- Dynamic partition overwrite khiến retry chỉ thay những ngày có trong window hiện tại.
- Watermark chỉ commit sau khi Glue job, crawler và purge thành công.
- Nếu Glue lỗi, execution fail và watermark giữ nguyên; lần chạy lại xử lý cùng window.
- Purge cũng idempotent: chạy lại window đã xóa thì bước verify thấy `0 row` trong RDS và
  job thoát sớm, không xóa lan sang window khác.
- Nếu purge fail giữa các batch, các batch đã commit vẫn giữ, window được verify lại từ
  phần còn lại ở lần chạy sau.

Đây là **at-least-once orchestration + idempotent output**, dễ hiểu và an toàn hơn việc cố
giả lập exactly-once bằng cách restart DMS.

## Schema và hiệu năng

Job dùng `SELECT *` để giữ schema RDS trong bản demo. Với bảng lớn, nên:

1. Chỉ select các cột cần archive.
2. Bảo đảm `DATE_COLUMN` có index; nếu không PostgreSQL phải sequential scan mỗi ngày.
   `database/RDS/schema.sql` chỉ có `idx_orders_status_closed (status, closed_at_utc)`.
   Index đó dẫn đầu bằng `status` nên một predicate chỉ trên `closed_at_utc` không dùng
   được nó. Khi lên volume thật, thêm index trên đúng cột đang cấu hình:

   ```sql
   CREATE INDEX idx_orders_closed_at ON orders (closed_at_utc);
   -- hoặc, nếu DATE_COLUMN=created_at_utc:
   CREATE INDEX idx_orders_created_at ON orders (created_at_utc);
   ```

   Index này phục vụ cả archive query lẫn vòng lặp DELETE của purge.
3. Chỉ archive trạng thái terminal. `DATE_COLUMN=closed_at_utc` tự nhiên thỏa điều này:
   order còn `OPEN` có `closed_at_utc IS NULL` nên bị loại khỏi window.
   `DATE_COLUMN=created_at_utc` thì khác: nó archive mọi order đủ tuổi, kể cả order vẫn
   đang `OPEN` và còn thay đổi. Khi bật purge, những row đó bị xóa khỏi RDS trong lúc
   nghiệp vụ chưa xong. Nếu vẫn muốn dùng `created_at_utc`, hãy thêm điều kiện trạng thái
   terminal vào query của `glue_job.py` và vào `window_sql` của `purge_job.py` — hai chỗ
   phải giống nhau, nếu không verify sẽ báo thiếu key.
4. Sau khi volume lớn, thêm JDBC partitioning theo primary key để Glue đọc song song.

Crawler tạo tên table từ folder S3 và thêm prefix trong `GLUE_TABLE`. Hãy kiểm tra tên thực
tế ở Glue Data Catalog sau lần chạy đầu tiên trước khi viết Athena query production.

## Bảo mật

Bản demo lưu credential trong Glue Connection, được AWS Glue mã hóa at rest nhưng vẫn được
deploy từ `.env`. Production nên chuyển password sang Secrets Manager, rotate credential và
không commit `.env`. Glue role, bucket policy và KMS key cũng cần thu hẹp theo đúng prefix.

## Lỗi thường gặp

- Không tìm thấy DMS task: kiểm tra `DMS_PREFIX` và AWS Region.
- Curated path mismatch: đặt `S3_BUCKET`/`CURATED_PREFIX` daily giống DMS bootstrap rồi deploy lại.
- `Connection timed out`: Glue subnet/SG/NACL không tới được RDS.
- `The role defined for the function cannot be assumed by Lambda`: IAM trust chưa propagate.
  Setup luôn repair principal `lambda.amazonaws.com` và retry `CreateFunction` tối đa 120 giây.
- `Could not find driver`: dùng Glue 4.0 như cấu hình, không đổi sang Python Shell job.
- Job chạy nhưng `0 rows`: chưa có record nào vừa đủ retention hoặc chọn sai `DATE_COLUMN`.
- Purge fail `key trong RDS chưa có trong archive`: archive job của window đó chưa ghi đủ.
  Chạy lại execution; đừng tắt verify.
- Purge fail `Child ... chưa archive đủ`: kiểm tra `PURGE_CHILD_TABLES` và `PRIMARY_KEY`
  có khớp foreign key thật không.
- `Missing a real value for PRIMARY_KEY`: `.env` cũ chưa có biến này; copy thêm từ
  `.env.example`.
- Cron lệch giờ: EventBridge cron dùng UTC; `02:00 UTC` là `09:00` Bangkok.
