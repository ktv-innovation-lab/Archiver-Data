# Athena query module

Module này setup lớp query sau khi dữ liệu đã được Glue ghi thành Parquet và partition theo:

```text
year=YYYY/month=MM/day=DD
```

Athena không cần copy dữ liệu và cũng không cần tạo một schema riêng. Nó query trực tiếp table
trong AWS Glue Data Catalog. Vì vậy trách nhiệm được tách rõ:

```text
Glue job/crawler -> schema + partition metadata -> Glue Data Catalog
Athena workgroup -> chạy SQL + lưu query results + giới hạn chi phí
```

## Module tạo những gì?

- Một Athena SQL workgroup riêng.
- S3 output location cố định cho query results.
- SSE-S3 encryption cho query results.
- `ExpectedBucketOwner` để tránh ghi nhầm sang bucket trùng tên ở account khác.
- Giới hạn số byte được scan trên mỗi query.
- CloudWatch metrics cho workgroup.
- Bốn saved queries mẫu cho RDS và DynamoDB.

Module **không tạo hoặc sửa Glue table**. Nếu table chưa tồn tại, setup dừng lại và yêu cầu chạy
partition job/crawler trước. Điều này tránh có hai nơi cùng quản lý schema.

## Chuẩn bị

1. DMS bootstrap partition job và crawler đã thành công.
2. RDS daily crawler đã tạo/cập nhật table, hoặc table bootstrap RDS vẫn còn trong Catalog.
3. DynamoDB pipeline đã tạo Glue table.
4. S3 bucket và Glue database nằm cùng Region.
5. AWS identity có quyền với Athena workgroup/named query, đọc Glue Catalog và đọc/ghi prefix
   Athena results trên S3.

Lấy **đúng tên table** tại AWS Console -> Glue -> Data Catalog -> Tables. Crawler có thể thêm
prefix vào tên table, nên không nên đoán tên từ folder S3.

## Cài đặt

PowerShell:

```powershell
cd athena
Copy-Item .env.example .env
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
```

Linux:

```bash
cd athena
cp .env.example .env
python -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
```

Sửa `.env` và điền bucket cùng tên Glue table thực tế. Không commit `.env`.

Liệt kê tên table thật do crawler tạo:

```bash
python setup.py discover
```

Output bắt đầu bằng AWS Account ID, Region và danh sách database. Nếu không có `archive`, hãy
đối chiếu account/Region với notebook partition. Không tạo database rỗng bằng tay để né lỗi: Athena
vẫn không có table/schema để query. Cần chạy thành công Glue partition job và crawler ở đúng
account/Region.

Crawler ghép `TablePrefix=orders_rds_` với folder cuối là `orders`, nên với cấu hình hiện tại tên
thường là `orders_rds_orders`, không phải `orders_rds`. Hãy đối chiếu **S3 location** trong output: table RDS phải trỏ vào
`curated/rds/orders`, còn table DynamoDB phải trỏ vào `curated/dynamodb/orders`.

## Setup và kiểm tra

```bash
python setup.py setup
python setup.py status
python setup.py test-rds
python setup.py test-ddb
```

`setup` là idempotent: chạy lại sẽ update cùng workgroup và thay bốn saved query do module quản
lý, không tạo workgroup trùng.

`test-rds` và `test-ddb` chạy `SELECT * ... LIMIT 10`. Đây chỉ là smoke test; `LIMIT` giới hạn số
row trả về nhưng không đảm bảo luôn scan ít dữ liệu. Workgroup vẫn chặn query khi vượt
`BYTES_SCAN_CUTOFF_MB`.

Khi query nghiệp vụ, luôn filter đủ partition keys:

```sql
SELECT status, COUNT(*) AS total_orders, SUM(amount) AS total_amount
FROM archive.orders
WHERE year = '2026'
  AND month = '08'
  AND day = '01'
GROUP BY status;
```

Thiếu filter `year/month/day` có thể làm Athena scan toàn bộ lịch sử. `LIMIT 10` không phải biện
pháp kiểm soát chi phí scan; partition pruning và workgroup byte cutoff mới là guardrail chính.

## Teardown

```bash
python setup.py destroy
```

Lệnh này xóa workgroup và saved queries bên trong. Nó giữ nguyên:

- Glue database và tables.
- Dữ liệu raw/curated trên S3.
- Các object query result đã được Athena ghi vào S3.

Query results được giữ lại có chủ ý để tránh xóa dữ liệu ngoài mong muốn. Nên cấu hình S3
lifecycle riêng cho `ATHENA_RESULTS_PREFIX`, ví dụ tự xóa sau 7 hoặc 30 ngày.

## Lỗi thường gặp

- `Glue table ... does not exist`: chạy crawler trước và copy đúng tên table vào `.env`.
- `AccessDenied` khi query: identity cần đọc Glue Catalog, đọc curated prefix và ghi results prefix.
- `No output location provided`: phải chạy query trong `ATHENA_WORKGROUP`; workgroup đã enforce
  output location nên client không cần tự truyền đường dẫn.
- Query bị cancel vì scan cutoff: thêm filter đủ `year/month/day`, chỉ select column cần thiết hoặc
  tăng giới hạn sau khi đã kiểm tra bằng `EXPLAIN`.
- Bucket khác Region: Athena, Glue Catalog và S3 nên cùng Region để cấu hình đơn giản và tránh
  lỗi/data-transfer không cần thiết.
