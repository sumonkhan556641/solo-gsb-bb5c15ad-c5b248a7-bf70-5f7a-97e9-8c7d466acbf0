# Chunked Upload Service

基于 **FastAPI + SQLite** 的可断点续传分片文件上传服务：

- 创建上传会话，按序号接收携带 **SHA-256** 校验的分片
- 分片**乱序上传、失败重试**均可，重复上传幂等
- 全部分片校验通过后才**原子合并**（临时文件 + `rename`）为目标文件
- 完成的文件可直接 **GET 下载**：支持完整下载与单段 `Range`（断点续传），携带以整文件 SHA-256 生成的强 **ETag**
- 已完成会话支持**批量 ZIP 导出**（`POST /archives`）：有序选择多个会话、重名自动去序号、原子发布、归档独立于原会话保存
- 查询接口实时返回**缺失分片列表**与会话状态
- **上传会话租约**：可指定 `idle_timeout_seconds`（默认 86400 秒 / 1 天，上限 604800 秒 / 7 天）；后台定时任务与启动恢复扫描自动回收超时会话（标记 `expired`、删除分片、记录 `expired_at`）
- 元数据存 SQLite、分片存磁盘，**服务重启后可继续未完成会话**
- 启动时自动初始化存储目录，默认监听 **8000** 端口

## 快速开始

### Docker Compose（推荐）

一条命令完成镜像构建、后台启动并监听 **8000** 端口：

```bash
docker compose up --build -d
```

查看日志用 `docker compose logs -f`，停止用 `docker compose down`。

### 本地运行

```bash
pip install -r requirements.txt
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

启动后访问交互式 API 文档：<http://localhost:8000/docs>

### 环境变量

| 变量                        | 默认值   | 说明                                                         |
| --------------------------- | -------- | ------------------------------------------------------------ |
| `DATA_DIR`                  | `./data` | 存储根目录（SQLite、分片、成品文件）                         |
| `PORT`                      | `8000`   | 监听端口（文档用途，以启动参数为准）                         |
| `REAPER_INTERVAL_SECONDS`   | `300`    | 后台回收任务的扫描间隔（秒）；启动时总会先执行一次恢复扫描   |

存储布局（`DATA_DIR` 下）：

```
data/
├── uploads.db          # SQLite：会话/分片/归档元数据（WAL 模式）
├── sessions/<id>/chunks/NNNNNNNN.part   # 已接收分片
├── files/<id>_<filename>                # 合并完成的文件
└── archives/<archive_id>.zip            # 批量导出的 ZIP 归档
```

## API 一览

| 方法   | 路径                                  | 说明                                          |
| ------ | ------------------------------------- | --------------------------------------------- |
| POST   | `/uploads`                            | 创建上传会话（可指定 `idle_timeout_seconds`） |
| PUT    | `/uploads/{id}/chunks/{index}`        | 上传一个分片（原始字节，刷新会话租约）        |
| GET    | `/uploads/{id}`                       | 查询状态与缺失分片（刷新会话租约）            |
| POST   | `/uploads/{id}/complete`              | 校验并原子合并                                |
| GET    | `/uploads/{id}/content`               | 下载成品文件（支持 Range）                    |
| GET    | `/uploads`                            | 列出可续传 / 已完成的会话（不含已过期）       |
| DELETE | `/uploads/{id}`                       | 中止会话并清理                                |
| POST   | `/archives`                           | 把多个已完成会话批量导出为一个 ZIP            |
| GET    | `/archives/{id}/content`              | 下载归档 ZIP                                  |
| DELETE | `/archives/{id}`                      | 删除归档 ZIP 及其记录                         |
| GET    | `/healthz`                            | 健康检查                                      |

### 1. 创建会话

```bash
curl -X POST http://localhost:8000/uploads \
  -H 'Content-Type: application/json' \
  -d '{
        "filename": "video.mp4",
        "total_size": 2109497,
        "chunk_size": 1048576,
        "file_sha256": "<可选：整文件 SHA-256，合并时校验>",
        "idle_timeout_seconds": 86400
      }'
```

`idle_timeout_seconds` 为会话**空闲租约**：距上次成功上传分片或查询状态超过该秒数，会话即被回收。默认 `86400`（1 天），取值范围 `1 ~ 604800`（7 天），越界返回 `422`。

响应（`201`）中包含 `session_id`、推导出的 `total_chunks`、初始 `missing_chunks` 与租约信息：

```json
{
  "session_id": "fd865e4c70874dffbf42abbe1bcf9f9d",
  "status": "uploading",
  "total_chunks": 3,
  "chunk_size": 1048576,
  "received_count": 0,
  "missing_chunks": [0, 1, 2],
  "idle_timeout_seconds": 86400,
  "last_activity": "2026-09-21T12:00:00.000+00:00",
  "expires_at": "2026-09-22T12:00:00.000+00:00",
  "expired_at": null
}
```

- `last_activity`：租约计时起点，每次成功上传分片或 `GET` 状态都会刷新
- `expires_at`：租约预计到期时刻（`last_activity + idle_timeout_seconds`）；非 `uploading` 状态为 `null`
- `expired_at`：会话实际被回收的时刻；未过期时为 `null`

### 2. 上传分片（乱序 / 重试均可）

分片以**原始字节**放在请求体中，SHA-256 通过 `X-Chunk-SHA256` 请求头（或 `?sha256=` 查询参数）携带：

```bash
curl -X PUT "http://localhost:8000/uploads/$SID/chunks/2" \
  -H "X-Chunk-SHA256: $(sha256sum part-2.bin | cut -d' ' -f1)" \
  --data-binary @part-2.bin
```

校验规则（任一不满足即返回 `400`，且不落盘，可直接重试）：

- 除最后一片外，每个分片必须恰好为 `chunk_size` 字节；最后一片为剩余字节数
- 请求体的 SHA-256 必须与声明值一致

同一序号重复上传会覆盖旧数据（幂等），网络重试安全。

分片成功落盘后会刷新会话的 `last_activity`（续租）。若会话已被回收，上传返回 `410 Gone`，响应体带 `expired_at`：

```json
{
  "detail": {
    "message": "upload session has expired and its chunks were removed",
    "expired_at": "2026-09-22T12:00:00.000+00:00"
  }
}
```

### 3. 查询进度

```bash
curl http://localhost:8000/uploads/$SID
```

```json
{
  "status": "uploading",
  "received_count": 2,
  "missing_chunks": [1],
  "total_chunks": 3,
  "idle_timeout_seconds": 86400,
  "last_activity": "2026-09-21T12:03:00.000+00:00",
  "expires_at": "2026-09-22T12:03:00.000+00:00",
  "expired_at": null
}
```

对 `uploading` 会话的状态查询同样**刷新租约**（客户端轮询即可保活）。已过期会话仍可通过本接口读取（`status: "expired"`、带 `expired_at`），但 `missing_chunks` 为空、不能再续传，也不会出现在 `GET /uploads` 列表中。

### 4. 合并

```bash
curl -X POST http://localhost:8000/uploads/$SID/complete
```

- 分片未齐：返回 `409`，响应体带 `missing_chunks`
- 合并时**逐分片重新校验 SHA-256**，再校验整文件哈希（若创建时提供）
- 目标文件先写入 `*.tmp`，再 `rename` 原子发布；成功响应：

```json
{
  "status": "completed",
  "file_sha256": "e92f5a7f…",
  "final_path": "/data/files/fd865e4c…_video.mp4"
}
```

`complete` 幂等：重复调用返回相同结果。整文件哈希不匹配返回 `422`，会话保持 `uploading`，可修正后重试。会话已过期则返回 `410 Gone` 并带 `expired_at`（租约在合并进行中不会被回收）。

### 4b. 会话租约与自动回收

- 创建会话时可指定 `idle_timeout_seconds`：默认 **86400** 秒（1 天），上限 **604800** 秒（7 天）
- 每次**成功上传分片**或**查询会话状态**都把 `last_activity` 刷新为当前时刻；租约到期时刻为 `expires_at`
- 回收时机：
  - **后台定时任务**：每 `REAPER_INTERVAL_SECONDS`（默认 300 秒）扫描一次
  - **启动恢复扫描**：服务启动时先执行一次，回收停机期间到期的会话
- 回收动作（仅针对仍为 `uploading` 的超时会话）：状态置为 `expired`、删除其全部分片文件与记录、写入 `expired_at`
- 过期后：上传分片、`complete` 返回 **410 Gone**（响应体含 `expired_at`）；状态查询仍 **200** 可读，但不再列入 `GET /uploads`，也无法续传
- `completed` 会话不参与空闲回收；正在 `complete`（合并）的会话由合并锁保护，不会被回收任务误改；`DELETE` 删除的会话直接消失，回收任务无从修改

如需放弃会话，可随时 `DELETE /uploads/{id}`（返回 `204`），其分片、元数据与成品文件都会被删除。

### 5. 下载成品文件（支持 Range / 断点续传）

仅 `completed` 状态的会话可下载；未完成返回 `409`，未知会话返回 `404`。响应始终携带：

- `Accept-Ranges: bytes`
- 强 **ETag**：以整文件 SHA-256 生成，形如 `"e92f5a7f…"`（合并时即固定）
- `Content-Length`；区间下载（`206`）另带 `Content-Range: bytes start-end/总长度`

```bash
# 完整下载（200）
curl -o video.mp4 http://localhost:8000/uploads/$SID/content

# 指定单段区间（206 Partial Content）
curl -H 'Range: bytes=0-1048575' -o part.bin \
  http://localhost:8000/uploads/$SID/content

# 断点续传：curl -C - 会自动补上 Range 请求头
curl -C - -o video.mp4 http://localhost:8000/uploads/$SID/content
```

支持的三种单段写法（仅支持**单段**）：

| Range                  | 含义                                   |
| ---------------------- | -------------------------------------- |
| `bytes=start-end`      | 第 start 到第 end 字节（闭区间，从 0 计） |
| `bytes=start-`        | 从 start 到文件尾                       |
| `bytes=-suffix`       | 文件末尾 suffix 字节                     |

条件请求与错误码：

- `If-None-Match` 命中当前 ETag（弱比较，`W/` 前缀或 `*` 亦可）→ `304 Not Modified`，无响应体：

```bash
curl -H 'If-None-Match: "e92f5a7f…"' -i \
  http://localhost:8000/uploads/$SID/content
```

- `If-Range` 与当前 ETag **不一致**（强比较，弱 ETag 不匹配；HTTP 日期不支持）→ 忽略 `Range`，返回 `200` 完整文件；一致才返回 `206`
- 多段（`bytes=0-1,3-4`）、语法错误或越界范围 → `416 Requested Range Not Satisfiable`，并携带 `Content-Range: bytes */总长度`：

```http
HTTP/1.1 416 Requested Range Not Satisfiable
Content-Range: bytes */3000000
```

下载时按区间在磁盘上**分块流式读取**（1 MiB 缓冲），不会把整个文件载入内存。

### 6. 批量 ZIP 归档

已完成的会话可以按给定顺序打包成单个 ZIP 归档。`POST /archives` 接收一个**有序、不重复**的 `session_id` 列表（空列表 / 重复 id 返回 `422`），列表中的每个会话都必须是 `completed` 且成品文件仍在磁盘上：

- 未知 session id → `404`（响应体 `detail.session_id` 指出是哪一个）
- 会话未完成或已过期 → `409`
- 数据库标记完成但成品文件丢失 / 尺寸异常 → `409`
- 任一源不满足条件，整个请求失败，**不会留下任何归档文件或记录**

```bash
curl -X POST http://localhost:8000/archives \
  -H 'Content-Type: application/json' \
  -d '{"session_ids": ["<sid-1>", "<sid-2>", "<sid-3>"]}'
```

归档过程：先**打开并固定全部源文件**（并发删除会话也无法移走已打开文件的字节），再以 1 MiB 有界缓冲把数据流式写入临时 ZIP，写入成功后 `rename` **原子发布**，最后在数据库记录归档 ID、条目列表与归档大小；任何一步失败都会删除临时文件。响应（`201`）：

```json
{
  "archive_id": "8b1f0c2e9a7d4f6b8c3e5a1d0f2b9c4e",
  "size": 2128063,
  "created_at": "2026-09-28T10:00:00.000+00:00",
  "files": [
    {"position": 0, "session_id": "<sid-1>", "entry_name": "video.mp4",  "size": 2109497},
    {"position": 1, "session_id": "<sid-2>", "entry_name": "report-2.pdf", "size": 18422}
  ]
}
```

ZIP 条目严格按请求顺序排列；重名条目自动获得**稳定且不冲突**的名字（仅主文件名去序号，大小写冲突也算重名），并保留原始扩展名：首个保持原名，后续依次追加 `-2`、`-3`……，例如 `report.pdf`、`report-2.pdf`、`REPORT-3.PDF`、`report-4.pdf`。条目不压缩（STORED，归档速度只受磁盘 IO 限制），单条目超过 4 GiB 时自动使用 ZIP64。

下载与删除：

```bash
# 下载（Content-Type: application/zip）
curl -o export.zip http://localhost:8000/archives/$AID/content

# 删除归档 ZIP 与其数据库记录（不影响任何上传会话；未知 id 返回 404）
curl -X DELETE http://localhost:8000/archives/$AID
```

归档是**独立快照**：创建成功后即使删除原上传会话（`DELETE /uploads/{id}` 会同时删除成品文件），既有归档的下载、记录与删除都不受影响。

## 断点续传语义

- 所有状态都在 `DATA_DIR` 内（SQLite + 分片文件），进程重启、容器重建（挂载同一 volume）后，未完成会话自动可用
- 启动时自动对账：数据库记录与磁盘分片逐一核对，缺失或尺寸不符的记录会被剔除，客户端按 `missing_chunks` 重传即可
- 启动对账后立即执行租约恢复扫描：停机期间超时的 `uploading` 会话被标记 `expired` 并清理分片
- 分片写入同样先写临时文件再原子改名，不存在半写的分片
- 旧版本数据库（无租约列）在启动时自动迁移：既有会话的空闲超时取默认值 86400 秒，`last_activity` 沿用其 `updated_at`，分片数据不受影响

## 测试

```bash
pip install -r requirements-dev.txt
pytest tests/
```

测试覆盖：乱序上传、错误哈希拒绝与重试、重复分片幂等、未齐时合并返回 409、重启续传、整文件哈希校验、会话中止、完整/区间下载、ETag 与 304、If-Range 回退、416 越界处理，以及会话租约默认值/上限校验、创建后立即查询状态返回 200 并续租（回归）、分片上传与状态查询续租、后台回收（标记过期、删分片、上传/complete 返回 410 带 `expired_at`、过期会话可读但不续租也不入列表）、合并中会话不被回收、已完成与已删除会话不被误改、启动恢复扫描和旧库迁移；批量归档覆盖有序打包与条目内容、重名去序号并保留扩展名（含大小写冲突与无扩展名）、请求校验（空/重复 id 422）、源未完成/未知/成品丢失返回 409/404 且不留半成品、写入失败清理、删除归档（204/404、级联清理条目）、原会话删除后归档仍可下载、启动对账清理孤儿归档，以及大文件分块流式归档。

## 客户端示例（Python）

```python
import hashlib, os, requests

BASE = "http://localhost:8000"
path, chunk_size = "video.mp4", 1024 * 1024
size = os.path.getsize(path)
sha = hashlib.sha256(open(path, "rb").read()).hexdigest()

sid = requests.post(f"{BASE}/uploads", json={
    "filename": os.path.basename(path),
    "total_size": size,
    "chunk_size": chunk_size,
    "file_sha256": sha,
    "idle_timeout_seconds": 86400,  # 可选，默认 1 天，上限 7 天
}).json()["session_id"]

with open(path, "rb") as f:
    for index in range((size + chunk_size - 1) // chunk_size):
        data = f.read(chunk_size)
        digest = hashlib.sha256(data).hexdigest()
        r = requests.put(f"{BASE}/uploads/{sid}/chunks/{index}",
                         data=data, headers={"X-Chunk-SHA256": digest})
        if r.status_code == 410:
            # 租约超时：会话及其分片已被回收，需要重新创建会话从头上传
            raise RuntimeError(f"session expired at {r.json()['detail']['expired_at']}")
        r.raise_for_status()

print(requests.post(f"{BASE}/uploads/{sid}/complete").json())

# 下载：完整 / 区间（stream=True 分块落盘，不占大内存）
with requests.get(f"{BASE}/uploads/{sid}/content", stream=True) as r:
    r.raise_for_status()
    with open("downloaded.mp4", "wb") as out:
        for block in r.iter_content(1024 * 1024):
            out.write(block)

# 断点续传：从第 1 MiB 开始取剩余部分
etag = '"e92f5a7f…"'  # 上次响应里的 ETag
r = requests.get(
    f"{BASE}/uploads/{sid}/content",
    headers={"Range": "bytes=1048576-", "If-Range": etag},
)
assert r.status_code in (200, 206)  # 206=区间有效；200=文件已变，给的是完整内容

# 批量 ZIP 归档：多个已完成会话按列表顺序打入一个 ZIP
r = requests.post(
    f"{BASE}/archives",
    json={"session_ids": [sid, another_sid]},  # 必须有序且不重复
)
r.raise_for_status()
archive = r.json()
print(archive["archive_id"], [(f["entry_name"], f["size"]) for f in archive["files"]])

with requests.get(f"{BASE}/archives/{archive['archive_id']}/content", stream=True) as dl:
    dl.raise_for_status()
    with open("export.zip", "wb") as out:
        for block in dl.iter_content(1024 * 1024):
            out.write(block)

# 删除归档（不影响原上传会话）
assert requests.delete(f"{BASE}/archives/{archive['archive_id']}").status_code == 204
```
