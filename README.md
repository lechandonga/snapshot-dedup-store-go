# snapshot-dedup-store-go

用本地磁盘目录模拟数据源与备份仓库的快照备份存储：内容寻址去重、原子提交、
崩溃可恢复、按引用回收容量。

## 仓库布局

```
<repo>/
  objects/<hash前2位>/<hash>   内容块（SHA-256 寻址，tmp+rename 原子落盘）
  manifests/<id>.json          已提交的快照清单
  tmp/                         写入暂存区，OpenStore 时整体清理
```

## 去重与快照规则

- 文件按固定 1 MiB 切块，每块以内容的 SHA-256 作为地址存入 `objects/`。
  相同内容的块全仓库只存一份；`PutChunk` 幂等，已存在即直接复用。
- `Backup(srcDir)` 遍历数据源，把每个文件表示为有序块哈希列表（`FileEntry`），
  全部文件组成一份 `Manifest`，以「UTC 时间戳 + 随机后缀」为快照 ID。
- 清单通过「写临时文件 → fsync → rename → fsync 目录」原子提交，
  rename 成功是快照的唯一提交点。任意一次已提交快照都可独立恢复出
  与当时源数据逐字节一致的结果。
- `Restore(id, dstDir)` 按清单取块、拼接、校验文件总长度，逐文件原子落盘，
  不会产生写了一半的目标文件。

## 并发与崩溃恢复语义

- 多个 `Backup`/`Restore` 可并发执行：块写入先落到 `tmp/` 下的唯一临时文件再
  原子 rename，读者永远看不到写了一半的块；并发写同一内容结果相同，互不覆盖。
- 进程异常退出后重启（重新 `OpenStore`）：
  - `tmp/` 整体清空，未完成的块写入与未提交的清单全部丢弃；
  - 已提交的快照（`manifests/` 中有完整清单）照常可用；
  - 未提交的快照不存在清单文件，访问返回 `ErrSnapshotNotFound`，
    其残留块可由 GC 安全回收后重做备份。

## 完整性校验与失败原因

读取时逐块重新计算哈希并比对，失败原因可用 `errors.Is` 区分：

| 错误 | 含义 |
|---|---|
| `ErrChunkMissing` | 块文件不存在 |
| `ErrChunkCorrupt` | 块内容哈希与地址不符（损坏/被篡改），或文件总长度与清单不符 |
| `ErrSnapshotNotFound` | 快照不存在或未提交 |
| `ErrSourceNotFound` | 备份时数据源路径不存在 |

损坏或缺失的数据绝不会被当作有效结果返回。

## 容量回收策略（GC）

`GC()` 采用标记-清扫：

1. **标记**：加载所有已提交清单，收集被引用的块哈希集合；
2. **清扫**：删除未被任何清单引用、且修改时间超过宽限期（默认 1 分钟）的块。

宽限期保护正在进行、尚未提交清单的备份所写的块不被误回收。仍被引用的块
绝不删除，因此回收之后所有已提交快照依然能完整恢复。

## 验证方法

```sh
go vet ./...
go test ./... -v          # 日志含关键判定依据（对象数、哈希比对、错误类型）
CGO_ENABLED=1 go test -race ./...
```

测试覆盖：

- `TestDedup`：重复快照零新增块、单文件修改只增一块、各快照独立逐字节恢复；
- `TestConcurrentBackupRestore`：8 路并发备份 + 并发恢复，无相互污染；
- `TestCrashRecovery`：注入崩溃残留后重启，已提交快照可用、半成品被清理拒绝；
- `TestCorruptionRejected`：篡改块报 `ErrChunkCorrupt`、删块报 `ErrChunkMissing`；
- `TestGC`：无引用块被回收、被引用块保留、存活快照恢复不受影响。
