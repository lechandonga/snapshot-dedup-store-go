# snapshot-dedup-store-go

基于本地磁盘目录的内容寻址快照备份存储。用本地目录模拟数据源与备份仓库，
重复内容只存一份，支持并发备份/恢复、崩溃安全恢复与容量回收。

## 构建与使用

```sh
go build ./cmd/snapstore

snapstore -repo <仓库目录> backup  <源目录>        # 做快照，打印快照 ID
snapstore -repo <仓库目录> list                    # 列出已提交快照
snapstore -repo <仓库目录> restore <ID> <目标目录>  # 恢复快照
snapstore -repo <仓库目录> verify  <ID>            # 校验快照完整性
snapstore -repo <仓库目录> delete  <ID>            # 删除快照（块由 gc 回收）
snapstore -repo <仓库目录> gc                      # 回收无引用数据块
```

## 仓库布局

```
<repo>/
  chunks/<hash[:2]>/<hash>   # 内容寻址数据块（SHA-256），不可变
  snapshots/<id>.json        # 已提交快照清单（不可变）
  staging/<id>.json          # 进行中的备份暂存清单（崩溃后可识别清理）
```

## 去重与快照规则

- 文件按固定 64 KiB 分块，每块以内容的 SHA-256 命名存储；相同内容的块
  全仓库只存一份（跨文件、跨快照去重）。
- 快照清单记录每个文件的有序块列表（哈希 + 长度），按序拼接即原文；
  恢复逐块校验哈希与长度，并按清单核对文件总大小，保证不出现
  内容错位、缺块或多出数据。
- 每个快照自包含、不可变，任意一次快照都可独立恢复出当时源数据的
  完全一致副本（含子目录结构与权限位）。

## 崩溃恢复语义

- **原子提交**：块与清单都通过"临时文件 + fsync + rename"写入，
  rename 是唯一提交点。读者只会看到完整数据或看不到，绝不会读到
  写了一半的中间状态。
- **备份流程**：写块 → 落暂存清单 → rename 提交到 `snapshots/`。
  在 rename 之前任何时刻崩溃，该快照都不存在、不算有效数据。
- **重启恢复**（`Open` 时执行）：删除 `chunks/` 下残留的 `.tmp-*`
  半成品文件；清空 `staging/` 暂存清单。未完成的备份可安全重做，
  其遗留的孤儿块由后续 GC 回收。已提交快照不受任何影响。

## 并发模型

- 多个备份/恢复可同时进行：块按内容寻址、不可变，并发写同一哈希
  等价于写同一份内容；恢复只读已提交的不可变数据。
- 进程内用读写锁协调：备份持读锁（彼此并发），GC 与删除持写锁
  （独占）。这保证"备份已写块但尚未提交清单"的窗口内不会运行 GC，
  进行中的备份所依赖的块不会被误回收。

## 容量回收策略（GC）

标记-清扫：

1. **标记**：汇总所有已提交快照引用的块哈希。
2. **清扫**：删除 `chunks/` 下未被任何快照引用的块（含崩溃遗留的
   孤儿块），并清理空分目录。

仍被任一快照引用的块绝不被回收；回收后所有已提交快照依然可正常
恢复（测试 `TestGCReclaimsOnlyUnreferenced` 覆盖）。

## 完整性与可区分的失败原因

读取路径上任何数据都必须通过校验，否则拒绝返回并给出可用
`errors.Is` 区分的错误：

| 错误 | 含义 |
|---|---|
| `store.ErrChunkMissing` | 数据块文件缺失 |
| `store.ErrChunkCorrupt` | 块内容哈希/长度与清单不符（被篡改或损坏） |
| `store.ErrSnapshotNotFound` | 快照不存在或未提交 |
| `store.ErrSnapshotCorrupt` | 快照清单本身无法解析 |

## 验证方法

```sh
go vet ./...
go test ./... -v          # 单测日志打印关键判定依据
CGO_ENABLED=1 go test -race ./...   # 并发竞态检测
```

测试覆盖：

- `TestDedupAndIndependentRestore`：重复内容去重、两次快照各自独立逐字节恢复；
- `TestConcurrentBackupAndRestore`：并发备份与恢复不互相覆盖、无中间状态；
- `TestCrashRecovery`：模拟崩溃现场（暂存清单/孤儿块/半成品临时文件），
  重启后已提交快照可用、未完成备份被安全清理；
- `TestCorruptAndMissingDataRejected`：篡改、缺失、清单损坏均被拒绝且原因可区分；
- `TestGCReclaimsOnlyUnreferenced`：GC 只回收无引用块，回收后快照仍可恢复。
