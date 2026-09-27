package snapshot

import "errors"

// 可区分的失败原因，调用方可用 errors.Is 判定。
var (
	// ErrChunkMissing 表示仓库中找不到指定内容块。
	ErrChunkMissing = errors.New("chunk missing")
	// ErrChunkCorrupt 表示块内容与其哈希不匹配（损坏或被篡改）。
	ErrChunkCorrupt = errors.New("chunk corrupt")
	// ErrSnapshotNotFound 表示指定的快照不存在或未完成提交。
	ErrSnapshotNotFound = errors.New("snapshot not found")
	// ErrSourceNotFound 表示备份时数据源路径不存在。
	ErrSourceNotFound = errors.New("source not found")
)
