package store

import "errors"

// 可区分的失败原因，调用方可用 errors.Is 判定。
var (
	// ErrChunkMissing 表示仓库中找不到所需的数据块。
	ErrChunkMissing = errors.New("store: chunk missing")
	// ErrChunkCorrupt 表示数据块内容与哈希不一致（被篡改或损坏）。
	ErrChunkCorrupt = errors.New("store: chunk corrupt")
	// ErrSnapshotNotFound 表示快照不存在或未提交。
	ErrSnapshotNotFound = errors.New("store: snapshot not found")
	// ErrSnapshotCorrupt 表示快照清单本身损坏。
	ErrSnapshotCorrupt = errors.New("store: snapshot manifest corrupt")
)
