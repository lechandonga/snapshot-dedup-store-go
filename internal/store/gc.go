package store

import (
	"os"
	"path/filepath"
	"strings"
)

// GCStats 记录一次回收的结果。
type GCStats struct {
	RemovedChunks int   // 被回收的块数量
	Reclaimed     int64 // 回收的字节数
}

// GC 标记-清扫：删除不被任何已提交快照引用的数据块。
//
// 安全性：GC 持有写锁，与进行中的备份（读锁）互斥，因此
// "备份已写块但尚未提交"的窗口内不会运行 GC；仍被引用的块不会被误回收。
func (s *Store) GC() (GCStats, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// 标记：收集所有已提交快照引用的块。
	live := make(map[string]struct{})
	entries, err := os.ReadDir(filepath.Join(s.root, "snapshots"))
	if err != nil {
		return GCStats{}, err
	}
	for _, e := range entries {
		if !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		m, err := s.loadManifest(strings.TrimSuffix(e.Name(), ".json"))
		if err != nil {
			return GCStats{}, err
		}
		for h := range m.chunkSet() {
			live[h] = struct{}{}
		}
	}

	// 清扫：遍历 chunks/，删除未被标记的块。
	var stats GCStats
	chunksDir := filepath.Join(s.root, "chunks")
	err = filepath.WalkDir(chunksDir, func(p string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || strings.HasPrefix(d.Name(), ".tmp-") {
			return nil
		}
		hash := d.Name()
		if _, ok := live[hash]; ok {
			return nil // 仍被引用，保留
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		if err := os.Remove(p); err != nil {
			return err
		}
		stats.RemovedChunks++
		stats.Reclaimed += info.Size()
		return nil
	})
	if err != nil {
		return stats, err
	}
	// 清理可能空掉的分目录。
	sub, _ := os.ReadDir(chunksDir)
	for _, e := range sub {
		if e.IsDir() {
			os.Remove(filepath.Join(chunksDir, e.Name())) // 仅当为空时成功
		}
	}
	return stats, nil
}
