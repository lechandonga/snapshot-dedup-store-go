package snapshot

import (
	"os"
	"path/filepath"
	"time"
)

// gcGrace 是回收宽限期：比它年轻的块即使未被任何清单引用也不回收，
// 以保护正在进行、尚未提交清单的备份所写的块。
const gcGrace = time.Minute

// GC 回收不再被任何已提交快照引用的块，返回回收的块数。
// 仍被引用的块绝不回收，回收后所有已提交快照依然可完整恢复。
func (s *Store) GC() (int, error) {
	return s.gc(gcGrace)
}

func (s *Store) gc(grace time.Duration) (int, error) {
	// 标记：收集所有已提交清单引用的块。
	ids, err := s.listManifests()
	if err != nil {
		return 0, err
	}
	live := make(map[Hash]bool)
	for _, id := range ids {
		m, err := s.loadManifest(id)
		if err != nil {
			return 0, err
		}
		for _, f := range m.Files {
			for _, h := range f.Chunks {
				live[h] = true
			}
		}
	}
	// 清扫：删除未被引用且超过宽限期的块。
	removed := 0
	cutoff := time.Now().Add(-grace)
	err = filepath.WalkDir(filepath.Join(s.root, "objects"), func(p string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		if live[Hash(d.Name())] {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		if info.ModTime().After(cutoff) {
			return nil // 宽限期内，可能是进行中的备份
		}
		if err := os.Remove(p); err != nil {
			return err
		}
		removed++
		return nil
	})
	if err != nil {
		return removed, err
	}
	// 顺手清理空的前缀目录。
	prefixes, _ := filepath.Glob(filepath.Join(s.root, "objects", "*"))
	for _, p := range prefixes {
		os.Remove(p) // 非空则失败，忽略
	}
	return removed, nil
}
