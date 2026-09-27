package store

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// FileEntry 描述快照中的一个文件。
type FileEntry struct {
	Path   string     `json:"path"`   // 相对路径，使用 '/' 分隔
	Mode   int64      `json:"mode"`   // 权限位
	Size   int64      `json:"size"`   // 文件总大小
	Chunks []ChunkRef `json:"chunks"` // 有序块列表，按序拼接即原文
}

// Manifest 是一次已提交快照的不可变描述。
type Manifest struct {
	ID        string      `json:"id"`
	CreatedAt time.Time   `json:"created_at"`
	Source    string      `json:"source"`
	Files     []FileEntry `json:"files"`
}

// chunkSet 返回清单引用的全部块哈希。
func (m *Manifest) chunkSet() map[string]struct{} {
	set := make(map[string]struct{})
	for _, f := range m.Files {
		for _, c := range f.Chunks {
			set[c.Hash] = struct{}{}
		}
	}
	return set
}

// manifestPath 返回已提交清单的路径。
func (s *Store) manifestPath(id string) string {
	return filepath.Join(s.root, "snapshots", id+".json")
}

// stagingPath 返回进行中备份的暂存清单路径。
// 暂存清单的存在同时用于：1) 崩溃恢复时识别未完成备份；2) GC 保护其引用的块。
func (s *Store) stagingPath(id string) string {
	return filepath.Join(s.root, "staging", id+".json")
}

// writeFileAtomic 将 data 原子写入 path（临时文件 + fsync + rename + 目录 fsync）。
func writeFileAtomic(path string, data []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".tmp-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpName, path); err != nil {
		return err
	}
	// fsync 目录，保证 rename 在崩溃后依然持久。
	dir, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

// commitManifest 原子提交清单：先写暂存（备份进行中已存在），
// 再 rename 到 snapshots/ 完成提交，最后删除暂存。
// 提交点即 rename：崩溃发生在 rename 前则快照无效，之后则完整可用。
func (s *Store) commitManifest(m *Manifest) error {
	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	if err := writeFileAtomic(s.manifestPath(m.ID), data); err != nil {
		return err
	}
	// 提交成功后移除暂存标记；失败无碍（Open 时会清理已提交对应的暂存）。
	os.Remove(s.stagingPath(m.ID))
	return nil
}

// writeStaging 把进行中的清单写入暂存区，供 GC 引用计数与崩溃恢复识别。
func (s *Store) writeStaging(m *Manifest) error {
	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	return writeFileAtomic(s.stagingPath(m.ID), data)
}

// loadManifest 读取并解析已提交清单；JSON 无法解析时报 ErrSnapshotCorrupt。
func (s *Store) loadManifest(id string) (*Manifest, error) {
	data, err := os.ReadFile(s.manifestPath(id))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("%w: %s", ErrSnapshotNotFound, id)
		}
		return nil, err
	}
	var m Manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf("%w: %s: %v", ErrSnapshotCorrupt, id, err)
	}
	return &m, nil
}
