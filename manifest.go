package snapshot

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

// FileEntry 描述快照中一个文件由哪些块组成。
type FileEntry struct {
	Path   string `json:"path"`
	Size   int64  `json:"size"`
	Chunks []Hash `json:"chunks"`
}

// Manifest 是一次快照的完整描述。
type Manifest struct {
	ID      string      `json:"id"`
	Created string      `json:"created"`
	Files   []FileEntry `json:"files"`
}

func (s *Store) manifestPath(id string) string {
	return filepath.Join(s.root, "manifests", id+".json")
}

// commitManifest 原子地提交一个快照清单；提交前对读取者不可见。
// 这是快照的“提交点”：rename 成功后快照才生效，进程崩溃只会留下
// tmp 里的半成品，不会被当成有效快照。
func (s *Store) commitManifest(m *Manifest) error {
	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Join(s.root, "tmp"), "manifest-*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
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
	if err := os.Rename(tmp.Name(), s.manifestPath(m.ID)); err != nil {
		return err
	}
	// fsync 目录项，保证重启后提交记录不丢。
	d, err := os.Open(filepath.Join(s.root, "manifests"))
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

// loadManifest 读取已提交的快照清单。
func (s *Store) loadManifest(id string) (*Manifest, error) {
	data, err := os.ReadFile(s.manifestPath(id))
	if errors.Is(err, fs.ErrNotExist) {
		return nil, fmt.Errorf("%w: %s", ErrSnapshotNotFound, id)
	}
	if err != nil {
		return nil, err
	}
	var m Manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf("manifest %s unreadable: %w", id, err)
	}
	return &m, nil
}

// listManifests 列出所有已提交快照的 ID。
func (s *Store) listManifests() ([]string, error) {
	ents, err := os.ReadDir(filepath.Join(s.root, "manifests"))
	if err != nil {
		return nil, err
	}
	var ids []string
	for _, e := range ents {
		if name := e.Name(); strings.HasSuffix(name, ".json") {
			ids = append(ids, strings.TrimSuffix(name, ".json"))
		}
	}
	return ids, nil
}
