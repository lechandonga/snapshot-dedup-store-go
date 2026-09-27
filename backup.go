package snapshot

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"time"
)

// Backup 对 srcDir 做一次快照，返回快照 ID；可并发调用。
// 相同内容的块自动去重；清单原子提交，提交前崩溃不会留下有效快照。
func (s *Store) Backup(srcDir string) (string, error) {
	if st, err := os.Stat(srcDir); err != nil || !st.IsDir() {
		return "", fmt.Errorf("%w: %s", ErrSourceNotFound, srcDir)
	}
	var files []FileEntry
	err := filepath.WalkDir(srcDir, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		if !d.Type().IsRegular() {
			return nil // 跳过非常规文件（socket、设备、符号链接等）
		}
		rel, err := filepath.Rel(srcDir, p)
		if err != nil {
			return err
		}
		data, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		entry := FileEntry{Path: filepath.ToSlash(rel), Size: int64(len(data))}
		for _, c := range splitChunks(data) {
			h, err := s.PutChunk(c)
			if err != nil {
				return err
			}
			entry.Chunks = append(entry.Chunks, h)
		}
		files = append(files, entry)
		return nil
	})
	if err != nil {
		return "", err
	}
	sort.Slice(files, func(i, j int) bool { return files[i].Path < files[j].Path })

	var rnd [6]byte
	if _, err := rand.Read(rnd[:]); err != nil {
		return "", err
	}
	id := time.Now().UTC().Format("20060102T150405.000000000") + "-" + hex.EncodeToString(rnd[:])
	m := &Manifest{ID: id, Created: time.Now().UTC().Format(time.RFC3339Nano), Files: files}
	if err := s.commitManifest(m); err != nil {
		return "", err
	}
	return id, nil
}

// Restore 把快照 id 恢复到 dstDir，结果与备份时源数据完全一致。
// 块缺失或损坏时返回可区分错误，且不会产出半成品文件（逐文件原子落盘）。
func (s *Store) Restore(id, dstDir string) error {
	m, err := s.loadManifest(id)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(dstDir, 0o755); err != nil {
		return err
	}
	for _, f := range m.Files {
		var data []byte
		for _, h := range f.Chunks {
			chunk, err := s.GetChunk(h) // 缺失/损坏在此被拦截
			if err != nil {
				return err
			}
			data = append(data, chunk...)
		}
		if int64(len(data)) != f.Size {
			return fmt.Errorf("%w: file %s size mismatch: want %d got %d",
				ErrChunkCorrupt, f.Path, f.Size, len(data))
		}
		dst := filepath.Join(dstDir, filepath.FromSlash(f.Path))
		if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
			return err
		}
		tmp, err := os.CreateTemp(filepath.Dir(dst), ".restore-*")
		if err != nil {
			return err
		}
		if _, err := tmp.Write(data); err != nil {
			tmp.Close()
			os.Remove(tmp.Name())
			return err
		}
		if err := tmp.Close(); err != nil {
			os.Remove(tmp.Name())
			return err
		}
		if err := os.Rename(tmp.Name(), dst); err != nil {
			os.Remove(tmp.Name())
			return err
		}
	}
	return nil
}
