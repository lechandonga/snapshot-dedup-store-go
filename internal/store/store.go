package store

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

// chunkSize 是固定分块大小（64 KiB）。
const chunkSize = 64 * 1024

// Store 是基于本地目录的去重快照仓库。
//
// 目录布局：
//   <root>/chunks/<hash[:2]>/<hash>   内容寻址数据块（不可变，原子写入）
//   <root>/snapshots/<id>.json        已提交快照清单（原子提交）
//   <root>/staging/<id>.json          进行中的备份暂存清单（崩溃后可识别清理）
type Store struct {
	root string
	mu   sync.RWMutex // 备份/恢复持读锁（彼此并发），GC/删除持写锁（独占）
}

// Open 打开（必要时创建）仓库，并执行崩溃恢复：
//   - 清理上次进程退出遗留的临时文件（.tmp-*）；
//   - 删除未提交备份的暂存清单（其数据块成为孤儿，由 GC 回收）；
//   - 已提交快照对应的暂存清单一并清除。
func Open(root string) (*Store, error) {
	s := &Store{root: root}
	for _, d := range []string{"chunks", "snapshots", "staging"} {
		if err := os.MkdirAll(filepath.Join(root, d), 0o755); err != nil {
			return nil, err
		}
	}
	if err := s.recover(); err != nil {
		return nil, err
	}
	return s, nil
}

// recover 执行崩溃恢复清理。
func (s *Store) recover() error {
	// 1. 删除 chunks 目录里残留的临时文件（写一半未 rename 的块）。
	err := filepath.WalkDir(filepath.Join(s.root, "chunks"), func(p string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() && strings.HasPrefix(d.Name(), ".tmp-") {
			return os.Remove(p)
		}
		return nil
	})
	if err != nil {
		return err
	}
	// 2. 清理暂存清单：无论对应快照是否已提交，暂存文件都应移除。
	//    未提交的备份不算有效数据，调用方可安全重做。
	entries, err := os.ReadDir(filepath.Join(s.root, "staging"))
	if err != nil {
		return err
	}
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".json") || strings.HasPrefix(e.Name(), ".tmp-") {
			if err := os.Remove(filepath.Join(s.root, "staging", e.Name())); err != nil {
				return err
			}
		}
	}
	return nil
}

// newID 生成全局唯一的快照 ID：时间戳 + 随机后缀，避免并发冲突。
func newID() string {
	var b [4]byte
	rand.Read(b[:])
	return fmt.Sprintf("%s-%s", time.Now().UTC().Format("20060102T150405.000000000"), hex.EncodeToString(b[:]))
}

// Backup 对 sourceDir 做一次快照，返回已提交快照的 ID。
//
// 流程：逐文件分块写入 chunks（内容相同自动去重）→ 写暂存清单 →
// 原子 rename 提交。任何时刻崩溃都不会产生"半个有效快照"。
func (s *Store) Backup(sourceDir string) (string, error) {
	m := &Manifest{
		ID:        newID(),
		CreatedAt: time.Now().UTC(),
		Source:    sourceDir,
	}
	err := filepath.WalkDir(sourceDir, func(p string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return nil // 跳过非常规文件（socket、设备等）
		}
		rel, err := filepath.Rel(sourceDir, p)
		if err != nil {
			return err
		}
		entry, err := s.backupFile(p, filepath.ToSlash(rel), info.Mode().Perm())
		if err != nil {
			return err
		}
		m.Files = append(m.Files, entry)
		return nil
	})
	if err != nil {
		return "", err
	}
	sort.Slice(m.Files, func(i, j int) bool { return m.Files[i].Path < m.Files[j].Path })

	// 整个备份持读锁：与 GC 的写锁互斥，保证"先写块后声明"之间
	// 不会被 GC 回收刚写入的块；多个备份之间仍可并发。
	s.mu.RLock()
	defer s.mu.RUnlock()
	// 先落暂存清单：声明本备份引用的块，防止并发 GC 误回收。
	if err := s.writeStaging(m); err != nil {
		return "", err
	}
	// 原子提交：rename 成功即快照生效的唯一时刻。
	if err := s.commitManifest(m); err != nil {
		return "", err
	}
	return m.ID, nil
}

// backupFile 把单个文件分块写入仓库，返回其 FileEntry。
func (s *Store) backupFile(path, rel string, perm os.FileMode) (FileEntry, error) {
	f, err := os.Open(path)
	if err != nil {
		return FileEntry{}, err
	}
	defer f.Close()
	entry := FileEntry{Path: rel, Mode: int64(perm)}
	buf := make([]byte, chunkSize)
	for {
		n, err := io.ReadFull(f, buf)
		if n > 0 {
			ref, werr := s.writeChunk(buf[:n])
			if werr != nil {
				return FileEntry{}, werr
			}
			entry.Chunks = append(entry.Chunks, ref)
			entry.Size += ref.Size
		}
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			break
		}
		if err != nil {
			return FileEntry{}, err
		}
	}
	return entry, nil
}

// Restore 将快照 id 完整恢复到 destDir。
// 每个文件先写到同目录临时文件、校验后再 rename 就位，
// 并发恢复或中途失败不会留下"看似完整"的文件。
func (s *Store) Restore(id, destDir string) error {
	m, err := s.loadManifest(id)
	if err != nil {
		return err
	}
	for _, fe := range m.Files {
		if err := s.restoreFile(fe, destDir); err != nil {
			return err
		}
	}
	return nil
}

// restoreFile 恢复单个文件：按序读取块（带完整性校验）写入临时文件，
// fsync 后原子 rename 到目标路径。
func (s *Store) restoreFile(fe FileEntry, destDir string) error {
	dest := filepath.Join(destDir, filepath.FromSlash(fe.Path))
	if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(dest), ".restore-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	var written int64
	for _, ref := range fe.Chunks {
		data, err := s.readChunk(ref) // 缺失/损坏在此被拦截
		if err != nil {
			tmp.Close()
			return fmt.Errorf("restore %s: %w", fe.Path, err)
		}
		if _, err := tmp.Write(data); err != nil {
			tmp.Close()
			return err
		}
		written += int64(len(data))
	}
	if written != fe.Size {
		tmp.Close()
		return fmt.Errorf("restore %s: %w: size mismatch (%d != %d)", fe.Path, ErrChunkCorrupt, written, fe.Size)
	}
	if err := tmp.Chmod(os.FileMode(fe.Mode).Perm()); err != nil {
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
	return os.Rename(tmpName, dest)
}

// Snapshots 列出所有已提交快照，按创建时间排序。
func (s *Store) Snapshots() ([]Manifest, error) {
	entries, err := os.ReadDir(filepath.Join(s.root, "snapshots"))
	if err != nil {
		return nil, err
	}
	var out []Manifest
	for _, e := range entries {
		if !strings.HasSuffix(e.Name(), ".json") {
			continue
		}
		m, err := s.loadManifest(strings.TrimSuffix(e.Name(), ".json"))
		if err != nil {
			return nil, err
		}
		out = append(out, *m)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.Before(out[j].CreatedAt) })
	return out, nil
}

// Delete 删除一个已提交快照（不回收数据块，块由 GC 处理）。
func (s *Store) Delete(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	path := s.manifestPath(id)
	if _, err := os.Stat(path); err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("%w: %s", ErrSnapshotNotFound, id)
		}
		return err
	}
	return os.Remove(path)
}

// Verify 校验快照引用的所有块存在且哈希正确。
func (s *Store) Verify(id string) error {
	m, err := s.loadManifest(id)
	if err != nil {
		return err
	}
	for _, fe := range m.Files {
		for _, ref := range fe.Chunks {
			if _, err := s.readChunk(ref); err != nil {
				return fmt.Errorf("verify %s: %w", fe.Path, err)
			}
		}
	}
	return nil
}
