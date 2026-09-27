package snapshot

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
)

// Store 是内容寻址的块仓库，落盘在 root 目录下。
// 目录布局：
//
//	objects/<前2位>/<hash>   已提交的块（tmp+rename 原子落盘）
//	manifests/<id>.json      已提交的快照清单
//	tmp/                     写入暂存区，OpenStore 时清理（崩溃恢复）
type Store struct {
	root string
}

// OpenStore 打开（必要时创建）仓库，并清理上次异常退出留下的中间状态。
func OpenStore(root string) (*Store, error) {
	s := &Store{root: root}
	for _, d := range []string{"objects", "manifests", "tmp"} {
		if err := os.MkdirAll(filepath.Join(root, d), 0o755); err != nil {
			return nil, err
		}
	}
	// 崩溃恢复：暂存区里只可能有未完成的写，全部丢弃。
	if err := os.RemoveAll(filepath.Join(root, "tmp")); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Join(root, "tmp"), 0o755); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *Store) objectPath(h Hash) string {
	str := string(h)
	return filepath.Join(s.root, "objects", str[:2], str)
}

// PutChunk 幂等地写入一个块，返回其哈希；并发写同一内容安全。
func (s *Store) PutChunk(data []byte) (Hash, error) {
	h := hashBytes(data)
	dst := s.objectPath(h)
	if _, err := os.Stat(dst); err == nil {
		return h, nil // 内容寻址：已存在即相同内容，直接复用（去重）
	}
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return "", err
	}
	// 先写唯一暂存文件并 fsync，再原子 rename，读者永远看不到写了一半的块。
	tmp, err := os.CreateTemp(filepath.Join(s.root, "tmp"), "chunk-*")
	if err != nil {
		return "", err
	}
	defer os.Remove(tmp.Name())
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return "", err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return "", err
	}
	if err := tmp.Close(); err != nil {
		return "", err
	}
	if err := os.Rename(tmp.Name(), dst); err != nil {
		return "", err
	}
	return h, nil
}

// GetChunk 读取并校验一个块；缺失或损坏返回可区分的错误。
func (s *Store) GetChunk(h Hash) ([]byte, error) {
	data, err := os.ReadFile(s.objectPath(h))
	if errors.Is(err, fs.ErrNotExist) {
		return nil, fmt.Errorf("%w: %s", ErrChunkMissing, h)
	}
	if err != nil {
		return nil, err
	}
	if got := hashBytes(data); got != h {
		return nil, fmt.Errorf("%w: want %s got %s", ErrChunkCorrupt, h, got)
	}
	return data, nil
}

// HasChunk 报告块是否存在（不做完整性校验）。
func (s *Store) HasChunk(h Hash) bool {
	_, err := os.Stat(s.objectPath(h))
	return err == nil
}
