package store

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// ChunkRef 引用一个内容寻址的数据块。
type ChunkRef struct {
	Hash string `json:"hash"` // 内容 SHA-256 的十六进制编码
	Size int64  `json:"size"` // 明文长度
}

// hashBytes 计算内容的 SHA-256，返回十六进制编码。
func hashBytes(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

// chunkPath 返回块在仓库内的完整路径（按哈希前两位分目录）。
func (s *Store) chunkPath(hash string) string {
	return filepath.Join(s.root, "chunks", hash[:2], hash)
}

// writeChunk 把 data 以内容寻址方式写入仓库。
// 已存在相同哈希的块时直接跳过（去重），写入采用 临时文件+rename 保证原子性：
// 并发读者只会看到完整块或看不到块，绝不会读到写了一半的内容。
func (s *Store) writeChunk(data []byte) (ChunkRef, error) {
	ref := ChunkRef{Hash: hashBytes(data), Size: int64(len(data))}
	path := s.chunkPath(ref.Hash)
	if _, err := os.Stat(path); err == nil {
		return ref, nil // 去重：内容已存在
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return ChunkRef{}, err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".tmp-*")
	if err != nil {
		return ChunkRef{}, err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName) // 已 rename 后是 no-op
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return ChunkRef{}, err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return ChunkRef{}, err
	}
	if err := tmp.Close(); err != nil {
		return ChunkRef{}, err
	}
	// rename 是原子操作；若并发写同一哈希，后写者覆盖同内容文件，结果一致。
	if err := os.Rename(tmpName, path); err != nil {
		return ChunkRef{}, err
	}
	return ref, nil
}

// readChunk 读取并校验一个块：缺失返回 ErrChunkMissing，
// 内容哈希与 ref 不符返回 ErrChunkCorrupt。
func (s *Store) readChunk(ref ChunkRef) ([]byte, error) {
	path := s.chunkPath(ref.Hash)
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("%w: %s", ErrChunkMissing, ref.Hash)
		}
		return nil, err
	}
	defer f.Close()
	data, err := io.ReadAll(f)
	if err != nil {
		return nil, err
	}
	if int64(len(data)) != ref.Size || hashBytes(data) != ref.Hash {
		return nil, fmt.Errorf("%w: %s", ErrChunkCorrupt, ref.Hash)
	}
	return data, nil
}
