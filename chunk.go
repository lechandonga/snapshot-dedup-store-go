package snapshot

import (
	"crypto/sha256"
	"encoding/hex"
)

// Hash 是内容块的寻址标识（SHA-256 的十六进制字符串）。
type Hash string

// hashBytes 计算内容的哈希。
func hashBytes(b []byte) Hash {
	sum := sha256.Sum256(b)
	return Hash(hex.EncodeToString(sum[:]))
}

// chunkSize 是固定的分块大小（字节）。
const chunkSize = 1 << 20

// splitChunks 把数据切成固定大小的块；空输入返回空切片。
func splitChunks(data []byte) [][]byte {
	var out [][]byte
	for len(data) > 0 {
		n := chunkSize
		if len(data) < n {
			n = len(data)
		}
		out = append(out, data[:n])
		data = data[n:]
	}
	return out
}
