package store

import (
	"bytes"
	"crypto/rand"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

// ---- 测试辅助 ----

func writeSource(t *testing.T, dir string, files map[string][]byte) {
	t.Helper()
	for name, data := range files {
		p := filepath.Join(dir, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, data, 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

func readTree(t *testing.T, dir string) map[string][]byte {
	t.Helper()
	out := map[string][]byte{}
	err := filepath.WalkDir(dir, func(p string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		rel, _ := filepath.Rel(dir, p)
		data, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		out[filepath.ToSlash(rel)] = data
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func countChunks(t *testing.T, repo string) int {
	t.Helper()
	n := 0
	filepath.WalkDir(filepath.Join(repo, "chunks"), func(p string, d os.DirEntry, err error) error {
		if err == nil && !d.IsDir() && !strings.HasPrefix(d.Name(), ".tmp-") {
			n++
		}
		return nil
	})
	return n
}

func randomBytes(n int) []byte {
	b := make([]byte, n)
	rand.Read(b)
	return b
}

// ---- 1. 去重与独立恢复 ----

func TestDedupAndIndependentRestore(t *testing.T) {
	repo := t.TempDir()
	src := t.TempDir()

	shared := randomBytes(200 * 1024) // 跨多个块的大文件
	writeSource(t, src, map[string][]byte{
		"a.bin":       shared,
		"dir/b.bin":   shared, // 与 a.bin 完全重复
		"dir/c.txt":   []byte("hello snapshot"),
		"empty.txt":   {},
		"sub/d/e.bin": randomBytes(70 * 1024),
	})

	s, err := Open(repo)
	if err != nil {
		t.Fatal(err)
	}
	id1, err := s.Backup(src)
	if err != nil {
		t.Fatal(err)
	}
	chunksAfterFirst := countChunks(t, repo)
	t.Logf("首次备份后块数=%d（a.bin 与 b.bin 内容相同应去重）", chunksAfterFirst)

	// 源目录新增一个文件后再备份：已有内容不应产生新块。
	writeSource(t, src, map[string][]byte{"new.txt": []byte("brand new file")})
	id2, err := s.Backup(src)
	if err != nil {
		t.Fatal(err)
	}
	chunksAfterSecond := countChunks(t, repo)
	t.Logf("二次备份后块数=%d（仅新增 1 个块）", chunksAfterSecond)
	if chunksAfterSecond != chunksAfterFirst+1 {
		t.Fatalf("去重失效：期望新增 1 块，实际 %d -> %d", chunksAfterFirst, chunksAfterSecond)
	}

	// 两次快照各自独立恢复，结果必须与当时源数据完全一致。
	snap1 := readTree(t, src)
	delete(snap1, "new.txt") // id1 时刻还没有 new.txt
	for _, tc := range []struct {
		id   string
		want map[string][]byte
	}{ {id1, snap1}, {id2, readTree(t, src)} } {
		dest := t.TempDir()
		if err := s.Restore(tc.id, dest); err != nil {
			t.Fatalf("restore %s: %v", tc.id, err)
		}
		got := readTree(t, dest)
		if len(got) != len(tc.want) {
			t.Fatalf("restore %s: 文件数 %d != %d（缺块或多出数据）", tc.id, len(got), len(tc.want))
		}
		for name, want := range tc.want {
			if !bytes.Equal(got[name], want) {
				t.Fatalf("restore %s: 文件 %s 内容错位", tc.id, name)
			}
		}
		t.Logf("快照 %s 恢复校验通过：%d 个文件逐字节一致", tc.id, len(tc.want))
	}
}

// ---- 2. 并发备份与恢复 ----

func TestConcurrentBackupAndRestore(t *testing.T) {
	repo := t.TempDir()
	s, err := Open(repo)
	if err != nil {
		t.Fatal(err)
	}

	// 准备 N 个内容部分重叠的源目录。
	const n = 6
	srcs := make([]string, n)
	shared := randomBytes(128 * 1024)
	for i := 0; i < n; i++ {
		srcs[i] = t.TempDir()
		writeSource(t, srcs[i], map[string][]byte{
			"shared.bin": shared,
			fmt.Sprintf("uniq-%d.bin", i): randomBytes(32 * 1024),
		})
	}

	// 并发备份。
	ids := make([]string, n)
	var wg sync.WaitGroup
	errs := make(chan error, 2*n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id, err := s.Backup(srcs[i])
			if err != nil {
				errs <- err
				return
			}
			ids[i] = id
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	t.Logf("并发完成 %d 个备份，块总数=%d（shared.bin 应只存一份）", n, countChunks(t, repo))

	// 并发恢复并逐字节校验。
	wg = sync.WaitGroup{}
	errs = make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			dest := t.TempDir()
			if err := s.Restore(ids[i], dest); err != nil {
				errs <- fmt.Errorf("restore %d: %w", i, err)
				return
			}
			got, want := readTree(t, dest), readTree(t, srcs[i])
			if len(got) != len(want) {
				errs <- fmt.Errorf("restore %d: 文件数不符", i)
				return
			}
			for name, w := range want {
				if !bytes.Equal(got[name], w) {
					errs <- fmt.Errorf("restore %d: %s 内容不一致", i, name)
					return
				}
			}
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	t.Logf("并发恢复 %d 个快照全部逐字节一致，未出现相互覆盖或中间状态", n)
}

// ---- 3. 异常退出后重启 ----

func TestCrashRecovery(t *testing.T) {
	repo := t.TempDir()
	src := t.TempDir()
	writeSource(t, src, map[string][]byte{"a.bin": randomBytes(100 * 1024)})

	s, err := Open(repo)
	if err != nil {
		t.Fatal(err)
	}
	committed, err := s.Backup(src)
	if err != nil {
		t.Fatal(err)
	}

	// 模拟进程在备份中途崩溃：留下暂存清单、孤儿块和写一半的临时文件。
	crash := &Manifest{ID: "crashed-backup", Files: []FileEntry{{
		Path: "x.bin", Size: 3,
		Chunks: []ChunkRef{{Hash: hashBytes([]byte("xyz")), Size: 3}},
	}}}
	if err := s.writeStaging(crash); err != nil {
		t.Fatal(err)
	}
	if _, err := s.writeChunk([]byte("orphan-chunk-data")); err != nil {
		t.Fatal(err)
	}
	tmpHalf := filepath.Join(repo, "chunks", "ab", ".tmp-halfwritten")
	os.MkdirAll(filepath.Dir(tmpHalf), 0o755)
	os.WriteFile(tmpHalf, []byte("partial"), 0o644)
	t.Logf("崩溃现场：暂存清单 1 个、孤儿块 1 个、半成品临时文件 1 个")

	// 重启：Open 应清理崩溃痕迹，已提交快照必须可用。
	s2, err := Open(repo)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(tmpHalf); !os.IsNotExist(err) {
		t.Fatal("重启后残留写一半的临时文件")
	}
	if entries, _ := os.ReadDir(filepath.Join(repo, "staging")); len(entries) != 0 {
		t.Fatal("重启后暂存区未清理")
	}
	snaps, err := s2.Snapshots()
	if err != nil {
		t.Fatal(err)
	}
	if len(snaps) != 1 || snaps[0].ID != committed {
		t.Fatalf("已提交快照丢失或未完成快照被当成有效数据: %+v", snaps)
	}
	t.Logf("重启后仅见已提交快照 %s，未提交备份未被当成有效数据", committed)

	// 已提交快照重启后仍可恢复。
	dest := t.TempDir()
	if err := s2.Restore(committed, dest); err != nil {
		t.Fatal(err)
	}
	if got, want := readTree(t, dest), readTree(t, src); !bytes.Equal(got["a.bin"], want["a.bin"]) {
		t.Fatal("重启后恢复内容不一致")
	}

	// 孤儿块不被误当成有效数据，GC 可回收；回收后快照仍正常。
	stats, err := s2.GC()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("GC 回收孤儿块 %d 个 / %d 字节", stats.RemovedChunks, stats.Reclaimed)
	if stats.RemovedChunks != 1 {
		t.Fatalf("期望回收 1 个孤儿块，实际 %d", stats.RemovedChunks)
	}
	if err := s2.Verify(committed); err != nil {
		t.Fatalf("GC 后已提交快照校验失败: %v", err)
	}
}

// ---- 4. 损坏/缺失数据被拒绝 ----

func TestCorruptAndMissingDataRejected(t *testing.T) {
	repo := t.TempDir()
	src := t.TempDir()
	payload := randomBytes(100 * 1024)
	writeSource(t, src, map[string][]byte{"a.bin": payload})

	s, err := Open(repo)
	if err != nil {
		t.Fatal(err)
	}
	id, err := s.Backup(src)
	if err != nil {
		t.Fatal(err)
	}
	m, err := s.loadManifest(id)
	if err != nil {
		t.Fatal(err)
	}
	target := m.Files[0].Chunks[0]

	// 场景 A：块被篡改 -> 必须报 ErrChunkCorrupt，且不能返回有效结果。
	corruptPath := s.chunkPath(target.Hash)
	orig, _ := os.ReadFile(corruptPath)
	tampered := append([]byte(nil), orig...)
	tampered[0] ^= 0xFF
	if err := os.WriteFile(corruptPath, tampered, 0o644); err != nil {
		t.Fatal(err)
	}
	err = s.Restore(id, t.TempDir())
	if !errors.Is(err, ErrChunkCorrupt) {
		t.Fatalf("篡改块未被识别为损坏: %v", err)
	}
	t.Logf("篡改块被拒绝，失败原因可区分: %v", err)
	if verr := s.Verify(id); !errors.Is(verr, ErrChunkCorrupt) {
		t.Fatalf("Verify 未报损坏: %v", verr)
	}

	// 场景 B：块被删除 -> 必须报 ErrChunkMissing。
	os.Remove(corruptPath)
	err = s.Restore(id, t.TempDir())
	if !errors.Is(err, ErrChunkMissing) {
		t.Fatalf("缺失块未被识别: %v", err)
	}
	t.Logf("缺失块被拒绝，失败原因可区分: %v", err)

	// 场景 C：清单损坏 -> ErrSnapshotCorrupt。
	os.WriteFile(s.manifestPath(id), []byte("{not-json"), 0o644)
	if _, err := s.loadManifest(id); !errors.Is(err, ErrSnapshotCorrupt) {
		t.Fatalf("损坏清单未被识别: %v", err)
	}
	t.Logf("损坏清单被拒绝，失败原因可区分")

	// 场景 D：不存在的快照 -> ErrSnapshotNotFound。
	if err := s.Restore("no-such-id", t.TempDir()); !errors.Is(err, ErrSnapshotNotFound) {
		t.Fatalf("不存在快照未被识别: %v", err)
	}
}

// ---- 5. GC 容量回收 ----

func TestGCReclaimsOnlyUnreferenced(t *testing.T) {
	repo := t.TempDir()
	src := t.TempDir()
	shared := randomBytes(80 * 1024)
	writeSource(t, src, map[string][]byte{"shared.bin": shared, "v1.bin": randomBytes(40 * 1024)})

	s, err := Open(repo)
	if err != nil {
		t.Fatal(err)
	}
	id1, err := s.Backup(src)
	if err != nil {
		t.Fatal(err)
	}
	// 第二次备份：shared.bin 保留，v1.bin 换成 v2.bin。
	os.Remove(filepath.Join(src, "v1.bin"))
	writeSource(t, src, map[string][]byte{"v2.bin": randomBytes(40 * 1024)})
	id2, err := s.Backup(src)
	if err != nil {
		t.Fatal(err)
	}
	before := countChunks(t, repo)

	// 删除 id1 后 GC：v1.bin 的块应被回收，shared.bin（仍被 id2 引用）必须保留。
	if err := s.Delete(id1); err != nil {
		t.Fatal(err)
	}
	stats, err := s.GC()
	if err != nil {
		t.Fatal(err)
	}
	after := countChunks(t, repo)
	t.Logf("GC 前块数=%d，GC 后=%d，回收 %d 块 / %d 字节", before, after, stats.RemovedChunks, stats.Reclaimed)
	if stats.RemovedChunks == 0 {
		t.Fatal("GC 未回收任何孤儿块")
	}
	if err := s.Verify(id2); err != nil {
		t.Fatalf("GC 误回收了仍被引用的块: %v", err)
	}
	// 回收后 id2 依然能完整恢复。
	dest := t.TempDir()
	if err := s.Restore(id2, dest); err != nil {
		t.Fatal(err)
	}
	if got, want := readTree(t, dest), readTree(t, src); !bytes.Equal(got["shared.bin"], want["shared.bin"]) {
		t.Fatal("GC 后恢复内容不一致")
	}
	t.Logf("GC 后快照 %s 仍可完整恢复，仍被引用的块未被误回收", id2)
}
