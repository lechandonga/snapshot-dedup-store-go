package snapshot

import (
	"bytes"
	"crypto/rand"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"
)

// writeSource 构造一个数据源目录。
func writeSource(t *testing.T, files map[string][]byte) string {
	t.Helper()
	dir := t.TempDir()
	for name, data := range files {
		p := filepath.Join(dir, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, data, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

// readTree 读取目录内容为 path->data 映射，用于逐字节比对。
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

func assertTreeEqual(t *testing.T, want, got map[string][]byte) {
	t.Helper()
	if len(want) != len(got) {
		t.Fatalf("文件数不一致: want %d got %d", len(want), len(got))
	}
	for name, wdata := range want {
		gdata, ok := got[name]
		if !ok {
			t.Fatalf("恢复结果缺少文件 %s", name)
		}
		if !bytes.Equal(wdata, gdata) {
			t.Fatalf("文件 %s 内容不一致: want %d bytes got %d bytes", name, len(wdata), len(gdata))
		}
	}
}

func countObjects(t *testing.T, root string) int {
	t.Helper()
	n := 0
	err := filepath.WalkDir(filepath.Join(root, "objects"), func(p string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() {
			n++
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func randBytes(n int) []byte {
	b := make([]byte, n)
	rand.Read(b)
	return b
}

// TestDedup 验证重复内容只存一份，且每次快照可独立完整恢复。
func TestDedup(t *testing.T) {
	repo := t.TempDir()
	s, err := OpenStore(repo)
	if err != nil {
		t.Fatal(err)
	}
	shared := randBytes(3 * chunkSize) // 跨多块的大文件
	v1 := map[string][]byte{
		"a.txt":       []byte("hello"),
		"dir/big.bin": shared,
	}
	src := writeSource(t, v1)

	id1, err := s.Backup(src)
	if err != nil {
		t.Fatal(err)
	}
	n1 := countObjects(t, repo)
	t.Logf("第一次快照 %s 后对象数=%d（hello 1 块 + big.bin 3 块）", id1, n1)

	// 第二次快照：内容完全不变，应全部去重。
	id2, err := s.Backup(src)
	if err != nil {
		t.Fatal(err)
	}
	n2 := countObjects(t, repo)
	t.Logf("第二次快照 %s 后对象数=%d", id2, n2)
	if n2 != n1 {
		t.Fatalf("重复内容未去重: 对象数从 %d 变为 %d", n1, n2)
	}

	// 第三次快照：改一个文件，shared 部分仍应去重。
	v2data := []byte("hello world")
	if err := os.WriteFile(filepath.Join(src, "a.txt"), v2data, 0o644); err != nil {
		t.Fatal(err)
	}
	id3, err := s.Backup(src)
	if err != nil {
		t.Fatal(err)
	}
	n3 := countObjects(t, repo)
	t.Logf("第三次快照 %s（修改 a.txt）后对象数=%d，仅新增 1 块", id3, n3)
	if n3 != n1+1 {
		t.Fatalf("部分修改未按块去重: 对象数=%d，期望 %d", n3, n1+1)
	}

	// 三次快照各自独立恢复，内容必须与当时源数据一致。
	dst1 := t.TempDir()
	if err := s.Restore(id1, dst1); err != nil {
		t.Fatal(err)
	}
	assertTreeEqual(t, v1, readTree(t, dst1))
	t.Logf("快照 %s 恢复校验通过（%d 个文件逐字节一致）", id1, len(v1))

	dst3 := t.TempDir()
	if err := s.Restore(id3, dst3); err != nil {
		t.Fatal(err)
	}
	got3 := readTree(t, dst3)
	assertTreeEqual(t, map[string][]byte{"a.txt": v2data, "dir/big.bin": shared}, got3)
	t.Logf("快照 %s 恢复校验通过，未出现内容错位或缺块", id3)
}

// TestConcurrentBackupRestore 验证并发备份与恢复互不干扰。
func TestConcurrentBackupRestore(t *testing.T) {
	repo := t.TempDir()
	s, err := OpenStore(repo)
	if err != nil {
		t.Fatal(err)
	}
	base := map[string][]byte{
		"shared.bin": randBytes(2 * chunkSize),
		"common.txt": []byte("common"),
	}

	const workers = 8
	ids := make([]string, workers)
	want := make([]map[string][]byte, workers)
	var wg sync.WaitGroup
	errs := make([]error, workers)
	for i := 0; i < workers; i++ {
		// 每个协程的源 = 共享内容 + 自己的私有文件。
		files := map[string][]byte{}
		for k, v := range base {
			files[k] = v
		}
		files[fmt.Sprintf("private-%d.dat", i)] = randBytes(chunkSize + i)
		want[i] = files
		d := writeSource(t, files)
		wg.Add(1)
		go func(i int, dir string) {
			defer wg.Done()
			ids[i], errs[i] = s.Backup(dir)
		}(i, d)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("并发备份 %d 失败: %v", i, err)
		}
	}
	t.Logf("%d 个并发备份全部提交: %v", workers, ids)

	// 并发恢复全部快照并逐一校验。
	var wg2 sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg2.Add(1)
		go func(i int) {
			defer wg2.Done()
			dst := filepath.Join(t.TempDir(), "out")
			if err := s.Restore(ids[i], dst); err != nil {
				errs[i] = err
				return
			}
			got := readTree(t, dst)
			if len(got) != len(want[i]) {
				errs[i] = fmt.Errorf("文件数不一致 want %d got %d", len(want[i]), len(got))
				return
			}
			for name, w := range want[i] {
				if !bytes.Equal(got[name], w) {
					errs[i] = fmt.Errorf("文件 %s 内容被并发污染", name)
					return
				}
			}
		}(i)
	}
	wg2.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("并发恢复 %d 失败: %v", i, err)
		}
	}
	t.Logf("%d 个并发恢复全部逐字节校验通过，无相互覆盖、无半成品", workers)
}

// TestCrashRecovery 验证进程异常退出后：已提交快照可用，未完成状态被清理。
func TestCrashRecovery(t *testing.T) {
	repo := t.TempDir()
	s, err := OpenStore(repo)
	if err != nil {
		t.Fatal(err)
	}
	src := writeSource(t, map[string][]byte{"f.txt": []byte("committed data")})
	id, err := s.Backup(src)
	if err != nil {
		t.Fatal(err)
	}

	// 模拟崩溃现场：tmp 里残留写了一半的块和未提交的清单。
	tmpDir := filepath.Join(repo, "tmp")
	os.WriteFile(filepath.Join(tmpDir, "chunk-half"), []byte("partial"), 0o644)
	os.WriteFile(filepath.Join(tmpDir, "manifest-half"), []byte(`{"id":"ghost"`), 0o644)
	t.Logf("注入崩溃残留: tmp/chunk-half, tmp/manifest-half")

	// 重启（重新 OpenStore）。
	s2, err := OpenStore(repo)
	if err != nil {
		t.Fatal(err)
	}
	ents, _ := os.ReadDir(tmpDir)
	if len(ents) != 0 {
		t.Fatalf("重启后 tmp 未清理，残留 %d 项", len(ents))
	}
	t.Logf("重启后 tmp 已清空")

	// 已提交快照仍可完整恢复。
	dst := t.TempDir()
	if err := s2.Restore(id, dst); err != nil {
		t.Fatalf("已提交快照重启后不可用: %v", err)
	}
	assertTreeEqual(t, map[string][]byte{"f.txt": []byte("committed data")}, readTree(t, dst))
	t.Logf("已提交快照 %s 重启后恢复校验通过", id)

	// 未提交的清单不可见。
	ids, err := s2.listManifests()
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 1 || ids[0] != id {
		t.Fatalf("清单列表异常: %v", ids)
	}
	if _, err := s2.loadManifest("ghost"); !errors.Is(err, ErrSnapshotNotFound) {
		t.Fatalf("未提交快照被当成有效数据: err=%v", err)
	}
	t.Logf("未提交快照 ghost 被正确拒绝（ErrSnapshotNotFound）")
}

// TestCorruptionRejected 验证缺失/损坏数据被以可区分的原因拒绝。
func TestCorruptionRejected(t *testing.T) {
	repo := t.TempDir()
	s, err := OpenStore(repo)
	if err != nil {
		t.Fatal(err)
	}
	src := writeSource(t, map[string][]byte{"f.bin": randBytes(chunkSize)})
	id, err := s.Backup(src)
	if err != nil {
		t.Fatal(err)
	}
	m, err := s.loadManifest(id)
	if err != nil {
		t.Fatal(err)
	}
	h := m.Files[0].Chunks[0]
	objPath := s.objectPath(h)

	// 篡改块内容 → ErrChunkCorrupt。
	orig, _ := os.ReadFile(objPath)
	tampered := append([]byte(nil), orig...)
	tampered[0] ^= 0xff
	if err := os.WriteFile(objPath, tampered, 0o644); err != nil {
		t.Fatal(err)
	}
	err = s.Restore(id, t.TempDir())
	if !errors.Is(err, ErrChunkCorrupt) {
		t.Fatalf("篡改后期望 ErrChunkCorrupt，得到 %v", err)
	}
	t.Logf("篡改块被拒绝: %v", err)

	// 删除块 → ErrChunkMissing。
	os.Remove(objPath)
	err = s.Restore(id, t.TempDir())
	if !errors.Is(err, ErrChunkMissing) {
		t.Fatalf("删除后期望 ErrChunkMissing，得到 %v", err)
	}
	t.Logf("缺失块被拒绝: %v", err)

	// 恢复原始内容后快照重新可用。
	if _, err := s.PutChunk(orig); err != nil {
		t.Fatal(err)
	}
	dst := t.TempDir()
	if err := s.Restore(id, dst); err != nil {
		t.Fatal(err)
	}
	assertTreeEqual(t, readTree(t, src), readTree(t, dst))
	t.Logf("块修复后快照恢复如初")
}

// TestGC 验证回收只清掉无引用的块，已有快照不受影响。
func TestGC(t *testing.T) {
	repo := t.TempDir()
	s, err := OpenStore(repo)
	if err != nil {
		t.Fatal(err)
	}
	keep := randBytes(chunkSize)
	src := writeSource(t, map[string][]byte{"keep.bin": keep, "drop.bin": randBytes(chunkSize)})
	id1, err := s.Backup(src)
	if err != nil {
		t.Fatal(err)
	}
	// 第二个快照只保留 keep.bin；drop.bin 的块从此无引用。
	src2 := writeSource(t, map[string][]byte{"keep.bin": keep})
	id2, err := s.Backup(src2)
	if err != nil {
		t.Fatal(err)
	}
	before := countObjects(t, repo)

	// 删除第一个快照清单（模拟快照过期），drop.bin 的块成为垃圾。
	if err := os.Remove(s.manifestPath(id1)); err != nil {
		t.Fatal(err)
	}
	removed, err := s.gc(0) // 测试用零宽限期
	if err != nil {
		t.Fatal(err)
	}
	after := countObjects(t, repo)
	t.Logf("GC 前对象数=%d，回收 %d 块，GC 后对象数=%d", before, removed, after)
	if removed != 1 || after != 1 {
		t.Fatalf("GC 结果异常: removed=%d after=%d，期望各为 1", removed, after)
	}

	// 仍被引用的快照恢复不受影响。
	dst := t.TempDir()
	if err := s.Restore(id2, dst); err != nil {
		t.Fatalf("GC 后存活快照恢复失败: %v", err)
	}
	assertTreeEqual(t, map[string][]byte{"keep.bin": keep}, readTree(t, dst))
	t.Logf("GC 后快照 %s 仍完整可恢复，被引用块未被误回收", id2)

	// 已删除的快照因缺块被明确拒绝。
	if err := s.Restore(id1, t.TempDir()); !errors.Is(err, ErrSnapshotNotFound) {
		t.Fatalf("已删除快照应报 ErrSnapshotNotFound，得到 %v", err)
	}
}

// TestManifestListingSorted 快照列表可用于容量管理遍历。
func TestManifestListingSorted(t *testing.T) {
	repo := t.TempDir()
	s, err := OpenStore(repo)
	if err != nil {
		t.Fatal(err)
	}
	src := writeSource(t, map[string][]byte{"x": []byte("x")})
	var ids []string
	for i := 0; i < 3; i++ {
		id, err := s.Backup(src)
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	listed, err := s.listManifests()
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(ids)
	sort.Strings(listed)
	if fmt.Sprint(ids) != fmt.Sprint(listed) {
		t.Fatalf("清单列表不一致: %v vs %v", ids, listed)
	}
	t.Logf("3 次快照全部在列表中: %v", listed)
}
