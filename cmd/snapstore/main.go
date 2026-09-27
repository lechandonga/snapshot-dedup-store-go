package main

import (
	"errors"
	"fmt"
	"os"

	"github.com/lechandonga/snapshot-dedup-store-go/internal/store"
)

func usage() {
	fmt.Fprintln(os.Stderr, `用法: snapstore -repo <仓库目录> <命令> [参数]

命令:
  backup <源目录>            对源目录做一次去重快照，打印快照 ID
  restore <快照ID> <目标目录> 恢复快照到目标目录
  list                       列出所有已提交快照
  delete <快照ID>            删除快照（数据块由 gc 回收）
  verify <快照ID>            校验快照完整性
  gc                         回收不再被引用的数据块`)
	os.Exit(2)
}

func main() {
	if len(os.Args) < 4 || os.Args[1] != "-repo" {
		usage()
	}
	repo, cmd, args := os.Args[2], os.Args[3], os.Args[4:]
	s, err := store.Open(repo)
	if err != nil {
		fatal(err)
	}
	switch cmd {
	case "backup":
		if len(args) != 1 {
			usage()
		}
		id, err := s.Backup(args[0])
		if err != nil {
			fatal(err)
		}
		fmt.Println(id)
	case "restore":
		if len(args) != 2 {
			usage()
		}
		fatal(s.Restore(args[0], args[1]))
	case "list":
		ms, err := s.Snapshots()
		if err != nil {
			fatal(err)
		}
		for _, m := range ms {
			fmt.Printf("%s  %s  files=%d  source=%s\n", m.ID, m.CreatedAt.Format("2006-01-02 15:04:05"), len(m.Files), m.Source)
		}
	case "delete":
		if len(args) != 1 {
			usage()
		}
		fatal(s.Delete(args[0]))
	case "verify":
		if len(args) != 1 {
			usage()
		}
		fatal(s.Verify(args[0]))
		fmt.Println("ok")
	case "gc":
		stats, err := s.GC()
		if err != nil {
			fatal(err)
		}
		fmt.Printf("removed %d chunks, reclaimed %d bytes\n", stats.RemovedChunks, stats.Reclaimed)
	default:
		usage()
	}
}

func fatal(err error) {
	if err == nil {
		return
	}
	switch {
	case errors.Is(err, store.ErrChunkMissing):
		fmt.Fprintln(os.Stderr, "失败[数据块缺失]:", err)
	case errors.Is(err, store.ErrChunkCorrupt):
		fmt.Fprintln(os.Stderr, "失败[数据块损坏]:", err)
	case errors.Is(err, store.ErrSnapshotNotFound):
		fmt.Fprintln(os.Stderr, "失败[快照不存在]:", err)
	case errors.Is(err, store.ErrSnapshotCorrupt):
		fmt.Fprintln(os.Stderr, "失败[快照清单损坏]:", err)
	default:
		fmt.Fprintln(os.Stderr, "失败:", err)
	}
	os.Exit(1)
}
