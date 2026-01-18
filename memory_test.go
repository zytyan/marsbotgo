package marsbot

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"marsbot/minicv"
	"marsbot/q"

	_ "github.com/mattn/go-sqlite3"
)

const (
	imageGlobEnv     = "MARS_IMAGE_GLOB"
	defaultImageGlob = "testdata/*.jpg"
)

func init() {
	registerSQLiteDriver("./build/libhammdist")
}

func TestMemoryUsageManyImages(t *testing.T) {
	imageGlob := os.Getenv(imageGlobEnv)
	if imageGlob == "" {
		imageGlob = defaultImageGlob
	}
	dbPath := os.Getenv("MARS_DB_PATH")
	if dbPath == "" {
		dbPath = "mars.db"
	}
	if _, err := os.Stat(dbPath); err != nil {
		t.Skipf("mars db not found at %s: %v", dbPath, err)
	}
	files, err := filepath.Glob(imageGlob)
	if err != nil {
		t.Fatalf("glob %s: %v", imageGlob, err)
	}
	if len(files) == 0 {
		t.Skipf("no images matched %s", imageGlob)
	}

	tmpDB := filepath.Join(t.TempDir(), "mars.db")
	if err := copyFile(tmpDB, dbPath); err != nil {
		t.Fatalf("copy mars db: %v", err)
	}
	db, err := sql.Open(sqliteDriverName, tmpDB)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	if err := applyPragmas(db); err != nil {
		t.Fatalf("apply pragmas: %v", err)
	}
	ctx := context.Background()
	queries, err := q.Prepare(ctx, db)
	if err != nil {
		t.Fatalf("prepare queries: %v", err)
	}
	defer queries.Close()

	runtime.GC()
	before := readRSS()
	maxRSS := before
	start := time.Now()

	groupID := int64(1)
	msgID := int64(1)
	const reportEvery = 50

	for i, path := range files {
		dhash, err := minicv.DHashFile(path)
		if err != nil {
			t.Fatalf("dhash %s: %v", path, err)
		}
		fuid := filepath.ToSlash(path)
		if err := queries.UpsertDhash(ctx, fuid, dhash[:]); err != nil {
			t.Fatalf("upsert dhash %s: %v", path, err)
		}
		if _, err := queries.IncrementMarsInfo(ctx, groupID, dhash[:], msgID); err != nil {
			t.Fatalf("increment mars info %s: %v", path, err)
		}
		if err := queries.IncrementGroupStat(ctx, groupID); err != nil {
			t.Fatalf("increment group stat %s: %v", path, err)
		}
		msgID++
		if (i+1)%reportEvery == 0 {
			rss := readRSS()
			if rss > maxRSS {
				maxRSS = rss
			}
			var memStat runtime.MemStats
			runtime.ReadMemStats(&memStat)
			t.Logf("processed=%d rss=%dMB elapsed=%s, heapInuse=%dMB, heapSys=%dMB",
				i+1, rss/(1024*1024), time.Since(start).Truncate(time.Second),
				memStat.HeapInuse/(1024*1024), memStat.HeapSys/(1024*1024))
		}
	}

	runtime.GC()
	time.Sleep(100 * time.Millisecond)
	after := readRSS()
	if after > maxRSS {
		maxRSS = after
	}
	t.Logf("done files=%d rss_before=%dMB rss_after=%dMB rss_max=%dMB elapsed=%s",
		len(files),
		before/(1024*1024),
		after/(1024*1024),
		maxRSS/(1024*1024),
		time.Since(start).Truncate(time.Second),
	)
}

func applyPragmas(db *sql.DB) error {
	pragmas := []string{
		"PRAGMA journal_mode=WAL;",
		"PRAGMA synchronous=OFF;",
		"PRAGMA busy_timeout=5000;",
		"PRAGMA cache_size=-20000;", // 20MB
	}
	for _, p := range pragmas {
		if _, err := db.Exec(p); err != nil {
			return fmt.Errorf("pragma %q: %w", p, err)
		}
	}
	return nil
}

func copyFile(dst, src string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	out, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer func() {
		_ = out.Close()
	}()

	if _, err := io.Copy(out, in); err != nil {
		return err
	}
	return out.Sync()
}

func readRSS() uint64 {
	statm, err := os.ReadFile("/proc/self/statm")
	if err != nil {
		panic(fmt.Sprintf("statm unavailable: %v", err))
	}
	var size, resident uint64
	if _, err := fmt.Sscanf(string(statm), "%d %d", &size, &resident); err != nil {
		panic(fmt.Sprintf("parse statm: %v", err))
	}
	return resident * uint64(os.Getpagesize())
}
