package main

/*
#include <stdint.h>
int malloc_trim(size_t sz);
*/
import "C"

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/DataDog/zstd"
	"github.com/mattn/go-sqlite3"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"go.uber.org/zap"
)

var (
	backupStopCh chan struct{}
)

// StartBackupThread launches a background ticker that performs a SQLite backup and uploads it to S3.
// Safe to call multiple times; only the first call starts the goroutine.
func StartBackupThread() {
	if config.NoBackup {
		logger.Info("NO_BACKUP is set; skipping backup thread")
		return
	}
	if backupStopCh != nil {
		return
	}
	backupStopCh = make(chan struct{})

	interval := time.Duration(config.S3BackupMinutes) * time.Minute
	if interval <= 0 {
		interval = 2880 * time.Minute
	}
	logger.Info("Starting backup thread", zap.Duration("interval", interval))
	go func() {
		if err := BackupAndUpload(context.Background()); err != nil && logger != nil {
			logger.Warn("backup failed", zap.Error(err))
		}
		timer := time.NewTicker(interval)
		for {
			select {
			case <-backupStopCh:
				timer.Stop()
				return
			case <-timer.C:
				if err := BackupAndUpload(context.Background()); err != nil && logger != nil {
					logger.Warn("backup failed", zap.Error(err))
				}
			}
		}
	}()
}

// BackupAndUpload performs a single backup and uploads it to S3 after validating configuration.
func BackupAndUpload(ctx context.Context) error {
	if db == nil {
		return fmt.Errorf("db not initialized")
	}
	if err := ensureS3Configured(); err != nil {
		return err
	}
	ts := time.Now().Format("2006-01-02-15_04_05")
	backupName := fmt.Sprintf("backup_mars_at_%s.db", ts)
	backupPath := filepath.Join(os.TempDir(), backupName)

	if err := backupWithSQLiteAPI(ctx, backupPath); err != nil {
		return err
	}

	compressedPath, err := zstdFile(backupPath)
	_ = os.Remove(backupPath)
	if err != nil {
		return err
	}

	if err := uploadToS3(ctx, compressedPath); err != nil {
		return err
	}
	_ = os.Remove(compressedPath)
	return nil
}

func ensureS3Configured() error {
	switch {
	case config.S3ApiEndpoint == "":
		return fmt.Errorf("S3_API_ENDPOINT is required for backup")
	case config.S3Bucket == "":
		return fmt.Errorf("S3_BUCKET is required for backup")
	case config.S3ApiKeyID == "":
		return fmt.Errorf("S3_API_KEY_ID is required for backup")
	case config.S3ApiKeySecret == "":
		return fmt.Errorf("S3_API_KEY_SECRET is required for backup")
	default:
		return nil
	}
}

func backupWithSQLiteAPI(ctx context.Context, destPath string) error {
	if err := os.Remove(destPath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove existing backup: %w", err)
	}
	defer C.malloc_trim(0)
	destDB, err := sql.Open(sqliteDriverName, destPath)
	if err != nil {
		return fmt.Errorf("open destination db: %w", err)
	}
	defer destDB.Close()

	if err := destDB.PingContext(ctx); err != nil {
		return fmt.Errorf("ping destination db: %w", err)
	}

	srcConn, err := db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("get source connection: %w", err)
	}
	defer srcConn.Close()

	destConn, err := destDB.Conn(ctx)
	if err != nil {
		return fmt.Errorf("get destination connection: %w", err)
	}
	defer destConn.Close()

	if err := destConn.Raw(func(destDriverConn interface{}) error {
		dst, ok := destDriverConn.(*sqlite3.SQLiteConn)
		if !ok {
			return fmt.Errorf("unexpected destination driver connection type %T", destDriverConn)
		}
		return srcConn.Raw(func(srcDriverConn interface{}) error {
			src, ok := srcDriverConn.(*sqlite3.SQLiteConn)
			if !ok {
				return fmt.Errorf("unexpected source driver connection type %T", srcDriverConn)
			}
			backup, err := dst.Backup("main", src, "main")
			if err != nil {
				return fmt.Errorf("start backup: %w", err)
			}
			for {
				done, err := backup.Step(64)
				if err != nil {
					_ = backup.Finish()
					return fmt.Errorf("backup step: %w", err)
				}
				if done {
					break
				}
				time.Sleep(10 * time.Millisecond)
			}
			if err := backup.Finish(); err != nil {
				return fmt.Errorf("finish backup: %w", err)
			}
			return nil
		})
	}); err != nil {
		return err
	}
	return nil
}

func zstdFile(path string) (string, error) {
	in, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer in.Close()

	outPath := path + ".zst"
	out, err := os.Create(outPath)
	if err != nil {
		return "", err
	}
	defer out.Close()

	encoder := zstd.NewWriterLevel(out, 15)
	if _, err = io.Copy(encoder, in); err != nil {
		_ = encoder.Close()
		return "", err
	}
	if err := encoder.Close(); err != nil {
		return "", err
	}
	return outPath, nil
}

func uploadToS3(ctx context.Context, path string) error {
	client, err := minio.New(config.S3ApiEndpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(config.S3ApiKeyID, config.S3ApiKeySecret, ""),
		Secure: true,
	})
	if err != nil {
		return fmt.Errorf("create s3 client: %w", err)
	}

	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()

	stat, err := file.Stat()
	if err != nil {
		return err
	}

	_, err = client.PutObject(ctx, config.S3Bucket, filepath.Base(path), file, stat.Size(), minio.PutObjectOptions{
		ContentType: "application/zstd",
	})
	if err != nil {
		return fmt.Errorf("upload to s3: %w", err)
	}
	if logger != nil {
		logger.Info("backup uploaded to S3", zap.String("bucket", config.S3Bucket), zap.String("key", filepath.Base(path)))
	}
	return nil
}
