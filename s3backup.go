package main

import (
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"go.uber.org/zap"
)

var (
	backupStopCh chan struct{}
)

// StartBackupThread launches a background ticker that performs a VACUUM INTO backup and optionally uploads it to S3.
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

	go func() {
		for {
			select {
			case <-backupStopCh:
				return
			case <-time.After(interval):
				if err := BackupAndUpload(context.Background()); err != nil && logger != nil {
					logger.Warn("backup failed", zap.Error(err))
				}
			}
		}
	}()
}

// BackupAndUpload performs a single backup and uploads it to S3 if credentials are configured.
func BackupAndUpload(ctx context.Context) error {
	if db == nil {
		return fmt.Errorf("db not initialized")
	}
	ts := time.Now().Format("2006-01-02-15_04_05")
	backupName := fmt.Sprintf("backup_mars_at_%s.db", ts)
	backupPath := filepath.Join(os.TempDir(), backupName)

	if err := vacuumInto(ctx, backupPath); err != nil {
		return err
	}

	compressedPath, err := gzipFile(backupPath)
	_ = os.Remove(backupPath)
	if err != nil {
		return err
	}

	if config.S3ApiEndpoint == "" || config.S3Bucket == "" || config.S3ApiKeyID == "" || config.S3ApiKeySecret == "" {
		if logger != nil {
			logger.Info("S3 not configured; backup saved locally", zap.String("path", compressedPath))
		}
		return nil
	}

	if err := uploadToS3(ctx, compressedPath); err != nil {
		return err
	}
	_ = os.Remove(compressedPath)
	return nil
}

func vacuumInto(ctx context.Context, dest string) error {
	// VACUUM INTO creates a consistent copy without closing the main database.
	_, err := db.ExecContext(ctx, fmt.Sprintf("VACUUM INTO '%s'", dest))
	if err != nil {
		return fmt.Errorf("vacuum into backup: %w", err)
	}
	return nil
}

func gzipFile(path string) (string, error) {
	in, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer in.Close()

	outPath := path + ".gz"
	out, err := os.Create(outPath)
	if err != nil {
		return "", err
	}
	defer out.Close()

	gw := gzip.NewWriter(out)
	_, err = io.Copy(gw, in)
	closeErr := gw.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		return "", err
	}
	return outPath, nil
}

func uploadToS3(ctx context.Context, path string) error {
	client, err := minio.New(config.S3ApiEndpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(config.S3ApiKeyID, config.S3ApiKeySecret, ""),
		Secure: strings.HasPrefix(config.S3ApiEndpoint, "https"),
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
		ContentType: "application/gzip",
	})
	if err != nil {
		return fmt.Errorf("upload to s3: %w", err)
	}
	if logger != nil {
		logger.Info("backup uploaded to S3", zap.String("bucket", config.S3Bucket), zap.String("key", filepath.Base(path)))
	}
	return nil
}
