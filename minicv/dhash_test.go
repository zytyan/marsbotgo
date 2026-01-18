package minicv

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"image"
	"image/png"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

func loadTestImage(t *testing.T) []byte {
	t.Helper()
	path := filepath.Join("testdata", "1x1.png")
	data, err := os.ReadFile(path)
	if err == nil {
		if _, _, err := image.Decode(bytes.NewReader(data)); err == nil {
			return data
		}
	}

	img := image.NewRGBA(image.Rect(0, 0, 1, 1))
	var buf bytes.Buffer
	if err := png.Encode(&buf, img); err != nil {
		t.Fatalf("encode fallback image: %v", err)
	}
	return buf.Bytes()
}

func readRSS(t *testing.T) uint64 {
	t.Helper()
	statm, err := os.ReadFile("/proc/self/statm")
	if err != nil {
		t.Skipf("statm unavailable: %v", err)
	}
	var size, resident uint64
	if _, err := fmt.Sscanf(string(statm), "%d %d", &size, &resident); err != nil {
		t.Skipf("parse statm: %v", err)
	}
	return resident * uint64(os.Getpagesize())
}

func TestDHashBytesDeterministic(t *testing.T) {
	img := loadTestImage(t)
	dhash, err := DHashBytes(img)
	if err != nil {
		t.Fatalf("DHashBytes failed: %v", err)
	}
	if hex.EncodeToString(dhash[:]) != "0000000000000000" {
		t.Fatalf("unexpected dhash: %x", dhash)
	}
}

func TestDHashBytesNoCLibraryLeak(t *testing.T) {
	img := loadTestImage(t)
	runtime.GC()
	before := readRSS(t)

	const iterations = 5000
	for i := 0; i < iterations; i++ {
		if _, err := DHashBytes(img); err != nil {
			t.Fatalf("DHashBytes failed at %d: %v", i, err)
		}
	}
	runtime.GC()
	time.Sleep(100 * time.Millisecond) // allow finalizers to run
	after := readRSS(t)

	const maxGrowth = 32 * 1024 * 1024 // 32MB headroom
	if after > before+maxGrowth {
		t.Fatalf("RSS grew too much: before=%d after=%d (+%d)", before, after, after-before)
	}
}
