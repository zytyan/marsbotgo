package minicv

/*
#cgo CFLAGS: -O2 -march=native -Wall -Wextra -std=c11 -g -ffast-math
#cgo LDFLAGS: -lm
#include "mini_cv.h"
*/
import "C"
import (
	"errors"
	"io"
	"os"
	"unsafe"
)

func DHashFile(path string) (out [8]byte, err error) {
	f, err := os.Open(path)
	if err != nil {
		return out, err
	}
	defer f.Close()

	data, err := io.ReadAll(f)
	if err != nil {
		return out, err
	}
	if len(data) == 0 {
		return out, errors.New("empty file")
	}

	ret := C.mini_dhash_from_bytes(
		(*C.uchar)(unsafe.Pointer(&data[0])),
		C.size_t(len(data)),
		(*C.uchar)(unsafe.Pointer(&out[0])),
	)
	if ret != 0 {
		return out, errors.New("C function mini_dhash_from_bytes failed")
	}
	return out, nil
}

// DHashBytes computes a dhash for the provided image bytes without touching disk.
func DHashBytes(data []byte) (out [8]byte, err error) {
	if len(data) == 0 {
		return out, errors.New("empty image data")
	}
	ret := C.mini_dhash_from_bytes(
		(*C.uchar)(unsafe.Pointer(&data[0])),
		C.size_t(len(data)),
		(*C.uchar)(unsafe.Pointer(&out[0])),
	)
	if ret != 0 {
		return out, errors.New("C function mini_dhash_from_bytes failed")
	}
	return out, nil
}
