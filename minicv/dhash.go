package minicv

/*
#cgo CFLAGS: -O2 -march=native -Wall -Wextra -std=c11 -g -ffast-math
#cgo LDFLAGS: -lm
#include "mini_cv.h"
*/
import "C"
import (
	"bytes"
	"errors"
	"image"
	"image/draw"
	_ "image/jpeg"
	_ "image/png"
	"math"
	"os"
	"unsafe"
)

func DHashFile(path string) (out [8]byte, err error) {
	f, err := os.Open(path)
	if err != nil {
		return out, err
	}
	defer f.Close()
	img, _, err := image.Decode(f)
	if err != nil {
		return out, err
	}
	return dhashFromImage(img)
}

// DHashBytes computes a dhash for the provided image bytes without touching disk.
func DHashBytes(data []byte) (out [8]byte, err error) {
	if len(data) == 0 {
		return out, errors.New("empty image data")
	}
	img, _, err := image.Decode(bytes.NewReader(data))
	if err != nil {
		return out, err
	}
	return dhashFromImage(img)
}

func dhashFromImage(img image.Image) (out [8]byte, err error) {
	if img == nil {
		return out, errors.New("nil image")
	}
	bounds := img.Bounds()
	width := bounds.Dx()
	height := bounds.Dy()
	if width <= 0 || height <= 0 {
		return out, errors.New("invalid image size")
	}
	if width > math.MaxInt32/4 || height > math.MaxInt32 {
		return out, errors.New("image too large")
	}
	var code C.mini_color_code = C.MINI_RGBA2GRAY
	var input *C.uchar
	var stride int
	switch img := img.(type) {
	case *image.RGBA:
		code = C.MINI_RGBA2GRAY
		input = (*C.uchar)(unsafe.Pointer(&img.Pix[0]))
		stride = img.Stride
	case *image.Gray:
		code = C.MINI_NO_CHANGE
		input = (*C.uchar)(unsafe.Pointer(&img.Pix[0]))
		stride = img.Stride
	default:
		rgba := image.NewRGBA(bounds)
		draw.Draw(rgba, bounds, img, bounds.Min, draw.Src)
		input = (*C.uchar)(unsafe.Pointer(&rgba.Pix[0]))
		stride = rgba.Stride
		code = C.MINI_RGBA2GRAY
	}
	ret := C.mini_dhash_from_raw(
		input,
		C.int(width),
		C.int(height),
		C.int(stride),
		(*C.uchar)(unsafe.Pointer(&out[0])),
		code,
	)
	if ret != 0 {
		return out, errors.New("C function mini_dhash_from_raw failed")
	}
	return out, nil
}
