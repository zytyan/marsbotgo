#ifndef MINI_CV_H
#define MINI_CV_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef enum {
    MINI_BGR2GRAY = 0,
    MINI_RGB2GRAY = 1,
    MINI_GRAY2BGR = 2,
    MINI_GRAY2RGB = 3,
    MINI_BGR2RGB = 4,
    MINI_RGB2BGR = 5,
    MINI_RGBA2GRAY = 6,
    MINI_NO_CHANGE = 7,
} mini_color_code;

int mini_cvtcolor_u8(const uint8_t* src, int width, int height, int src_stride,
                     int src_channels, uint8_t* dst, int dst_stride, int dst_channels,
                     mini_color_code code);

int mini_resize_area_u8(const uint8_t* src, int src_w, int src_h, int src_stride,
                        int channels, uint8_t* dst, int dst_w, int dst_h, int dst_stride);

int mini_dhash_from_raw(const uint8_t* raw, int width, int height, int stride, uint8_t* out_hash, mini_color_code code);

#ifdef __cplusplus
}
#endif

#endif // MINI_CV_H
