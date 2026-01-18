#include "mini_cv.h"

#include <limits.h>
#include <math.h>
#include <stdlib.h>
#include <string.h>

struct mini_decimate_alpha {
    int si;
    int di;
    float alpha;
};

#define MINI_GRAY_SHIFT 15
#define MINI_RY15 9798   /* R2YF * 32768 + 0.5 */
#define MINI_GY15 19235  /* G2YF * 32768 + 0.5 */
#define MINI_BY15 3735   /* B2YF * 32768 + 0.5 */

static inline uint8_t mini_saturate_u8(int v) {
    if (v < 0) return 0;
    if (v > 255) return 255;
    return (uint8_t)v;
}

static inline uint8_t mini_saturate_from_float(float v) {
    int iv = (int)lrintf(v); // round to nearest-even to match OpenCV's SIMD path
    return mini_saturate_u8(iv);
}

static int mini_compute_resize_area_tab(int ssize, int dsize, int cn, double scale,
                                        struct mini_decimate_alpha* tab) {
    int k = 0;
    for (int dx = 0; dx < dsize; ++dx) {
        double fsx1 = dx * scale;
        double fsx2 = fsx1 + scale;
        double cell_width = scale < (ssize - fsx1) ? scale : (ssize - fsx1);

        int sx1 = (int)ceil(fsx1);
        int sx2 = (int)floor(fsx2);

        if (sx2 > ssize - 1) sx2 = ssize - 1;
        if (sx1 > sx2) sx1 = sx2;

        if (sx1 - fsx1 > 1e-3) {
            tab[k].di = dx * cn;
            tab[k].si = (sx1 - 1) * cn;
            tab[k].alpha = (float)((sx1 - fsx1) / cell_width);
            ++k;
        }

        for (int sx = sx1; sx < sx2; ++sx) {
            tab[k].di = dx * cn;
            tab[k].si = sx * cn;
            tab[k].alpha = (float)(1.0 / cell_width);
            ++k;
        }

        if (fsx2 - sx2 > 1e-3) {
            tab[k].di = dx * cn;
            tab[k].si = sx2 * cn;
            double w = fsx2 - sx2;
            if (w > 1.0) w = 1.0;
            if (w > cell_width) w = cell_width;
            tab[k].alpha = (float)(w / cell_width);
            ++k;
        }
    }
    return k;
}

static int mini_resize_area_fast_int(const uint8_t* src, int src_w, int src_h, int src_stride,
                                     int cn, uint8_t* dst, int dst_w, int dst_h, int dst_stride,
                                     int iscale_x, int iscale_y) {
    (void)src_w;
    (void)src_h;
    int area = iscale_x * iscale_y;
    float scale = 1.0f / (float)area;
    if (iscale_x == 2 && iscale_y == 2) {
        for (int dy = 0; dy < dst_h; ++dy) {
            const uint8_t* s0 = src + (dy * 2) * src_stride;
            const uint8_t* s1 = s0 + src_stride;
            uint8_t* drow = dst + dy * dst_stride;
            for (int dx = 0; dx < dst_w; ++dx) {
                const uint8_t* p0 = s0 + dx * 2 * cn;
                const uint8_t* p1 = s1 + dx * 2 * cn;
                for (int c = 0; c < cn; ++c) {
                    int sum = p0[c] + p0[c + cn] + p1[c] + p1[c + cn];
                    drow[dx * cn + c] = (uint8_t)((sum + 2) >> 2);
                }
            }
        }
        return 0;
    }
    for (int dy = 0; dy < dst_h; ++dy) {
        int sy0 = dy * iscale_y;
        const uint8_t* srow0 = src + sy0 * src_stride;
        uint8_t* drow = dst + dy * dst_stride;
        for (int dx = 0; dx < dst_w; ++dx) {
            int sx0 = dx * iscale_x * cn;
            for (int c = 0; c < cn; ++c) {
                int sum = 0;
                for (int ky = 0; ky < iscale_y; ++ky) {
                    const uint8_t* srow = srow0 + ky * src_stride;
                    const uint8_t* sp = srow + sx0 + c;
                    for (int kx = 0; kx < iscale_x; ++kx) {
                        sum += sp[kx * cn];
                    }
                }
                drow[dx * cn + c] = mini_saturate_from_float((float)sum * scale);
            }
        }
    }
    return 0;
}

static int mini_resize_area_down(const uint8_t* src, int src_w, int src_h, int src_stride,
                                 int cn, uint8_t* dst, int dst_w, int dst_h, int dst_stride) {
    double scale_x = (double)src_w / (double)dst_w;
    double scale_y = (double)src_h / (double)dst_h;

    int iscale_x = (int)lround(scale_x);
    int iscale_y = (int)lround(scale_y);
    if (fabs(scale_x - (double)iscale_x) < 1e-6 && fabs(scale_y - (double)iscale_y) < 1e-6) {
        // Integer scale fast-path (closest to OpenCV's area_fast path).
        return mini_resize_area_fast_int(src, src_w, src_h, src_stride, cn, dst, dst_w, dst_h,
                                         dst_stride, iscale_x, iscale_y);
    }

    int xtab_capacity = src_w * 2 + 2;
    int ytab_capacity = src_h * 2 + 2;
    struct mini_decimate_alpha* xtab =
        (struct mini_decimate_alpha*)malloc(sizeof(struct mini_decimate_alpha) * xtab_capacity);
    struct mini_decimate_alpha* ytab =
        (struct mini_decimate_alpha*)malloc(sizeof(struct mini_decimate_alpha) * ytab_capacity);
    int* tabofs = (int*)malloc(sizeof(int) * (dst_h + 1));
    float* float_buf = (float*)malloc(sizeof(float) * dst_w * cn * 2);
    if (!xtab || !ytab || !tabofs || !float_buf) {
        free(xtab);
        free(ytab);
        free(tabofs);
        free(float_buf);
        return -1;
    }
    float* buf = float_buf;
    float* sum = float_buf + dst_w * cn;
    int xtab_size = mini_compute_resize_area_tab(src_w, dst_w, cn, scale_x, xtab);
    int ytab_size = mini_compute_resize_area_tab(src_h, dst_h, 1, scale_y, ytab);

    int dy_count = 0;
    int prev_di = -1;
    for (int k = 0; k < ytab_size; ++k) {
        if (k == 0 || ytab[k].di != prev_di) {
            tabofs[dy_count++] = k;
            prev_di = ytab[k].di;
        }
    }
    tabofs[dy_count] = ytab_size;

    for (int dy = 0; dy < dst_h; ++dy) {
        int y_start = tabofs[dy];
        int y_end = tabofs[dy + 1];
        int first = 1;
        for (int jy = y_start; jy < y_end; ++jy) {
            int sy = ytab[jy].si;
            float beta = ytab[jy].alpha;
            const uint8_t* srow = src + sy * src_stride;
            memset(buf, 0, sizeof(float) * dst_w * cn);
            for (int k = 0; k < xtab_size; ++k) {
                int dx = xtab[k].di;
                int sx = xtab[k].si;
                float alpha = xtab[k].alpha;
                const uint8_t* sp = srow + sx;
                float* bp = buf + dx;
                for (int c = 0; c < cn; ++c) {
                    bp[c] += alpha * (float)sp[c];
                }
            }
            if (first) {
                for (int i = 0; i < dst_w * cn; ++i) {
                    sum[i] = buf[i] * beta;
                }
                first = 0;
            } else {
                for (int i = 0; i < dst_w * cn; ++i) {
                    sum[i] += buf[i] * beta;
                }
            }
        }

        uint8_t* drow = dst + dy * dst_stride;
        for (int i = 0; i < dst_w * cn; ++i) {
            drow[i] = mini_saturate_from_float(sum[i]);
        }
    }
    free(xtab);
    free(ytab);
    free(tabofs);
    free(float_buf);
    return 0;
}

static int mini_resize_area_linear(const uint8_t* src, int src_w, int src_h, int src_stride,
                                   int cn, uint8_t* dst, int dst_w, int dst_h, int dst_stride) {
    const int COEF_BITS = 11;
    const int ONE = 1 << COEF_BITS;
    double scale_x = (double)src_w / (double)dst_w;
    double scale_y = (double)src_h / (double)dst_h;
    double inv_scale_x = 1.0 / scale_x;
    double inv_scale_y = 1.0 / scale_y;
    int width = dst_w * cn;
    int* alpha = (int*)malloc(sizeof(int) * width * 2);
    int* beta = (int*)malloc(sizeof(int) * dst_h * 2);
    int* xofs = (int*)malloc(sizeof(int) * width);
    int* yofs = (int*)malloc(sizeof(int) * dst_h);
    if (!alpha || !beta || !xofs || !yofs) {
        free(alpha);
        free(beta);
        free(xofs);
        free(yofs);
        return -1;
    }
    for (int dx = 0; dx < dst_w; ++dx) {
        int sx = 0;
        float fx = 0.0f;
        if (src_w > 1) {
            sx = (int)floor(dx * scale_x);
            fx = (float)((dx + 1) - (sx + 1) * inv_scale_x);
            if (fx <= 0) {
                fx = 0.0f;
            } else {
                fx -= (float)floor((double)fx);
            }
            if (sx < 0) {
                sx = 0;
                fx = 0.0f;
            }
            if (sx >= src_w - 1) {
                sx = src_w - 2;
                fx = 1.0f;
            }
        }
        int w1 = (int)lrintf(fx * (float)ONE);
        int w0 = ONE - w1;
        for (int c = 0; c < cn; ++c) {
            int ofs = dx * cn + c;
            xofs[ofs] = sx * cn + c;
            alpha[ofs * 2] = w0;
            alpha[ofs * 2 + 1] = w1;
        }
    }

    for (int dy = 0; dy < dst_h; ++dy) {
        int sy = 0;
        float fy = 0.0f;
        if (src_h > 1) {
            sy = (int)floor(dy * scale_y);
            fy = (float)((dy + 1) - (sy + 1) * inv_scale_y);
            if (fy <= 0) {
                fy = 0.0f;
            } else {
                fy -= (float)floor((double)fy);
            }
            if (sy < 0) {
                sy = 0;
                fy = 0.0f;
            }
            if (sy >= src_h - 1) {
                sy = src_h - 2;
                fy = 1.0f;
            }
        }
        int w1 = (int)lrintf(fy * (float)ONE);
        int w0 = ONE - w1;
        yofs[dy] = sy;
        beta[dy * 2] = w0;
        beta[dy * 2 + 1] = w1;
    }

    for (int dy = 0; dy < dst_h; ++dy) {
        const uint8_t* srow0 = src + yofs[dy] * src_stride;
        const uint8_t* srow1 = srow0;
        if (src_h > 1) {
            srow1 = src + (yofs[dy] + 1) * src_stride;
        }
        uint8_t* drow = dst + dy * dst_stride;
        int wy0 = beta[dy * 2];
        int wy1 = beta[dy * 2 + 1];
        for (int dx = 0; dx < dst_w; ++dx) {
            int base = dx * cn;
            for (int c = 0; c < cn; ++c) {
                int ofs = base + c;
                int sx0 = xofs[ofs];
                int sx1 = sx0 + cn;
                if (sx1 >= src_w * cn) sx1 = sx0;
                int wx0 = alpha[ofs * 2];
                int wx1 = alpha[ofs * 2 + 1];
                int t0 = wx0 * (int)srow0[sx0] + wx1 * (int)srow0[sx1];
                int t1 = wx0 * (int)srow1[sx0] + wx1 * (int)srow1[sx1];
                int v0 = (wy0 * (t0 >> 4)) >> 16;
                int v1 = (wy1 * (t1 >> 4)) >> 16;
                int v = (v0 + v1 + 2) >> 2;
                drow[ofs] = mini_saturate_u8(v);
            }
        }
    }
    free(alpha);
    free(beta);
    free(xofs);
    free(yofs);
    return 0;
}

int mini_resize_area_u8(const uint8_t* src, int src_w, int src_h, int src_stride,
                        int channels, uint8_t* dst, int dst_w, int dst_h, int dst_stride) {
    if (!src || !dst || src_w <= 0 || src_h <= 0 || dst_w <= 0 || dst_h <= 0 || channels <= 0)
        return -1;

    double scale_x = (double)src_w / (double)dst_w;
    double scale_y = (double)src_h / (double)dst_h;

    if (scale_x >= 1.0 && scale_y >= 1.0) {
        return mini_resize_area_down(src, src_w, src_h, src_stride, channels, dst, dst_w, dst_h,
                                     dst_stride);
    }
    // OpenCV switches to the linear kernel when either axis is upscaled.
    return mini_resize_area_linear(src, src_w, src_h, src_stride, channels, dst, dst_w, dst_h,
                                   dst_stride);
}

static void mini_gray_to_rgb(const uint8_t* src, int width, int height, int src_stride,
                             uint8_t* dst, int dst_stride, int dcn) {
    for (int y = 0; y < height; ++y) {
        const uint8_t* srow = src + y * src_stride;
        uint8_t* drow = dst + y * dst_stride;
        for (int x = 0; x < width; ++x) {
            uint8_t g = srow[x];
            int base = x * dcn;
            drow[base] = g;
            drow[base + 1] = g;
            drow[base + 2] = g;
            if (dcn == 4) drow[base + 3] = 255;
        }
    }
}

static void mini_bgr_to_gray(const uint8_t* src, int width, int height, int src_stride, int scn,
                             int blue_idx, uint8_t* dst, int dst_stride) {
    for (int y = 0; y < height; ++y) {
        const uint8_t* srow = src + y * src_stride;
        uint8_t* drow = dst + y * dst_stride;
        for (int x = 0; x < width; ++x) {
            const uint8_t* p = srow + x * scn;
            int b = p[blue_idx];
            int g = p[1];
            int r = p[(blue_idx == 0) ? 2 : 0];
            int yv = (b * MINI_BY15 + g * MINI_GY15 + r * MINI_RY15 + (1 << (MINI_GRAY_SHIFT - 1))) >>
                     MINI_GRAY_SHIFT;
            drow[x] = mini_saturate_u8(yv);
        }
    }
}

static void mini_swap_rb(const uint8_t* src, int width, int height, int src_stride, int scn,
                         uint8_t* dst, int dst_stride, int dcn) {
    for (int y = 0; y < height; ++y) {
        const uint8_t* srow = src + y * src_stride;
        uint8_t* drow = dst + y * dst_stride;
        for (int x = 0; x < width; ++x) {
            const uint8_t* p = srow + x * scn;
            int base = x * dcn;
            drow[base] = p[2];
            drow[base + 1] = p[1];
            drow[base + 2] = p[0];
            if (dcn == 4) drow[base + 3] = (scn == 4) ? p[3] : 255;
        }
    }
}

int mini_cvtcolor_u8(const uint8_t* src, int width, int height, int src_stride,
                     int src_channels, uint8_t* dst, int dst_stride, int dst_channels,
                     mini_color_code code) {
    if (!src || !dst || width <= 0 || height <= 0) return -1;

    switch (code) {
    case MINI_BGR2GRAY:
        if (src_channels < 3 || dst_channels != 1) return -1;
        mini_bgr_to_gray(src, width, height, src_stride, src_channels, 0, dst, dst_stride);
        return 0;
    case MINI_RGB2GRAY:
        if (src_channels < 3 || dst_channels != 1) return -1;
        mini_bgr_to_gray(src, width, height, src_stride, src_channels, 2, dst, dst_stride);
        return 0;
    case MINI_RGBA2GRAY:
        if (src_channels < 4 || dst_channels != 1) return -1;
        mini_bgr_to_gray(src, width, height, src_stride, src_channels, 2, dst, dst_stride);
        return 0;
    case MINI_GRAY2BGR:
        if (src_channels != 1 || (dst_channels != 3 && dst_channels != 4)) return -1;
        mini_gray_to_rgb(src, width, height, src_stride, dst, dst_stride, dst_channels);
        return 0;
    case MINI_GRAY2RGB:
        if (src_channels != 1 || (dst_channels != 3 && dst_channels != 4)) return -1;
        mini_gray_to_rgb(src, width, height, src_stride, dst, dst_stride, dst_channels);
        return 0;
    case MINI_BGR2RGB:
        if (src_channels < 3 || (dst_channels != 3 && dst_channels != 4)) return -1;
        mini_swap_rb(src, width, height, src_stride, src_channels, dst, dst_stride, dst_channels);
        return 0;
    case MINI_RGB2BGR:
        if (src_channels < 3 || (dst_channels != 3 && dst_channels != 4)) return -1;
        mini_swap_rb(src, width, height, src_stride, src_channels, dst, dst_stride, dst_channels);
        return 0;
    default:
        return -1;
    }
}

static void mini_pack_dhash_bits(const uint8_t* img, int width, uint8_t* out_hash) {
    // img is 8 rows of width 9 grayscale pixels.
    memset(out_hash, 0, 8);
    for (int y = 0; y < 8; ++y) {
        const uint8_t* row = img + y * width;
        for (int x = 0; x < 8; ++x) {
            int bit_index = y * 8 + x;
            uint8_t mask = (uint8_t)(1u << (7 - (bit_index & 7)));
            if (row[x] > row[x + 1]) {
                out_hash[bit_index >> 3] |= mask;
            }
        }
    }
}

int mini_dhash_from_raw(const uint8_t* raw, int width, int height, int stride, uint8_t* out_hash, mini_color_code code) {
    if (!raw || !out_hash || width <= 0 || height <= 0 || stride <= 0) return -1;
    if (width > INT_MAX / 4 || height > INT_MAX) return -2;
    if (stride < width) return -3;
    const uint8_t* gray = NULL;
    if (code == MINI_NO_CHANGE) {
        gray = raw;
    } else {
        size_t gray_size = (size_t)(width > stride ? width : stride) * (size_t)height;
        if (gray_size == 0) {
            return -4;
        }
        uint8_t* converted_gray = (uint8_t*)malloc(gray_size);
        if (!converted_gray) {
            return -5;
        }

        int rc = mini_cvtcolor_u8(raw, width, height, stride, 4, converted_gray, stride, 1, code);
        if (rc != 0) {
            free(converted_gray);
            return rc;
        }
        gray = (const uint8_t*)converted_gray;
    }
    uint8_t resized[8 * 9];
    int rc = mini_resize_area_u8(gray, width, height, stride, 1, resized, 9, 8, 9);
    if (gray != raw) {
        free((void*)gray);
    }
    if (rc != 0) return rc;

    mini_pack_dhash_bits(resized, 9, out_hash);
    return 0;
}
