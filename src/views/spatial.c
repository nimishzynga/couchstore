/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

/**
 * @copyright 2013 Couchbase, Inc.
 *
 * @author Volker Mische  <volker@couchbase.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 **/

#include <stdlib.h>
#include <limits.h>
#include <assert.h>
#include "spatial.h"
#include "../bitfield.h"
#include "../couch_btree.h"


#define BYTE_PER_COORD sizeof(uint32_t)

#define IS_BIT_SET(num, bit)        (num & (1 << bit))
#define CHUNK_BITS                  (sizeof(unsigned char) * CHAR_BIT)
#define CHUNK_INDEX(map, size, bit) (size - 1 - ((bit) / CHUNK_BITS))
#define MAP_CHUNK(map, size, bit)   (map)[CHUNK_INDEX(map, size, bit)]
#define CHUNK_OFFSET(bit)           ((bit) % CHUNK_BITS)

#define MIN(a, b) (a) < (b) ? a : b
#define MAX(a, b) (a) > (b) ? a : b


STATIC couchstore_error_t encode_spatial_key(const sized_mbb_t *mbb,
                                          char *key,
                                          size_t nkey);
STATIC couchstore_error_t decode_spatial_key(const char *key,
                                             sized_mbb_t *mbb);
STATIC couchstore_error_t expand_mbb(sized_mbb_t *original,
                                     sized_mbb_t *expander);


int spatial_key_cmp(const sized_buf *key1, const sized_buf *key2,
                    const void *user_ctx)
{
    scale_factor_t *sf =
            ((view_spatial_builder_ctx_t *) user_ctx)->scale_factor;
    uint16_t mbb1_num = decode_raw16(*((raw_16 *) key1->buf));
    uint16_t mbb2_num = decode_raw16(*((raw_16 *) key2->buf));
    sized_mbb_t mbbs[2];
    double *mbbs_center[2];
    uint32_t *mbbs_scaled[2];
    unsigned char *mbbs_zcode[2];
    int res;

    mbbs[0].num = mbb1_num;
    mbbs[0].mbb = (double *)(key1->buf + sizeof(uint16_t));
    mbbs_center[0] = spatial_center(&mbbs[0]);
    mbbs_scaled[0] = spatial_scale_point(mbbs_center[0], sf);
    mbbs_zcode[0] = interleave_uint32s(mbbs_scaled[0], sf->dim);

    mbbs[1].num = mbb2_num;
    mbbs[1].mbb = (double *)(key2->buf + sizeof(uint16_t));
    mbbs_center[1] = spatial_center(&mbbs[1]);
    mbbs_scaled[1] = spatial_scale_point(mbbs_center[1], sf);
    mbbs_zcode[1] = interleave_uint32s(mbbs_scaled[1], sf->dim);

    res = memcmp(mbbs_zcode[0], mbbs_zcode[1], sf->dim * BYTE_PER_COORD);

    free(mbbs_center[0]);
    free(mbbs_scaled[0]);
    free(mbbs_zcode[0]);
    free(mbbs_center[1]);
    free(mbbs_scaled[1]);
    free(mbbs_zcode[1]);

    return res;
}


scale_factor_t *spatial_scale_factor(const double *mbb, uint16_t dim,
                                     uint32_t max)
{
    int i;
    double range;
    scale_factor_t *sf = NULL;
    double *offsets = NULL;
    double *scales = NULL;

    sf = (scale_factor_t *)malloc(sizeof(scale_factor_t));
    if (sf == NULL) {
        return NULL;
    }
    offsets = (double *)malloc(sizeof(double) * dim);
    if (offsets == NULL) {
        free(sf);
        return NULL;
    }
    scales = (double *)malloc(sizeof(double) * dim);
    if (scales == NULL) {
        free(sf);
        free(offsets);
        return NULL;
    }

    for (i = 0; i < dim; ++i) {
        offsets[i] = mbb[i*2];
        range = mbb[(i * 2) + 1] - mbb[i * 2];
        if (range == 0.0) {
            scales[i] = 0.0;
        } else {
            scales[i] = max / range;
        }
    }

    sf->offsets = offsets;
    sf->scales = scales;
    sf->dim = dim;
    return sf;
}

void free_spatial_scale_factor(scale_factor_t *sf)
{
    if (sf == NULL) {
        return;
    }
    free(sf->offsets);
    free(sf->scales);
    free(sf);
}


double *spatial_center(const sized_mbb_t *mbb)
{
    double *center = (double *)malloc(sizeof(double) * (mbb->num/2));
    uint32_t i;
    if (center == NULL) {
        return NULL;
    }

    for (i = 0; i < mbb->num; i += 2) {
        center[i/2] = mbb->mbb[i] + ((mbb->mbb[i+1] - mbb->mbb[i])/2);
    }
    return center;
}


uint32_t *spatial_scale_point(const double *point, const scale_factor_t *sf)
{
    int i;
    uint32_t *scaled = (uint32_t *)malloc(sizeof(uint32_t) * sf->dim);
    if (scaled == NULL) {
        return NULL;
    }

    for (i = 0; i < sf->dim; ++i) {
        /* casting to int is OK. No rounding is needed for the
           space-filling curve */
        scaled[i] = (uint32_t)((point[i] - sf->offsets[i]) *
                               sf->scales[i]);
    }
    return scaled;
}


void set_bit_sized(unsigned char *bitmap, uint16_t size, uint16_t bit)
{
    (MAP_CHUNK(bitmap, size, bit)) |= (1 << CHUNK_OFFSET(bit));
}


unsigned char *interleave_uint32s(uint32_t *numbers, uint16_t num)
{
    uint8_t i;
    uint16_t j, bitmap_size;
    unsigned char *bitmap = NULL;

    assert(num < 16384);

    /* bitmap_size in bits (hence the `*8`) */
    bitmap_size = (sizeof(uint32_t) * num * 8);
    bitmap = (unsigned char *)calloc(bitmap_size / 8, sizeof(unsigned char));
    if (bitmap == NULL) {
        return NULL;
    }

    /* i is the bit offset within a number
     * j is the current number offset */
    for (i = 0; i * num < bitmap_size; i++) {
        for (j = 0; j < num; j++) {
            /* Start with the last number, as we built up the bitmap
             * from right to left */
            if (IS_BIT_SET(numbers[(num - 1) - j], i)) {
                set_bit_sized(bitmap, bitmap_size/8, (i * num) + j);
            }
        }
    }
    return bitmap;
}


STATIC couchstore_error_t decode_spatial_key(const char *key, sized_mbb_t *mbb)
{
    mbb->num = decode_raw16(*((raw_16 *) key));
    mbb->mbb = (double *)(key + 2);
    return COUCHSTORE_SUCCESS;
}


STATIC couchstore_error_t encode_spatial_key(const sized_mbb_t *mbb,
                                             char *key,
                                             size_t nkey)
{
    raw_16 num = encode_raw16(mbb->num);

    memcpy(key, &num, 2);
    key += 2;

    assert(mbb->num * sizeof(double) <= nkey);
    memcpy(key, mbb->mbb, mbb->num * sizeof(double));

    return COUCHSTORE_SUCCESS;
}


/* Expands the `original` MBB with the `expander` */
STATIC couchstore_error_t expand_mbb(sized_mbb_t *original,
                                     sized_mbb_t *expander) {
    int i;

    assert(original->num == expander->num);

    for (i = 0; i < original->num; ++i) {
        if (i % 2 == 0) {
            original->mbb[i] = MIN(original->mbb[i], expander->mbb[i]);
        } else {
            original->mbb[i] = MAX(original->mbb[i], expander->mbb[i]);
        }
    }

    return COUCHSTORE_SUCCESS;
}


/* This reduce function is also used for the re-reduce */
couchstore_error_t view_spatial_reduce(char *dst,
                                       size_t *size_r,
                                       const nodelist *leaflist,
                                       int count,
                                       void *ctx)
{
    sized_mbb_t enclosing;
    sized_mbb_t tmp_mbb;
    const nodelist *i;
    (void) ctx;

    decode_spatial_key(leaflist->key.buf, &enclosing);
    count--;

    for (i = leaflist->next; i != NULL && count > 0; i = i->next, count--) {
        decode_spatial_key(i->key.buf, &tmp_mbb);
        expand_mbb(&enclosing, &tmp_mbb);
    }

    encode_spatial_key(&enclosing, dst, MAX_REDUCTION_SIZE);
    /* 2 is the prefix with the number of doubles */
    *size_r = 2 + (enclosing.num * sizeof(double));

    return COUCHSTORE_SUCCESS;
}
