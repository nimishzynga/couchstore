/* -*- Mode: C; tab-width: 4; c-basic-offset: 4; indent-tabs-mode: nil -*- */

/**
 * @copyright 2013 Couchbase, Inc.
 *
 * @author Volker Mische <volker@couchbase.com>
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

#include "spatial_tests.h"


/* Convert a binary number encoded as string to an uint32 */
static uint32_t b2u(const char *binary)
{
    uint32_t result = 0;

    while(*binary != '\0')
    {
        result <<= 1;
        if (*binary == '1') {
            result |= 1;
        }
        binary++;
    }
    return result;
}


/* Convert a binary number encoded as string to an unsigned char array
 * The size (in bytes) will be used for padding the result with leading
 * zeros. */
static unsigned char *b2c(const char *binary, size_t size)
{
    int i = 0;
    /* The offset is on the input string, hence it is in bits */
    size_t offset = size*8 - strlen(binary);
    unsigned char *result = (unsigned char *)calloc(
        size, sizeof(unsigned char));

    for(i = 0; *binary != '\0'; i++, binary++) {
        result[(i+offset)/8] <<= 1;
        if (*binary == '1') {
            result[(i+offset)/8] |= 1;
        }
    }
    return result;
}


/* Return an assertable value: != 0 when the interleaving returned the
 * expected result */
static int interleaving(uint32_t numbers[],
                        const uint16_t size_numbers,
                        const char *expected)
{
    unsigned char *interleaved;
    unsigned char *expected_bin;
    int ret = 0;

    interleaved = interleave_uint32s(numbers, size_numbers/sizeof(uint32_t));
    expected_bin = b2c(expected, size_numbers);

    ret = memcmp((void *)interleaved, expected_bin, size_numbers) == 0;

    free(interleaved);
    free(expected_bin);

    return ret;
}


void test_interleaving()
{
    uint32_t *numbers;
    uint32_t numbers_size;

    fprintf(stderr, "Running spatial interleaving tests\n");

    numbers_size = 2 * sizeof(uint32_t);
    numbers = (uint32_t *)malloc(numbers_size);
    numbers[0] = b2u("111111000000");
    numbers[1] = b2u("000000111111");
    assert(interleaving(numbers, numbers_size, "101010101010010101010101"));
    free(numbers);

    numbers_size = 3 * sizeof(uint32_t);
    numbers = (uint32_t *)malloc(numbers_size);
    numbers[0] = b2u("1101");
    numbers[1] = b2u("0101");
    numbers[2] = b2u("1111");
    assert(interleaving(numbers, numbers_size, "101111001111"));
    free(numbers);

    numbers_size = 4 * sizeof(uint32_t);
    numbers = (uint32_t *)malloc(numbers_size);
    numbers[0] = b2u("0000");
    numbers[1] = b2u("1111");
    numbers[2] = b2u("1111");
    numbers[3] = b2u("0000");
    assert(interleaving(numbers, numbers_size, "0110011001100110"));
    free(numbers);

    numbers_size = 5 * sizeof(uint32_t);
    numbers = (uint32_t *)malloc(numbers_size);
    numbers[0] = b2u("11");
    numbers[1] = b2u("01");
    numbers[2] = b2u("11");
    numbers[3] = b2u("00");
    numbers[4] = b2u("10");
    assert(interleaving(numbers, numbers_size, "1010111100"));
    free(numbers);

    numbers_size = 2 * sizeof(uint32_t);
    numbers = (uint32_t *)malloc(numbers_size);
    numbers[0] = b2u("11111111111111111111111111111111");
    numbers[1] = b2u("00000000000000000000000000000000");
    assert(interleaving(numbers, numbers_size, "1010101010101010101010101010101010101010101010101010101010101010"));
    free(numbers);

    numbers_size = 12 * sizeof(uint32_t);
    numbers = (uint32_t *)malloc(numbers_size);
    numbers[0] = b2u("11");
    numbers[1] = b2u("00");
    numbers[2] = b2u("11");
    numbers[3] = b2u("11");
    numbers[4] = b2u("00");
    numbers[5] = b2u("00");
    numbers[6] = b2u("11");
    numbers[7] = b2u("11");
    numbers[8] = b2u("11");
    numbers[9] = b2u("00");
    numbers[10] = b2u("00");
    numbers[11] = b2u("00");
    assert(interleaving(numbers, numbers_size, "101100111000101100111000"));
    free(numbers);
}


void test_spatial_scale_factor()
{
    double mbb[] = {1.0, 3.0, 30.33, 31.33, 15.4, 138.7, 7.8, 7.8};
    uint16_t dim = (sizeof(mbb)/sizeof(double))/2;
    uint32_t max = ZCODE_MAX_VALUE;
    scale_factor_t *sf = NULL;

    fprintf(stderr, "Running spatial scale factor tests\n");

    sf = spatial_scale_factor(mbb, dim, max);

    assert(sf->offsets[0] == mbb[0]);
    assert(sf->offsets[1] == mbb[2]);
    assert(sf->offsets[2] == mbb[4]);
    assert(sf->offsets[3] == mbb[6]);
    assert((uint32_t)(sf->scales[0]*(mbb[1]-mbb[0])) == max);
    assert((uint32_t)(sf->scales[1]*(mbb[3]-mbb[2])) == max);
    assert((uint32_t)(sf->scales[2]*(mbb[5]-mbb[4])) == max);
    assert((uint32_t)(sf->scales[3]) == 0);
    assert(sf->dim == dim);

    free_spatial_scale_factor(sf);
}


void test_spatial_center()
{
    double mbb[] = {1.0, 3.0, 30.33, 31.33, 15.4, 138.7, 7.8, 7.8};
    double mbb2[] = {6.3, 18.7};
    sized_mbb_t mbb_struct;
    double *center;

    fprintf(stderr, "Running spatial scale factor tests\n");

    mbb_struct.mbb = mbb;
    mbb_struct.num = sizeof(mbb)/sizeof(double);
    center = spatial_center(&mbb_struct);
    assert(center[0] == 2.0);
    assert(center[1] == 30.83);
    assert(center[2] == 77.05);
    assert(center[3] == 7.8);
    free(center);

    mbb_struct.mbb = mbb2;
    mbb_struct.num = sizeof(mbb2)/sizeof(double);
    center = spatial_center(&mbb_struct);
    assert(center[0] == 12.5);
    free(center);
}


void test_spatial_scale_point()
{
    double mbb[] = {1.0, 3.0, 30.33, 31.33, 15.4, 138.7, 7.8, 7.8};
    double point[] = {2.0, 31.0, 42.02, 7.8};
    uint16_t dim = (sizeof(mbb)/sizeof(double))/2;
    uint32_t max = ZCODE_MAX_VALUE;
    scale_factor_t *sf = NULL;
    uint32_t *scaled;

    fprintf(stderr, "Running spatial scale point tests\n");

    sf = spatial_scale_factor(mbb, dim, max);
    scaled = spatial_scale_point(point, sf);
    assert(scaled[0] == UINT32_MAX/2);
    assert(scaled[1] > UINT32_MAX/2 && scaled[1] > 0);
    assert(scaled[2] < UINT32_MAX/2 && scaled[2] > 0);
    assert(scaled[3] == 0);

    free_spatial_scale_factor(sf);
    free(scaled);
}


static int cmp_bytes(const unsigned char *bitmap,
                     const char *expected,
                     const uint8_t size)
{
    int result = 0;
    unsigned char *tmp = b2c(expected, size);

    result = memcmp(bitmap, tmp, size) == 0;
    free(tmp);

    return result;
}

void test_set_bit_sized()
{
    uint8_t size = 1;
    unsigned char* bitmap = b2c("00010001", size);
    unsigned char* bitmap2 = b2c("0000000000000000", 2);

    fprintf(stderr, "Running set bit sized tests\n");

    set_bit_sized(bitmap, size, 1);
    assert(cmp_bytes(bitmap, "00010011", size));

    set_bit_sized(bitmap, size, 7);
    assert(cmp_bytes(bitmap, "10010011", size));
    set_bit_sized(bitmap, size, 2);
    assert(cmp_bytes(bitmap, "10010111", size));
    set_bit_sized(bitmap, size, 3);
    assert(cmp_bytes(bitmap, "10011111", size));
    /* Setting the same bit doesn't change the value */
    set_bit_sized(bitmap, size, 3);
    assert(cmp_bytes(bitmap, "10011111", size));

    size = 2;
    set_bit_sized(bitmap2, size, 0);
    assert(cmp_bytes(bitmap2, "0000000000000001", size));
    set_bit_sized(bitmap2, size, 13);
    assert(cmp_bytes(bitmap2, "0010000000000001", size));
    set_bit_sized(bitmap2, size, 7);
    assert(cmp_bytes(bitmap2, "0010000010000001", size));
    set_bit_sized(bitmap2, size, 3);
    assert(cmp_bytes(bitmap2, "0010000010001001", size));
    set_bit_sized(bitmap2, size, 12);
    assert(cmp_bytes(bitmap2, "0011000010001001", size));

    free(bitmap);
    free(bitmap2);
}


void test_encode_spatial_key()
{
    sized_mbb_t mbb_struct;
    char encoded[66];
    double mbb[] = {6.3, 18.7};
    double mbb2[] = {1.0, 3.0, 30.33, 31.33, 15.4, 138.7, 7.8, 7.8};
    unsigned char expected[] = {
        0x00, 0x02, 0x33, 0x33, 0x33, 0x33, 0x33, 0x33, 0x19, 0x40,
        0x33, 0x33, 0x33, 0x33, 0x33, 0xb3, 0x32, 0x40
    };
    unsigned char expected2[] = {
        0x00, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xf0, 0x3f,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08, 0x40, 0x14, 0xae,
        0x47, 0xe1, 0x7a, 0x54, 0x3e, 0x40, 0x14, 0xae, 0x47, 0xe1,
        0x7a, 0x54, 0x3f, 0x40, 0xcd, 0xcc, 0xcc, 0xcc, 0xcc, 0xcc,
        0x2e, 0x40, 0x66, 0x66, 0x66, 0x66, 0x66, 0x56, 0x61, 0x40,
        0x33, 0x33, 0x33, 0x33, 0x33, 0x33, 0x1f, 0x40, 0x33, 0x33,
        0x33, 0x33, 0x33, 0x33, 0x1f, 0x40
    };

    fprintf(stderr, "Running encode spatial key tests\n");

    mbb_struct.mbb = mbb;
    mbb_struct.num = sizeof(mbb)/sizeof(double);
    encode_spatial_key(&mbb_struct, (char *)&encoded, sizeof(encoded));
    assert(memcmp(encoded, expected, 18) == 0);

    mbb_struct.mbb = mbb2;
    mbb_struct.num = sizeof(mbb2)/sizeof(double);
    encode_spatial_key(&mbb_struct, (char *)&encoded, sizeof(encoded));
    assert(memcmp(encoded, expected2, 66) == 0);
}


/* Compares two arrays of doubles. 1 if equal, 0 if not equal */
static bool is_double_array_equal(double *a, double *b, int len)
{
    int i;

    for (i = 0; i < len; ++i) {
        if (a[i] != b[i]) {
            return false;
        }
    }

    return true;
}

void test_decode_spatial_key()
{
    sized_mbb_t decoded;
    unsigned char mbb[] = {
        0x00, 0x02, 0x33, 0x33, 0x33, 0x33, 0x33, 0x33, 0x19, 0x40,
        0x33, 0x33, 0x33, 0x33, 0x33, 0xb3, 0x32, 0x40
    };
    unsigned char mbb2[] = {
        0x00, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xf0, 0x3f,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08, 0x40, 0x14, 0xae,
        0x47, 0xe1, 0x7a, 0x54, 0x3e, 0x40, 0x14, 0xae, 0x47, 0xe1,
        0x7a, 0x54, 0x3f, 0x40, 0xcd, 0xcc, 0xcc, 0xcc, 0xcc, 0xcc,
        0x2e, 0x40, 0x66, 0x66, 0x66, 0x66, 0x66, 0x56, 0x61, 0x40,
        0x33, 0x33, 0x33, 0x33, 0x33, 0x33, 0x1f, 0x40, 0x33, 0x33,
        0x33, 0x33, 0x33, 0x33, 0x1f, 0x40
    };
    double expected[] = {6.3, 18.7};
    double expected2[] = {1.0, 3.0, 30.33, 31.33, 15.4, 138.7, 7.8, 7.8};

    fprintf(stderr, "Running decode spatial key tests\n");

    decode_spatial_key((char *)mbb, &decoded);
    assert(decoded.num == 2);
    assert(is_double_array_equal(decoded.mbb, expected, 2));

    decode_spatial_key((char *)mbb2, &decoded);
    assert(decoded.num == 8);
    assert(is_double_array_equal(decoded.mbb, expected2, 8));
}


void test_expand_mbb()
{
    sized_mbb_t mbb_struct_a, mbb_struct_b;
    double mbb_a[] = {6.3, 18.7};
    double mbb_b[] = {10.1, 31.5};
    double expected_mbb[] = {6.3, 31.5};
    double mbb2_a[] = {3.0, 5.0, 100.1, 150.2, 12.5, 13.75};
    double mbb2_b[] = {2.9, 4.9, 80.0, 222.2, 13.0, 13.1};
    double expected2_mbb[] = {2.9, 5.0, 80.0, 222.2, 12.5, 13.75};

    fprintf(stderr, "Running expand MBB tests\n");

    mbb_struct_a.num = 2;
    mbb_struct_a.mbb = mbb_a;
    mbb_struct_b.num = 2;
    mbb_struct_b.mbb = mbb_b;
    expand_mbb(&mbb_struct_a, &mbb_struct_b);
    assert(is_double_array_equal(mbb_struct_a.mbb, expected_mbb, 2));

    mbb_struct_a.num = 6;
    mbb_struct_a.mbb = mbb2_a;
    mbb_struct_b.num = 6;
    mbb_struct_b.mbb = mbb2_b;
    expand_mbb(&mbb_struct_a, &mbb_struct_b);
    assert(is_double_array_equal(mbb_struct_a.mbb, expected2_mbb, 6));
}
