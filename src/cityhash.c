/*
 * CityHash128 - C port for ClickHouse PHP extension
 * Based on Google's CityHash by Geoff Pike and Jyrki Alakuijala
 *
 * Copyright (c) 2011 Google, Inc.
 * MIT License
 */

#include "cityhash.h"
#include <string.h>

/* Some primes between 2^63 and 2^64 for various uses */
static const uint64_t k0 = 0xc3a5c85c97cb3127ULL;
static const uint64_t k1 = 0xb492b66fbe98f273ULL;
static const uint64_t k2 = 0x9ae16a3b2f90404fULL;
static const uint64_t k3 = 0xc949d7c7509e6557ULL;

/* Unaligned load functions */
static inline uint64_t unaligned_load64(const char *p) {
    uint64_t result;
    memcpy(&result, p, sizeof(result));
    return result;
}

static inline uint32_t unaligned_load32(const char *p) {
    uint32_t result;
    memcpy(&result, p, sizeof(result));
    return result;
}

/* On little-endian systems, no byte swap needed */
static inline uint64_t fetch64(const char *p) {
    return unaligned_load64(p);
}

static inline uint32_t fetch32(const char *p) {
    return unaligned_load32(p);
}

/* Bitwise right rotate */
static inline uint64_t rotate(uint64_t val, int shift) {
    return shift == 0 ? val : ((val >> shift) | (val << (64 - shift)));
}

static inline uint64_t rotate_by_at_least_1(uint64_t val, int shift) {
    return (val >> shift) | (val << (64 - shift));
}

static inline uint64_t shift_mix(uint64_t val) {
    return val ^ (val >> 47);
}

/* Hash 128 bits down to 64 bits */
uint64_t hash128to64(cityhash128_t x) {
    const uint64_t kMul = 0x9ddfea08eb382d69ULL;
    uint64_t a = (x.low ^ x.high) * kMul;
    a ^= (a >> 47);
    uint64_t b = (x.high ^ a) * kMul;
    b ^= (b >> 47);
    b *= kMul;
    return b;
}

static inline uint64_t hash_len_16(uint64_t u, uint64_t v) {
    cityhash128_t x = {u, v};
    return hash128to64(x);
}

static uint64_t hash_len_0_to_16(const char *s, size_t len) {
    if (len > 8) {
        uint64_t a = fetch64(s);
        uint64_t b = fetch64(s + len - 8);
        return hash_len_16(a, rotate_by_at_least_1(b + len, (int)len)) ^ b;
    }
    if (len >= 4) {
        uint64_t a = fetch32(s);
        return hash_len_16(len + (a << 3), fetch32(s + len - 4));
    }
    if (len > 0) {
        uint8_t a = (uint8_t)s[0];
        uint8_t b = (uint8_t)s[len >> 1];
        uint8_t c = (uint8_t)s[len - 1];
        uint32_t y = (uint32_t)a + ((uint32_t)b << 8);
        uint32_t z = (uint32_t)len + ((uint32_t)c << 2);
        return shift_mix(y * k2 ^ z * k3) * k2;
    }
    return k2;
}

static uint64_t hash_len_17_to_32(const char *s, size_t len) {
    uint64_t a = fetch64(s) * k1;
    uint64_t b = fetch64(s + 8);
    uint64_t c = fetch64(s + len - 8) * k2;
    uint64_t d = fetch64(s + len - 16) * k0;
    return hash_len_16(rotate(a - b, 43) + rotate(c, 30) + d,
                       a + rotate(b ^ k3, 20) - c + len);
}

/* WeakHashLen32WithSeeds helper */
typedef struct {
    uint64_t first;
    uint64_t second;
} uint64_pair_t;

static uint64_pair_t weak_hash_len_32_with_seeds_vals(
    uint64_t w, uint64_t x, uint64_t y, uint64_t z, uint64_t a, uint64_t b) {
    a += w;
    b = rotate(b + a + z, 21);
    uint64_t c = a;
    a += x;
    a += y;
    b += rotate(a, 44);
    uint64_pair_t result = {a + z, b + c};
    return result;
}

static uint64_pair_t weak_hash_len_32_with_seeds(const char *s, uint64_t a, uint64_t b) {
    return weak_hash_len_32_with_seeds_vals(
        fetch64(s), fetch64(s + 8), fetch64(s + 16), fetch64(s + 24), a, b);
}

static uint64_t hash_len_33_to_64(const char *s, size_t len) {
    uint64_t z = fetch64(s + 24);
    uint64_t a = fetch64(s) + (len + fetch64(s + len - 16)) * k0;
    uint64_t b = rotate(a + z, 52);
    uint64_t c = rotate(a, 37);
    a += fetch64(s + 8);
    c += rotate(a, 7);
    a += fetch64(s + 16);
    uint64_t vf = a + z;
    uint64_t vs = b + rotate(a, 31) + c;
    a = fetch64(s + 16) + fetch64(s + len - 32);
    z = fetch64(s + len - 8);
    b = rotate(a + z, 52);
    c = rotate(a, 37);
    a += fetch64(s + len - 24);
    c += rotate(a, 7);
    a += fetch64(s + len - 16);
    uint64_t wf = a + z;
    uint64_t ws = b + rotate(a, 31) + c;
    uint64_t r = shift_mix((vf + ws) * k2 + (wf + vs) * k0);
    return shift_mix(r * k0 + vs) * k2;
}

static uint64_t city_hash_64(const char *s, size_t len) {
    if (len <= 32) {
        if (len <= 16) {
            return hash_len_0_to_16(s, len);
        } else {
            return hash_len_17_to_32(s, len);
        }
    } else if (len <= 64) {
        return hash_len_33_to_64(s, len);
    }

    uint64_t x = fetch64(s);
    uint64_t y = fetch64(s + len - 16) ^ k1;
    uint64_t z = fetch64(s + len - 56) ^ k0;
    uint64_pair_t v = weak_hash_len_32_with_seeds(s + len - 64, len, y);
    uint64_pair_t w = weak_hash_len_32_with_seeds(s + len - 32, len * k1, k0);
    z += shift_mix(v.second) * k1;
    x = rotate(z + x, 39) * k1;
    y = rotate(y, 33) * k1;

    len = (len - 1) & ~((size_t)63);
    do {
        x = rotate(x + y + v.first + fetch64(s + 16), 37) * k1;
        y = rotate(y + v.second + fetch64(s + 48), 42) * k1;
        x ^= w.second;
        y ^= v.first;
        z = rotate(z ^ w.first, 33);
        v = weak_hash_len_32_with_seeds(s, v.second * k1, x + w.first);
        w = weak_hash_len_32_with_seeds(s + 32, z + w.second, y);
        uint64_t tmp = z;
        z = x;
        x = tmp;
        s += 64;
        len -= 64;
    } while (len != 0);

    return hash_len_16(hash_len_16(v.first, w.first) + shift_mix(y) * k1 + z,
                       hash_len_16(v.second, w.second) + x);
}

/* CityMurmur - for strings < 128 bytes */
static cityhash128_t city_murmur(const char *s, size_t len, cityhash128_t seed) {
    uint64_t a = seed.low;
    uint64_t b = seed.high;
    uint64_t c = 0;
    uint64_t d = 0;

    if (len <= 16) {
        a = shift_mix(a * k1) * k1;
        c = b * k1 + hash_len_0_to_16(s, len);
        d = shift_mix(a + (len >= 8 ? fetch64(s) : c));
    } else {
        c = hash_len_16(fetch64(s + len - 8) + k1, a);
        d = hash_len_16(b + len, c + fetch64(s + len - 16));
        a += d;
        do {
            a ^= shift_mix(fetch64(s) * k1) * k1;
            a *= k1;
            b ^= a;
            c ^= shift_mix(fetch64(s + 8) * k1) * k1;
            c *= k1;
            d ^= c;
            s += 16;
            len -= 16;
        } while (len > 16);
    }
    a = hash_len_16(a, c);
    b = hash_len_16(d, b);
    cityhash128_t result = {a ^ b, hash_len_16(b, a)};
    return result;
}

cityhash128_t cityhash128_with_seed(const char *s, size_t len, cityhash128_t seed) {
    if (len < 128) {
        return city_murmur(s, len, seed);
    }

    uint64_pair_t v, w;
    uint64_t x = seed.low;
    uint64_t y = seed.high;
    uint64_t z = len * k1;

    v.first = rotate(y ^ k1, 49) * k1 + fetch64(s);
    v.second = rotate(v.first, 42) * k1 + fetch64(s + 8);
    w.first = rotate(y + z, 35) * k1 + x;
    w.second = rotate(x + fetch64(s + 88), 53) * k1;

    do {
        x = rotate(x + y + v.first + fetch64(s + 16), 37) * k1;
        y = rotate(y + v.second + fetch64(s + 48), 42) * k1;
        x ^= w.second;
        y ^= v.first;
        z = rotate(z ^ w.first, 33);
        v = weak_hash_len_32_with_seeds(s, v.second * k1, x + w.first);
        w = weak_hash_len_32_with_seeds(s + 32, z + w.second, y);
        { uint64_t tmp = z; z = x; x = tmp; }
        s += 64;

        x = rotate(x + y + v.first + fetch64(s + 16), 37) * k1;
        y = rotate(y + v.second + fetch64(s + 48), 42) * k1;
        x ^= w.second;
        y ^= v.first;
        z = rotate(z ^ w.first, 33);
        v = weak_hash_len_32_with_seeds(s, v.second * k1, x + w.first);
        w = weak_hash_len_32_with_seeds(s + 32, z + w.second, y);
        { uint64_t tmp = z; z = x; x = tmp; }
        s += 64;
        len -= 128;
    } while (len >= 128);

    y += rotate(w.first, 37) * k0 + z;
    x += rotate(v.first + z, 49) * k0;

    for (size_t tail_done = 0; tail_done < len; ) {
        tail_done += 32;
        y = rotate(y - x, 42) * k0 + v.second;
        w.first += fetch64(s + len - tail_done + 16);
        x = rotate(x, 49) * k0 + w.first;
        w.first += v.first;
        v = weak_hash_len_32_with_seeds(s + len - tail_done, v.first, v.second);
    }

    x = hash_len_16(x, v.first);
    y = hash_len_16(y, w.first);

    cityhash128_t result = {
        hash_len_16(x + v.second, w.second) + y,
        hash_len_16(x + w.second, y + v.second)
    };
    return result;
}

cityhash128_t cityhash128(const char *s, size_t len) {
    if (len >= 16) {
        cityhash128_t seed = {fetch64(s) ^ k3, fetch64(s + 8)};
        return cityhash128_with_seed(s + 16, len - 16, seed);
    } else if (len >= 8) {
        cityhash128_t seed = {fetch64(s) ^ (len * k0), fetch64(s + len - 8) ^ k1};
        return cityhash128_with_seed(NULL, 0, seed);
    } else {
        cityhash128_t seed = {k0, k1};
        return cityhash128_with_seed(s, len, seed);
    }
}
