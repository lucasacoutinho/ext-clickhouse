/*
 * CityHash128 - C port for ClickHouse PHP extension
 * Based on Google's CityHash by Geoff Pike and Jyrki Alakuijala
 *
 * Copyright (c) 2011 Google, Inc.
 * MIT License
 */

#ifndef CITYHASH_H
#define CITYHASH_H

#include <stdint.h>
#include <stddef.h>

/* 128-bit hash result */
typedef struct {
    uint64_t low;
    uint64_t high;
} cityhash128_t;

/* Compute CityHash128 of a byte buffer */
cityhash128_t cityhash128(const char *s, size_t len);

/* Compute CityHash128 with a seed */
cityhash128_t cityhash128_with_seed(const char *s, size_t len, cityhash128_t seed);

/* Hash 128 bits down to 64 bits */
uint64_t hash128to64(cityhash128_t x);

#endif /* CITYHASH_H */
