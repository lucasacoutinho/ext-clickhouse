/*
  +----------------------------------------------------------------------+
  | ClickHouse Native Driver - Binary Buffer Implementation             |
  +----------------------------------------------------------------------+
*/

#include "buffer.h"
#include <stdlib.h>
#include <string.h>

#define BUFFER_INITIAL_CAPACITY 4096
#define BUFFER_GROWTH_FACTOR 2

clickhouse_buffer *clickhouse_buffer_create(size_t initial_capacity) {
    clickhouse_buffer *buf = malloc(sizeof(clickhouse_buffer));
    if (!buf) {
        return NULL;
    }

    if (initial_capacity == 0) {
        initial_capacity = BUFFER_INITIAL_CAPACITY;
    }

    buf->data = malloc(initial_capacity);
    if (!buf->data) {
        free(buf);
        return NULL;
    }

    buf->size = 0;
    buf->capacity = initial_capacity;
    buf->position = 0;

    return buf;
}

void clickhouse_buffer_free(clickhouse_buffer *buf) {
    if (buf) {
        if (buf->data) {
            free(buf->data);
        }
        free(buf);
    }
}

void clickhouse_buffer_reset(clickhouse_buffer *buf) {
    buf->size = 0;
    buf->position = 0;
}

void clickhouse_buffer_compact(clickhouse_buffer *buf) {
    /* Move unread data from position to the beginning of the buffer */
    if (buf->position > 0) {
        size_t remaining = buf->size - buf->position;
        if (remaining > 0) {
            memmove(buf->data, buf->data + buf->position, remaining);
        }
        buf->size = remaining;
        buf->position = 0;
    }
}

int clickhouse_buffer_ensure_capacity(clickhouse_buffer *buf, size_t needed) {
    size_t required = buf->size + needed;

    if (required <= buf->capacity) {
        return 0;
    }

    size_t new_capacity = buf->capacity;
    while (new_capacity < required) {
        new_capacity *= BUFFER_GROWTH_FACTOR;
    }

    uint8_t *new_data = realloc(buf->data, new_capacity);
    if (!new_data) {
        return -1;
    }

    buf->data = new_data;
    buf->capacity = new_capacity;

    return 0;
}

/* Write operations - Little Endian */

int clickhouse_buffer_write_uint8(clickhouse_buffer *buf, uint8_t value) {
    if (clickhouse_buffer_ensure_capacity(buf, 1) != 0) {
        return -1;
    }
    buf->data[buf->size++] = value;
    return 0;
}

int clickhouse_buffer_write_uint16(clickhouse_buffer *buf, uint16_t value) {
    if (clickhouse_buffer_ensure_capacity(buf, 2) != 0) {
        return -1;
    }
    buf->data[buf->size++] = (uint8_t)(value & 0xFF);
    buf->data[buf->size++] = (uint8_t)((value >> 8) & 0xFF);
    return 0;
}

int clickhouse_buffer_write_uint32(clickhouse_buffer *buf, uint32_t value) {
    if (clickhouse_buffer_ensure_capacity(buf, 4) != 0) {
        return -1;
    }
    buf->data[buf->size++] = (uint8_t)(value & 0xFF);
    buf->data[buf->size++] = (uint8_t)((value >> 8) & 0xFF);
    buf->data[buf->size++] = (uint8_t)((value >> 16) & 0xFF);
    buf->data[buf->size++] = (uint8_t)((value >> 24) & 0xFF);
    return 0;
}

int clickhouse_buffer_write_uint64(clickhouse_buffer *buf, uint64_t value) {
    if (clickhouse_buffer_ensure_capacity(buf, 8) != 0) {
        return -1;
    }
    for (int i = 0; i < 8; i++) {
        buf->data[buf->size++] = (uint8_t)((value >> (i * 8)) & 0xFF);
    }
    return 0;
}

int clickhouse_buffer_write_int8(clickhouse_buffer *buf, int8_t value) {
    return clickhouse_buffer_write_uint8(buf, (uint8_t)value);
}

int clickhouse_buffer_write_int16(clickhouse_buffer *buf, int16_t value) {
    return clickhouse_buffer_write_uint16(buf, (uint16_t)value);
}

int clickhouse_buffer_write_int32(clickhouse_buffer *buf, int32_t value) {
    return clickhouse_buffer_write_uint32(buf, (uint32_t)value);
}

int clickhouse_buffer_write_int64(clickhouse_buffer *buf, int64_t value) {
    return clickhouse_buffer_write_uint64(buf, (uint64_t)value);
}

int clickhouse_buffer_write_float32(clickhouse_buffer *buf, float value) {
    uint32_t bits;
    memcpy(&bits, &value, sizeof(bits));
    return clickhouse_buffer_write_uint32(buf, bits);
}

int clickhouse_buffer_write_float64(clickhouse_buffer *buf, double value) {
    uint64_t bits;
    memcpy(&bits, &value, sizeof(bits));
    return clickhouse_buffer_write_uint64(buf, bits);
}

int clickhouse_buffer_write_varint(clickhouse_buffer *buf, uint64_t value) {
    while (value >= 0x80) {
        if (clickhouse_buffer_write_uint8(buf, (uint8_t)(value | 0x80)) != 0) {
            return -1;
        }
        value >>= 7;
    }
    return clickhouse_buffer_write_uint8(buf, (uint8_t)value);
}

int clickhouse_buffer_write_string(clickhouse_buffer *buf, const char *str, size_t len) {
    if (clickhouse_buffer_write_varint(buf, len) != 0) {
        return -1;
    }
    return clickhouse_buffer_write_bytes(buf, (const uint8_t *)str, len);
}

int clickhouse_buffer_write_bytes(clickhouse_buffer *buf, const uint8_t *data, size_t len) {
    if (len == 0) {
        return 0;
    }
    if (clickhouse_buffer_ensure_capacity(buf, len) != 0) {
        return -1;
    }
    memcpy(buf->data + buf->size, data, len);
    buf->size += len;
    return 0;
}

/* Read operations - Little Endian */

int clickhouse_buffer_read_uint8(clickhouse_buffer *buf, uint8_t *value) {
    if (buf->position + 1 > buf->size) {
        return -1;
    }
    *value = buf->data[buf->position++];
    return 0;
}

int clickhouse_buffer_read_uint16(clickhouse_buffer *buf, uint16_t *value) {
    if (buf->position + 2 > buf->size) {
        return -1;
    }
    *value = (uint16_t)buf->data[buf->position] |
             ((uint16_t)buf->data[buf->position + 1] << 8);
    buf->position += 2;
    return 0;
}

int clickhouse_buffer_read_uint32(clickhouse_buffer *buf, uint32_t *value) {
    if (buf->position + 4 > buf->size) {
        return -1;
    }
    *value = (uint32_t)buf->data[buf->position] |
             ((uint32_t)buf->data[buf->position + 1] << 8) |
             ((uint32_t)buf->data[buf->position + 2] << 16) |
             ((uint32_t)buf->data[buf->position + 3] << 24);
    buf->position += 4;
    return 0;
}

int clickhouse_buffer_read_uint64(clickhouse_buffer *buf, uint64_t *value) {
    if (buf->position + 8 > buf->size) {
        return -1;
    }
    *value = 0;
    for (int i = 0; i < 8; i++) {
        *value |= ((uint64_t)buf->data[buf->position + i] << (i * 8));
    }
    buf->position += 8;
    return 0;
}

int clickhouse_buffer_read_int8(clickhouse_buffer *buf, int8_t *value) {
    return clickhouse_buffer_read_uint8(buf, (uint8_t *)value);
}

int clickhouse_buffer_read_int16(clickhouse_buffer *buf, int16_t *value) {
    return clickhouse_buffer_read_uint16(buf, (uint16_t *)value);
}

int clickhouse_buffer_read_int32(clickhouse_buffer *buf, int32_t *value) {
    return clickhouse_buffer_read_uint32(buf, (uint32_t *)value);
}

int clickhouse_buffer_read_int64(clickhouse_buffer *buf, int64_t *value) {
    return clickhouse_buffer_read_uint64(buf, (uint64_t *)value);
}

int clickhouse_buffer_read_float32(clickhouse_buffer *buf, float *value) {
    uint32_t bits;
    if (clickhouse_buffer_read_uint32(buf, &bits) != 0) {
        return -1;
    }
    memcpy(value, &bits, sizeof(*value));
    return 0;
}

int clickhouse_buffer_read_float64(clickhouse_buffer *buf, double *value) {
    uint64_t bits;
    if (clickhouse_buffer_read_uint64(buf, &bits) != 0) {
        return -1;
    }
    memcpy(value, &bits, sizeof(*value));
    return 0;
}

int clickhouse_buffer_read_varint(clickhouse_buffer *buf, uint64_t *value) {
    *value = 0;
    int shift = 0;
    uint8_t byte;

    do {
        if (shift >= 64) {
            return -1; /* Varint too long */
        }
        if (clickhouse_buffer_read_uint8(buf, &byte) != 0) {
            return -1;
        }
        *value |= ((uint64_t)(byte & 0x7F)) << shift;
        shift += 7;
    } while (byte & 0x80);

    return 0;
}

int clickhouse_buffer_read_string(clickhouse_buffer *buf, char **str, size_t *len) {
    uint64_t length;
    if (clickhouse_buffer_read_varint(buf, &length) != 0) {
        return -1;
    }

    if (buf->position + length > buf->size) {
        return -1;
    }

    *str = malloc(length + 1);
    if (!*str) {
        return -1;
    }

    memcpy(*str, buf->data + buf->position, length);
    (*str)[length] = '\0';
    buf->position += length;
    *len = (size_t)length;

    return 0;
}

int clickhouse_buffer_read_bytes(clickhouse_buffer *buf, uint8_t *data, size_t len) {
    if (buf->position + len > buf->size) {
        return -1;
    }
    memcpy(data, buf->data + buf->position, len);
    buf->position += len;
    return 0;
}

size_t clickhouse_buffer_remaining(const clickhouse_buffer *buf) {
    return buf->size - buf->position;
}

bool clickhouse_buffer_eof(const clickhouse_buffer *buf) {
    return buf->position >= buf->size;
}
