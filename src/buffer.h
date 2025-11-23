/*
  +----------------------------------------------------------------------+
  | ClickHouse Native Driver - Binary Buffer                            |
  +----------------------------------------------------------------------+
*/

#ifndef CLICKHOUSE_BUFFER_H
#define CLICKHOUSE_BUFFER_H

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>

/* Buffer structure for binary operations */
typedef struct {
    uint8_t *data;
    size_t size;
    size_t capacity;
    size_t position;
} clickhouse_buffer;

/* Buffer creation and destruction */
clickhouse_buffer *clickhouse_buffer_create(size_t initial_capacity);
void clickhouse_buffer_free(clickhouse_buffer *buf);
void clickhouse_buffer_reset(clickhouse_buffer *buf);

/* Buffer capacity management */
int clickhouse_buffer_ensure_capacity(clickhouse_buffer *buf, size_t needed);
void clickhouse_buffer_compact(clickhouse_buffer *buf);

/* Write operations */
int clickhouse_buffer_write_uint8(clickhouse_buffer *buf, uint8_t value);
int clickhouse_buffer_write_uint16(clickhouse_buffer *buf, uint16_t value);
int clickhouse_buffer_write_uint32(clickhouse_buffer *buf, uint32_t value);
int clickhouse_buffer_write_uint64(clickhouse_buffer *buf, uint64_t value);
int clickhouse_buffer_write_int8(clickhouse_buffer *buf, int8_t value);
int clickhouse_buffer_write_int16(clickhouse_buffer *buf, int16_t value);
int clickhouse_buffer_write_int32(clickhouse_buffer *buf, int32_t value);
int clickhouse_buffer_write_int64(clickhouse_buffer *buf, int64_t value);
int clickhouse_buffer_write_float32(clickhouse_buffer *buf, float value);
int clickhouse_buffer_write_float64(clickhouse_buffer *buf, double value);
int clickhouse_buffer_write_varint(clickhouse_buffer *buf, uint64_t value);
int clickhouse_buffer_write_string(clickhouse_buffer *buf, const char *str, size_t len);
int clickhouse_buffer_write_bytes(clickhouse_buffer *buf, const uint8_t *data, size_t len);

/* Read operations */
int clickhouse_buffer_read_uint8(clickhouse_buffer *buf, uint8_t *value);
int clickhouse_buffer_read_uint16(clickhouse_buffer *buf, uint16_t *value);
int clickhouse_buffer_read_uint32(clickhouse_buffer *buf, uint32_t *value);
int clickhouse_buffer_read_uint64(clickhouse_buffer *buf, uint64_t *value);
int clickhouse_buffer_read_int8(clickhouse_buffer *buf, int8_t *value);
int clickhouse_buffer_read_int16(clickhouse_buffer *buf, int16_t *value);
int clickhouse_buffer_read_int32(clickhouse_buffer *buf, int32_t *value);
int clickhouse_buffer_read_int64(clickhouse_buffer *buf, int64_t *value);
int clickhouse_buffer_read_float32(clickhouse_buffer *buf, float *value);
int clickhouse_buffer_read_float64(clickhouse_buffer *buf, double *value);
int clickhouse_buffer_read_varint(clickhouse_buffer *buf, uint64_t *value);
int clickhouse_buffer_read_string(clickhouse_buffer *buf, char **str, size_t *len);
int clickhouse_buffer_read_bytes(clickhouse_buffer *buf, uint8_t *data, size_t len);

/* Buffer state */
size_t clickhouse_buffer_remaining(clickhouse_buffer *buf);
bool clickhouse_buffer_eof(clickhouse_buffer *buf);

#endif /* CLICKHOUSE_BUFFER_H */
