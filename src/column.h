/*
  +----------------------------------------------------------------------+
  | ClickHouse Native Driver - Column Types                             |
  +----------------------------------------------------------------------+
*/

#ifndef CLICKHOUSE_COLUMN_H
#define CLICKHOUSE_COLUMN_H

#include <stdint.h>
#include <stddef.h>
#include "buffer.h"

/* Column types */
typedef enum {
    CH_TYPE_UNKNOWN = 0,
    CH_TYPE_INT8,
    CH_TYPE_INT16,
    CH_TYPE_INT32,
    CH_TYPE_INT64,
    CH_TYPE_INT128,
    CH_TYPE_INT256,
    CH_TYPE_UINT8,
    CH_TYPE_UINT16,
    CH_TYPE_UINT32,
    CH_TYPE_UINT64,
    CH_TYPE_UINT128,
    CH_TYPE_UINT256,
    CH_TYPE_FLOAT32,
    CH_TYPE_FLOAT64,
    CH_TYPE_STRING,
    CH_TYPE_FIXED_STRING,
    CH_TYPE_DATE,
    CH_TYPE_DATE32,
    CH_TYPE_DATETIME,
    CH_TYPE_DATETIME64,
    CH_TYPE_UUID,
    CH_TYPE_ENUM8,
    CH_TYPE_ENUM16,
    CH_TYPE_ARRAY,
    CH_TYPE_NULLABLE,
    CH_TYPE_TUPLE,
    CH_TYPE_MAP,
    CH_TYPE_LOWCARDINALITY,
    CH_TYPE_DECIMAL,
    CH_TYPE_DECIMAL32,
    CH_TYPE_DECIMAL64,
    CH_TYPE_DECIMAL128,
    CH_TYPE_DECIMAL256,
    CH_TYPE_IPV4,
    CH_TYPE_IPV6,
    CH_TYPE_BOOL,
    CH_TYPE_NOTHING,
    CH_TYPE_POINT,
    CH_TYPE_RING,
    CH_TYPE_POLYGON,
    CH_TYPE_MULTIPOLYGON,
    CH_TYPE_SIMPLEAGGREGATEFUNCTION,
    CH_TYPE_JSON,
    CH_TYPE_OBJECT,
    CH_TYPE_VARIANT,
    CH_TYPE_DYNAMIC
} clickhouse_type_id;

/* Forward declaration */
struct clickhouse_column;

/* Enum value mapping */
typedef struct clickhouse_enum_value {
    char *name;
    int16_t value;
} clickhouse_enum_value;

/* Column type info */
typedef struct clickhouse_type_info {
    clickhouse_type_id type_id;
    char *type_name;
    size_t fixed_size;              /* For FixedString(N), Decimal precision, etc */
    struct clickhouse_type_info *nested; /* For Array, Nullable, LowCardinality */
    struct clickhouse_type_info **tuple_elements; /* For Tuple */
    size_t tuple_size;
    /* Enum support */
    clickhouse_enum_value *enum_values;
    size_t enum_count;
} clickhouse_type_info;

/* Column structure */
typedef struct clickhouse_column {
    char *name;
    clickhouse_type_info *type;
    void *data;
    uint8_t *nulls;             /* NULL bitmap for Nullable */
    size_t row_count;
    size_t capacity;
    /* Array support */
    uint64_t *offsets;          /* Array offsets (cumulative element counts) */
    struct clickhouse_column *nested_column; /* Nested column data for Arrays */
    /* Tuple support */
    struct clickhouse_column **tuple_columns; /* Columns for each Tuple element */
    size_t tuple_column_count;
    /* Variant support */
    uint8_t *discriminators;    /* Variant type discriminators */
    size_t max_dynamic_types;   /* For Dynamic type */
} clickhouse_column;

/* Block structure (collection of columns) */
typedef struct {
    clickhouse_column **columns;
    size_t column_count;
    size_t row_count;
} clickhouse_block;

/* Type parsing */
clickhouse_type_info *clickhouse_type_parse(const char *type_str);
void clickhouse_type_free(clickhouse_type_info *type);
clickhouse_type_id clickhouse_type_from_name(const char *name);
size_t clickhouse_type_size(clickhouse_type_info *type);

/* Column operations */
clickhouse_column *clickhouse_column_create(const char *name, clickhouse_type_info *type);
void clickhouse_column_free(clickhouse_column *col);
int clickhouse_column_read(clickhouse_buffer *buf, clickhouse_column *col, size_t row_count);
int clickhouse_column_write(clickhouse_buffer *buf, clickhouse_column *col);

/* Block operations */
clickhouse_block *clickhouse_block_create(void);
void clickhouse_block_free(clickhouse_block *block);
int clickhouse_block_add_column(clickhouse_block *block, clickhouse_column *col);
int clickhouse_block_read(clickhouse_buffer *buf, clickhouse_block *block);
int clickhouse_block_write(clickhouse_buffer *buf, clickhouse_block *block);

/* Block header for data packets */
int clickhouse_block_read_header(clickhouse_buffer *buf, char **table_name,
                                  uint64_t *column_count, uint64_t *row_count);
int clickhouse_block_write_header(clickhouse_buffer *buf, const char *table_name,
                                   uint64_t column_count, uint64_t row_count);

#endif /* CLICKHOUSE_COLUMN_H */
