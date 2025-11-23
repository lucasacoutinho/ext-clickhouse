/*
  +----------------------------------------------------------------------+
  | ClickHouse Native Driver - Column Implementation                    |
  +----------------------------------------------------------------------+
*/

/* Enable POSIX and GNU extensions for strdup, strndup, etc. */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#ifndef _POSIX_C_SOURCE
#define _POSIX_C_SOURCE 200809L
#endif

#include "column.h"
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <stdio.h>

/* Type name mappings */
static const struct {
    const char *name;
    clickhouse_type_id type_id;
    size_t size;
} type_mappings[] = {
    {"Int8", CH_TYPE_INT8, 1},
    {"Int16", CH_TYPE_INT16, 2},
    {"Int32", CH_TYPE_INT32, 4},
    {"Int64", CH_TYPE_INT64, 8},
    {"Int128", CH_TYPE_INT128, 16},
    {"Int256", CH_TYPE_INT256, 32},
    {"UInt8", CH_TYPE_UINT8, 1},
    {"UInt16", CH_TYPE_UINT16, 2},
    {"UInt32", CH_TYPE_UINT32, 4},
    {"UInt64", CH_TYPE_UINT64, 8},
    {"UInt128", CH_TYPE_UINT128, 16},
    {"UInt256", CH_TYPE_UINT256, 32},
    {"Float32", CH_TYPE_FLOAT32, 4},
    {"Float64", CH_TYPE_FLOAT64, 8},
    {"String", CH_TYPE_STRING, 0},
    {"Date", CH_TYPE_DATE, 2},
    {"Date32", CH_TYPE_DATE32, 4},
    {"DateTime", CH_TYPE_DATETIME, 4},
    {"UUID", CH_TYPE_UUID, 16},
    {"IPv4", CH_TYPE_IPV4, 4},
    {"IPv6", CH_TYPE_IPV6, 16},
    {"Bool", CH_TYPE_BOOL, 1},
    {"Nothing", CH_TYPE_NOTHING, 0},
    {"Point", CH_TYPE_POINT, 0},
    {"Ring", CH_TYPE_RING, 0},
    {"Polygon", CH_TYPE_POLYGON, 0},
    {"MultiPolygon", CH_TYPE_MULTIPOLYGON, 0},
    {"JSON", CH_TYPE_JSON, 0},
    {NULL, CH_TYPE_UNKNOWN, 0}
};

/* Size mappings for parametric types by type_id */
static size_t get_type_size_by_id(clickhouse_type_id type_id) {
    switch (type_id) {
        case CH_TYPE_DATETIME64: return 8;  /* Int64 */
        case CH_TYPE_DECIMAL32: return 4;   /* Int32 */
        case CH_TYPE_DECIMAL64: return 8;   /* Int64 */
        case CH_TYPE_DECIMAL128: return 16; /* Int128 */
        case CH_TYPE_DECIMAL256: return 32; /* Int256 */
        case CH_TYPE_ENUM8: return 1;       /* Int8 */
        case CH_TYPE_ENUM16: return 2;      /* Int16 */
        default: return 0;
    }
}

clickhouse_type_id clickhouse_type_from_name(const char *name) {
    for (int i = 0; type_mappings[i].name != NULL; i++) {
        if (strcmp(name, type_mappings[i].name) == 0) {
            return type_mappings[i].type_id;
        }
    }
    return CH_TYPE_UNKNOWN;
}

size_t clickhouse_type_size(clickhouse_type_info *type) {
    if (!type) return 0;

    for (int i = 0; type_mappings[i].name != NULL; i++) {
        if (type->type_id == type_mappings[i].type_id) {
            return type_mappings[i].size;
        }
    }

    if (type->type_id == CH_TYPE_FIXED_STRING) {
        return type->fixed_size;
    }

    /* Check parametric types */
    size_t size = get_type_size_by_id(type->type_id);
    if (size > 0) {
        return size;
    }

    return 0;
}

clickhouse_type_info *clickhouse_type_parse(const char *type_str) {
    clickhouse_type_info *type = calloc(1, sizeof(clickhouse_type_info));
    if (!type) return NULL;

    type->type_name = strdup(type_str);

    /* Check for parametric types */
    const char *paren = strchr(type_str, '(');

    if (paren == NULL) {
        /* Simple type */
        type->type_id = clickhouse_type_from_name(type_str);

        /* Geo types are aliases for underlying types */
        if (type->type_id == CH_TYPE_POINT) {
            /* Point = Tuple(Float64, Float64) */
            type->tuple_elements = malloc(2 * sizeof(clickhouse_type_info *));
            type->tuple_size = 2;
            type->tuple_elements[0] = clickhouse_type_parse("Float64");
            type->tuple_elements[1] = clickhouse_type_parse("Float64");
        } else if (type->type_id == CH_TYPE_RING) {
            /* Ring = Array(Point) */
            type->nested = clickhouse_type_parse("Point");
        } else if (type->type_id == CH_TYPE_POLYGON) {
            /* Polygon = Array(Ring) */
            type->nested = clickhouse_type_parse("Ring");
        } else if (type->type_id == CH_TYPE_MULTIPOLYGON) {
            /* MultiPolygon = Array(Polygon) */
            type->nested = clickhouse_type_parse("Polygon");
        }
    } else {
        /* Parametric type */
        size_t base_len = paren - type_str;
        char *base_name = strndup(type_str, base_len);

        if (strcmp(base_name, "FixedString") == 0) {
            type->type_id = CH_TYPE_FIXED_STRING;
            type->fixed_size = (size_t)atol(paren + 1);
        } else if (strcmp(base_name, "Nullable") == 0) {
            type->type_id = CH_TYPE_NULLABLE;
            /* Extract inner type */
            size_t inner_len = strlen(paren + 1) - 1; /* Remove closing paren */
            char *inner_str = strndup(paren + 1, inner_len);
            type->nested = clickhouse_type_parse(inner_str);
            free(inner_str);
        } else if (strcmp(base_name, "Array") == 0) {
            type->type_id = CH_TYPE_ARRAY;
            size_t inner_len = strlen(paren + 1) - 1;
            char *inner_str = strndup(paren + 1, inner_len);
            type->nested = clickhouse_type_parse(inner_str);
            free(inner_str);
        } else if (strcmp(base_name, "LowCardinality") == 0) {
            type->type_id = CH_TYPE_LOWCARDINALITY;
            size_t inner_len = strlen(paren + 1) - 1;
            char *inner_str = strndup(paren + 1, inner_len);
            type->nested = clickhouse_type_parse(inner_str);
            free(inner_str);
        } else if (strcmp(base_name, "DateTime64") == 0) {
            type->type_id = CH_TYPE_DATETIME64;
            type->fixed_size = (size_t)atol(paren + 1); /* precision */
        } else if (strcmp(base_name, "Decimal") == 0 ||
                   strcmp(base_name, "Decimal32") == 0 ||
                   strcmp(base_name, "Decimal64") == 0 ||
                   strcmp(base_name, "Decimal128") == 0 ||
                   strcmp(base_name, "Decimal256") == 0) {
            if (strcmp(base_name, "Decimal32") == 0) type->type_id = CH_TYPE_DECIMAL32;
            else if (strcmp(base_name, "Decimal64") == 0) type->type_id = CH_TYPE_DECIMAL64;
            else if (strcmp(base_name, "Decimal128") == 0) type->type_id = CH_TYPE_DECIMAL128;
            else if (strcmp(base_name, "Decimal256") == 0) type->type_id = CH_TYPE_DECIMAL256;
            else {
                /* Decimal(P, S) - determine type based on precision */
                int precision = atoi(paren + 1);
                if (precision <= 9) type->type_id = CH_TYPE_DECIMAL32;
                else if (precision <= 18) type->type_id = CH_TYPE_DECIMAL64;
                else if (precision <= 38) type->type_id = CH_TYPE_DECIMAL128;
                else type->type_id = CH_TYPE_DECIMAL256;
            }
            /* Store scale for potential future use */
            const char *comma = strchr(paren + 1, ',');
            if (comma) {
                type->fixed_size = (size_t)atoi(comma + 1);  /* Store scale */
            }
        } else if (strcmp(base_name, "Enum8") == 0) {
            type->type_id = CH_TYPE_ENUM8;
            /* Parse enum values: Enum8('name1' = 1, 'name2' = 2, ...) */
            const char *start = paren + 1;
            const char *end = type_str + strlen(type_str) - 1;

            /* Count entries */
            size_t capacity = 8;
            type->enum_values = malloc(capacity * sizeof(clickhouse_enum_value));
            type->enum_count = 0;

            const char *p = start;
            while (p < end) {
                /* Skip whitespace */
                while (p < end && (*p == ' ' || *p == ',')) p++;
                if (p >= end) break;

                /* Find quoted string: 'name' */
                if (*p == '\'') {
                    const char *name_start = p + 1;
                    const char *name_end = strchr(name_start, '\'');
                    if (!name_end) break;

                    /* Extract name */
                    size_t name_len = name_end - name_start;

                    /* Find = and value */
                    p = name_end + 1;
                    while (p < end && (*p == ' ' || *p == '=')) p++;

                    int value = 0;
                    int negative = 0;
                    if (*p == '-') { negative = 1; p++; }
                    while (p < end && *p >= '0' && *p <= '9') {
                        value = value * 10 + (*p - '0');
                        p++;
                    }
                    if (negative) value = -value;

                    /* Grow array if needed */
                    if (type->enum_count >= capacity) {
                        capacity *= 2;
                        type->enum_values = realloc(type->enum_values, capacity * sizeof(clickhouse_enum_value));
                    }

                    type->enum_values[type->enum_count].name = strndup(name_start, name_len);
                    type->enum_values[type->enum_count].value = (int16_t)value;
                    type->enum_count++;
                } else {
                    p++;
                }
            }
        } else if (strcmp(base_name, "Enum16") == 0) {
            type->type_id = CH_TYPE_ENUM16;
            /* Parse enum values (same as Enum8) */
            const char *start = paren + 1;
            const char *end = type_str + strlen(type_str) - 1;

            size_t capacity = 8;
            type->enum_values = malloc(capacity * sizeof(clickhouse_enum_value));
            type->enum_count = 0;

            const char *p = start;
            while (p < end) {
                while (p < end && (*p == ' ' || *p == ',')) p++;
                if (p >= end) break;

                if (*p == '\'') {
                    const char *name_start = p + 1;
                    const char *name_end = strchr(name_start, '\'');
                    if (!name_end) break;

                    size_t name_len = name_end - name_start;

                    p = name_end + 1;
                    while (p < end && (*p == ' ' || *p == '=')) p++;

                    int value = 0;
                    int negative = 0;
                    if (*p == '-') { negative = 1; p++; }
                    while (p < end && *p >= '0' && *p <= '9') {
                        value = value * 10 + (*p - '0');
                        p++;
                    }
                    if (negative) value = -value;

                    if (type->enum_count >= capacity) {
                        capacity *= 2;
                        type->enum_values = realloc(type->enum_values, capacity * sizeof(clickhouse_enum_value));
                    }

                    type->enum_values[type->enum_count].name = strndup(name_start, name_len);
                    type->enum_values[type->enum_count].value = (int16_t)value;
                    type->enum_count++;
                } else {
                    p++;
                }
            }
        } else if (strcmp(base_name, "Tuple") == 0) {
            type->type_id = CH_TYPE_TUPLE;
            /* Parse tuple elements: Tuple(Type1, Type2, ...) or Tuple(name1 Type1, name2 Type2, ...) */
            const char *start = paren + 1;
            const char *end = type_str + strlen(type_str) - 1; /* Skip closing ) */

            /* Count elements and allocate */
            size_t capacity = 4;
            type->tuple_elements = malloc(capacity * sizeof(clickhouse_type_info *));
            type->tuple_size = 0;

            int depth = 0;
            const char *elem_start = start;

            for (const char *p = start; p <= end; p++) {
                if (*p == '(' || *p == '<') depth++;
                else if (*p == ')' || *p == '>') depth--;

                if (((*p == ',' && depth == 0) || p == end) && elem_start < p) {
                    /* Extract element type */
                    size_t elem_len = p - elem_start;
                    /* Skip leading whitespace and newlines */
                    while (elem_len > 0 && (*elem_start == ' ' || *elem_start == '\t' || *elem_start == '\n' || *elem_start == '\r')) {
                        elem_start++;
                        elem_len--;
                    }
                    /* Skip trailing whitespace and newlines */
                    while (elem_len > 0 && (elem_start[elem_len-1] == ' ' || elem_start[elem_len-1] == '\t' ||
                           elem_start[elem_len-1] == '\n' || elem_start[elem_len-1] == '\r')) {
                        elem_len--;
                    }

                    if (elem_len > 0) {
                        char *elem_str = strndup(elem_start, elem_len);

                        /* Check for named tuple element: "fieldname Type" or "fieldname Nullable(Type)" etc.
                         * Named elements have format: identifier followed by space and then type.
                         * The type always starts with uppercase or is a parametric type.
                         * We need to find the first space at depth 0 and check if what follows is a type. */
                        const char *type_str_to_parse = elem_str;
                        char *space = NULL;

                        /* Find first space at depth 0 (not inside parentheses) */
                        int inner_depth = 0;
                        for (char *s = elem_str; *s; s++) {
                            if (*s == '(' || *s == '<') inner_depth++;
                            else if (*s == ')' || *s == '>') inner_depth--;
                            else if (*s == ' ' && inner_depth == 0) {
                                space = s;
                                break;
                            }
                        }

                        if (space) {
                            /* Check if part before space is an identifier (field name) */
                            /* Field names are lowercase identifiers, types start with uppercase or are like 'Nullable(' */
                            char *after_space = space + 1;
                            while (*after_space == ' ') after_space++;

                            /* If what follows starts with uppercase or is a known type pattern, use it */
                            if (after_space[0] >= 'A' && after_space[0] <= 'Z') {
                                type_str_to_parse = after_space;
                            }
                        }

                        if (type->tuple_size >= capacity) {
                            capacity *= 2;
                            type->tuple_elements = realloc(type->tuple_elements,
                                capacity * sizeof(clickhouse_type_info *));
                        }
                        type->tuple_elements[type->tuple_size++] = clickhouse_type_parse(type_str_to_parse);
                        free(elem_str);
                    }
                    elem_start = p + 1;
                }
            }
        } else if (strcmp(base_name, "Map") == 0) {
            type->type_id = CH_TYPE_MAP;
            /* Map(K, V) is stored as Array(Tuple(K, V)) internally */
            /* Parse and store the key/value types as tuple_elements[0] and [1] */
            const char *start = paren + 1;
            const char *end = type_str + strlen(type_str) - 1;

            type->tuple_elements = malloc(2 * sizeof(clickhouse_type_info *));
            type->tuple_size = 0;

            int depth = 0;
            const char *elem_start = start;

            for (const char *p = start; p <= end && type->tuple_size < 2; p++) {
                if (*p == '(' || *p == '<') depth++;
                else if (*p == ')' || *p == '>') depth--;

                if (((*p == ',' && depth == 0) || p == end) && elem_start < p) {
                    size_t elem_len = p - elem_start;
                    while (elem_len > 0 && (*elem_start == ' ' || *elem_start == '\t')) {
                        elem_start++;
                        elem_len--;
                    }
                    while (elem_len > 0 && (elem_start[elem_len-1] == ' ' || elem_start[elem_len-1] == '\t')) {
                        elem_len--;
                    }

                    if (elem_len > 0) {
                        char *elem_str = strndup(elem_start, elem_len);
                        type->tuple_elements[type->tuple_size++] = clickhouse_type_parse(elem_str);
                        free(elem_str);
                    }
                    elem_start = p + 1;
                }
            }
        } else if (strcmp(base_name, "SimpleAggregateFunction") == 0) {
            type->type_id = CH_TYPE_SIMPLEAGGREGATEFUNCTION;
            /* SimpleAggregateFunction(func_name, type) - extract the last argument (the type) */
            const char *start = paren + 1;
            const char *end = type_str + strlen(type_str) - 1;

            /* Find the last comma at depth 0 */
            int depth = 0;
            const char *last_comma = NULL;
            for (const char *p = start; p < end; p++) {
                if (*p == '(' || *p == '<') depth++;
                else if (*p == ')' || *p == '>') depth--;
                else if (*p == ',' && depth == 0) {
                    last_comma = p;
                }
            }

            if (last_comma) {
                /* Extract the type after the last comma */
                const char *type_start = last_comma + 1;
                size_t type_len = end - type_start;
                while (type_len > 0 && (*type_start == ' ' || *type_start == '\t')) {
                    type_start++;
                    type_len--;
                }
                while (type_len > 0 && (type_start[type_len-1] == ' ' || type_start[type_len-1] == '\t')) {
                    type_len--;
                }
                if (type_len > 0) {
                    char *type_str_inner = strndup(type_start, type_len);
                    type->nested = clickhouse_type_parse(type_str_inner);
                    free(type_str_inner);
                }
            }
        } else if (strcmp(base_name, "Object") == 0) {
            /* Object('json') - JSON type stored as string */
            type->type_id = CH_TYPE_OBJECT;
        } else if (strcmp(base_name, "Variant") == 0) {
            /* Variant(T1, T2, ...) - parse variant types like Tuple */
            type->type_id = CH_TYPE_VARIANT;
            const char *start = paren + 1;
            const char *end = type_str + strlen(type_str) - 1;

            size_t capacity = 4;
            type->tuple_elements = malloc(capacity * sizeof(clickhouse_type_info *));
            type->tuple_size = 0;

            int depth = 0;
            const char *elem_start = start;

            for (const char *p = start; p <= end; p++) {
                if (*p == '(' || *p == '<') depth++;
                else if (*p == ')' || *p == '>') depth--;

                if (((*p == ',' && depth == 0) || p == end) && elem_start < p) {
                    size_t elem_len = p - elem_start;
                    while (elem_len > 0 && (*elem_start == ' ' || *elem_start == '\t')) {
                        elem_start++;
                        elem_len--;
                    }
                    while (elem_len > 0 && (elem_start[elem_len-1] == ' ' || elem_start[elem_len-1] == '\t')) {
                        elem_len--;
                    }

                    if (elem_len > 0) {
                        char *elem_str = strndup(elem_start, elem_len);
                        if (type->tuple_size >= capacity) {
                            capacity *= 2;
                            type->tuple_elements = realloc(type->tuple_elements,
                                capacity * sizeof(clickhouse_type_info *));
                        }
                        type->tuple_elements[type->tuple_size++] = clickhouse_type_parse(elem_str);
                        free(elem_str);
                    }
                    elem_start = p + 1;
                }
            }
        } else if (strcmp(base_name, "Dynamic") == 0) {
            /* Dynamic(max_types=N) - dynamic type */
            type->type_id = CH_TYPE_DYNAMIC;
            /* Extract max_types if present */
            const char *max_types = strstr(paren + 1, "max_types");
            if (max_types) {
                const char *eq = strchr(max_types, '=');
                if (eq) {
                    type->fixed_size = (size_t)atol(eq + 1);
                }
            }
        }

        free(base_name);
    }

    return type;
}

void clickhouse_type_free(clickhouse_type_info *type) {
    if (type) {
        free(type->type_name);
        if (type->nested) {
            clickhouse_type_free(type->nested);
        }
        if (type->tuple_elements) {
            for (size_t i = 0; i < type->tuple_size; i++) {
                clickhouse_type_free(type->tuple_elements[i]);
            }
            free(type->tuple_elements);
        }
        if (type->enum_values) {
            for (size_t i = 0; i < type->enum_count; i++) {
                free(type->enum_values[i].name);
            }
            free(type->enum_values);
        }
        free(type);
    }
}

clickhouse_column *clickhouse_column_create(const char *name, clickhouse_type_info *type) {
    clickhouse_column *col = calloc(1, sizeof(clickhouse_column));
    if (!col) return NULL;

    col->name = strdup(name);
    col->type = type;
    col->data = NULL;
    col->nulls = NULL;
    col->row_count = 0;
    col->capacity = 0;
    col->offsets = NULL;
    col->nested_column = NULL;
    col->tuple_columns = NULL;
    col->tuple_column_count = 0;

    return col;
}

void clickhouse_column_free(clickhouse_column *col) {
    if (col) {
        free(col->name);

        /* Free string data if applicable - check type BEFORE freeing it */
        if (col->type && (col->type->type_id == CH_TYPE_STRING ||
            col->type->type_id == CH_TYPE_JSON ||
            col->type->type_id == CH_TYPE_OBJECT ||
            col->type->type_id == CH_TYPE_DYNAMIC ||
            (col->type->type_id == CH_TYPE_NULLABLE && col->type->nested &&
             col->type->nested->type_id == CH_TYPE_STRING))) {
            char **strings = (char **)col->data;
            if (strings) {
                for (size_t i = 0; i < col->row_count; i++) {
                    free(strings[i]);
                }
            }
        }

        /* Now free the type info */
        clickhouse_type_free(col->type);

        free(col->data);
        free(col->nulls);
        free(col->offsets);

        if (col->nested_column) {
            clickhouse_column_free(col->nested_column);
        }

        /* Free tuple columns */
        if (col->tuple_columns) {
            for (size_t i = 0; i < col->tuple_column_count; i++) {
                if (col->tuple_columns[i]) {
                    clickhouse_column_free(col->tuple_columns[i]);
                }
            }
            free(col->tuple_columns);
        }

        /* Free Variant discriminators */
        free(col->discriminators);

        free(col);
    }
}

int clickhouse_column_read(clickhouse_buffer *buf, clickhouse_column *col, size_t row_count) {
    col->row_count = row_count;

    size_t elem_size = clickhouse_type_size(col->type);

    /* Handle Nullable */
    if (col->type->type_id == CH_TYPE_NULLABLE) {
        col->nulls = malloc(row_count);
        if (!col->nulls) return -1;

        for (size_t i = 0; i < row_count; i++) {
            uint8_t is_null;
            if (clickhouse_buffer_read_uint8(buf, &is_null) != 0) return -1;
            col->nulls[i] = is_null;
        }

        /* For Nullable(Nothing), ClickHouse sends 1 byte per row for the "value" even though
         * Nothing has size 0. This appears to be for consistency with how Nullable<T> is serialized.
         * We need to skip these bytes. */
        if (col->type->nested && col->type->nested->type_id == CH_TYPE_NOTHING) {
            for (size_t i = 0; i < row_count; i++) {
                uint8_t dummy;
                if (clickhouse_buffer_read_uint8(buf, &dummy) != 0) return -1;
            }
            return 0;
        }

        /* Read underlying data */
        elem_size = clickhouse_type_size(col->type->nested);
    }

    /* Handle SimpleAggregateFunction - just read as the underlying type */
    if (col->type->type_id == CH_TYPE_SIMPLEAGGREGATEFUNCTION && col->type->nested) {
        /* Create column for the underlying type and read it */
        col->nested_column = clickhouse_column_create("", col->type->nested);
        if (!col->nested_column) return -1;
        col->nested_column->type = col->type->nested;
        if (clickhouse_column_read(buf, col->nested_column, row_count) != 0) {
            col->nested_column->type = NULL;
            return -1;
        }
        col->nested_column->type = NULL;
        return 0;
    }

    /* Handle String type */
    if (col->type->type_id == CH_TYPE_STRING ||
        (col->type->type_id == CH_TYPE_NULLABLE && col->type->nested &&
         col->type->nested->type_id == CH_TYPE_STRING)) {
        /* Strings are stored as array of pointers */
        col->data = calloc(row_count, sizeof(char *));
        if (!col->data) return -1;

        char **strings = (char **)col->data;
        for (size_t i = 0; i < row_count; i++) {
            size_t len;
            if (clickhouse_buffer_read_string(buf, &strings[i], &len) != 0) return -1;
        }
        return 0;
    }

    /* Handle FixedString */
    if (col->type->type_id == CH_TYPE_FIXED_STRING) {
        size_t total = row_count * col->type->fixed_size;
        col->data = malloc(total);
        if (!col->data) return -1;

        if (clickhouse_buffer_read_bytes(buf, col->data, total) != 0) return -1;
        return 0;
    }

    /* Handle LowCardinality type */
    if (col->type->type_id == CH_TYPE_LOWCARDINALITY && col->type->nested) {
        /* LowCardinality serialization format (from clickhouse-cpp):
         * Prefix (LoadPrefix - sent once per column):
         *   - key_version (uint64) = 1 (SharedDictionariesWithAdditionalKeys)
         * Body (LoadBody):
         *   - index_serialization_type (uint64): bits 0-7 = index type, bit 9 = HasAdditionalKeysBit
         *   - number_of_keys (uint64): dictionary entry count
         *   - Dictionary values (column body)
         *   - number_of_rows (uint64)
         *   - Index values
         */

        /* Read key_version (prefix) */
        uint64_t key_version;
        if (clickhouse_buffer_read_bytes(buf, (uint8_t *)&key_version, sizeof(uint64_t)) != 0) return -1;
        /* key_version should be 1 (SharedDictionariesWithAdditionalKeys) */

        /* Read index_serialization_type */
        uint64_t index_serialization_type;
        if (clickhouse_buffer_read_bytes(buf, (uint8_t *)&index_serialization_type, sizeof(uint64_t)) != 0) return -1;

        /* Extract index type from bits 0-7 */
        int index_type = index_serialization_type & 0xFF;
        size_t key_size;
        switch (index_type) {
            case 0: key_size = 1; break;  /* UInt8 */
            case 1: key_size = 2; break;  /* UInt16 */
            case 2: key_size = 4; break;  /* UInt32 */
            case 3: key_size = 8; break;  /* UInt64 */
            default: key_size = 1; break;
        }

        /* Read number_of_keys (dictionary entry count) */
        uint64_t number_of_keys;
        if (clickhouse_buffer_read_bytes(buf, (uint8_t *)&number_of_keys, sizeof(uint64_t)) != 0) return -1;

        /* Read dictionary values.
         * For LowCardinality(Nullable(T)), the dictionary body is just T (not Nullable(T)).
         * Index 0 represents NULL. */
        clickhouse_type_info *dict_type = col->type->nested;
        if (dict_type->type_id == CH_TYPE_NULLABLE && dict_type->nested) {
            /* For Nullable, read inner type as dictionary */
            dict_type = dict_type->nested;
        }

        col->nested_column = clickhouse_column_create("", dict_type);
        if (!col->nested_column) return -1;
        col->nested_column->type = dict_type;

        if (number_of_keys > 0) {
            if (clickhouse_column_read(buf, col->nested_column, (size_t)number_of_keys) != 0) {
                col->nested_column->type = NULL;
                return -1;
            }
        }
        col->nested_column->type = NULL;

        /* Read number_of_rows */
        uint64_t number_of_rows;
        if (clickhouse_buffer_read_uint64(buf, &number_of_rows) != 0) return -1;

        /* Read indices */
        col->offsets = malloc(row_count * sizeof(uint64_t));
        if (!col->offsets) return -1;

        for (size_t i = 0; i < row_count; i++) {
            uint64_t idx = 0;
            int read_result = -1;
            switch (key_size) {
                case 1: {
                    uint8_t val;
                    read_result = clickhouse_buffer_read_uint8(buf, &val);
                    idx = val;
                    break;
                }
                case 2: {
                    uint16_t val;
                    read_result = clickhouse_buffer_read_uint16(buf, &val);
                    idx = val;
                    break;
                }
                case 4: {
                    uint32_t val;
                    read_result = clickhouse_buffer_read_uint32(buf, &val);
                    idx = val;
                    break;
                }
                case 8:
                    read_result = clickhouse_buffer_read_uint64(buf, &idx);
                    break;
            }
            if (read_result != 0) return -1;
            col->offsets[i] = idx;
        }

        return 0;
    }

    /* Handle Map type - stored as Array(Tuple(K, V)) */
    if (col->type->type_id == CH_TYPE_MAP && col->type->tuple_elements && col->type->tuple_size == 2) {
        /* Read offsets (like Array) */
        col->offsets = malloc(row_count * sizeof(uint64_t));
        if (!col->offsets) return -1;

        for (size_t i = 0; i < row_count; i++) {
            if (clickhouse_buffer_read_uint64(buf, &col->offsets[i]) != 0) {
                return -1;
            }
        }

        /* Calculate total elements */
        size_t total_elements = row_count > 0 ? (size_t)col->offsets[row_count - 1] : 0;

        if (total_elements > 0) {
            /* Read keys and values as two tuple columns */
            col->tuple_column_count = 2;
            col->tuple_columns = calloc(2, sizeof(clickhouse_column *));
            if (!col->tuple_columns) return -1;

            /* Read keys */
            col->tuple_columns[0] = clickhouse_column_create("", col->type->tuple_elements[0]);
            if (!col->tuple_columns[0]) return -1;
            col->tuple_columns[0]->type = col->type->tuple_elements[0];
            if (clickhouse_column_read(buf, col->tuple_columns[0], total_elements) != 0) {
                col->tuple_columns[0]->type = NULL;
                return -1;
            }
            col->tuple_columns[0]->type = NULL;

            /* Read values */
            col->tuple_columns[1] = clickhouse_column_create("", col->type->tuple_elements[1]);
            if (!col->tuple_columns[1]) return -1;
            col->tuple_columns[1]->type = col->type->tuple_elements[1];
            if (clickhouse_column_read(buf, col->tuple_columns[1], total_elements) != 0) {
                col->tuple_columns[1]->type = NULL;
                return -1;
            }
            col->tuple_columns[1]->type = NULL;
        }

        return 0;
    }

    /* Handle Tuple type (and Point which is Tuple(Float64, Float64)) */
    if ((col->type->type_id == CH_TYPE_TUPLE || col->type->type_id == CH_TYPE_POINT) && col->type->tuple_elements && col->type->tuple_size > 0) {
        /* Tuple elements are stored as separate columns in sequence */
        col->tuple_column_count = col->type->tuple_size;
        col->tuple_columns = calloc(col->tuple_column_count, sizeof(clickhouse_column *));
        if (!col->tuple_columns) return -1;

        for (size_t i = 0; i < col->tuple_column_count; i++) {
            col->tuple_columns[i] = clickhouse_column_create("", col->type->tuple_elements[i]);
            if (!col->tuple_columns[i]) return -1;

            /* Prevent double-free of shared type */
            col->tuple_columns[i]->type = col->type->tuple_elements[i];

            if (clickhouse_column_read(buf, col->tuple_columns[i], row_count) != 0) {
                col->tuple_columns[i]->type = NULL;
                return -1;
            }
            col->tuple_columns[i]->type = NULL;
        }
        return 0;
    }

    /* Handle Array type (and Ring/Polygon/MultiPolygon which are Array aliases) */
    if ((col->type->type_id == CH_TYPE_ARRAY || col->type->type_id == CH_TYPE_RING ||
         col->type->type_id == CH_TYPE_POLYGON || col->type->type_id == CH_TYPE_MULTIPOLYGON) && col->type->nested) {
        /* Read offsets (cumulative element counts) */
        col->offsets = malloc(row_count * sizeof(uint64_t));
        if (!col->offsets) return -1;

        for (size_t i = 0; i < row_count; i++) {
            if (clickhouse_buffer_read_uint64(buf, &col->offsets[i]) != 0) {
                return -1;
            }
        }

        /* Calculate total elements (last offset is total count) */
        size_t total_elements = row_count > 0 ? (size_t)col->offsets[row_count - 1] : 0;

        if (total_elements > 0) {
            /* Create nested column for element data */
            col->nested_column = clickhouse_column_create("", col->type->nested);
            if (!col->nested_column) return -1;

            /* Don't free the type - it's shared with parent */
            col->nested_column->type = col->type->nested;

            /* Read all elements as a single column */
            if (clickhouse_column_read(buf, col->nested_column, total_elements) != 0) {
                /* Don't double-free the shared type */
                col->nested_column->type = NULL;
                clickhouse_column_free(col->nested_column);
                col->nested_column = NULL;
                return -1;
            }

            /* Prevent double-free of shared type */
            col->nested_column->type = NULL;
        }

        return 0;
    }

    /* Handle JSON type - serialized as String in native protocol */
    if (col->type->type_id == CH_TYPE_JSON) {
        col->data = calloc(row_count, sizeof(char *));
        if (!col->data) return -1;

        char **strings = (char **)col->data;
        for (size_t i = 0; i < row_count; i++) {
            size_t len;
            if (clickhouse_buffer_read_string(buf, &strings[i], &len) != 0) return -1;
        }
        return 0;
    }

    /* Handle Object type - serialized as String (deprecated JSON type) */
    if (col->type->type_id == CH_TYPE_OBJECT) {
        col->data = calloc(row_count, sizeof(char *));
        if (!col->data) return -1;

        char **strings = (char **)col->data;
        for (size_t i = 0; i < row_count; i++) {
            size_t len;
            if (clickhouse_buffer_read_string(buf, &strings[i], &len) != 0) return -1;
        }
        return 0;
    }

    /* Handle Variant type - discriminator byte + nested types */
    if (col->type->type_id == CH_TYPE_VARIANT && col->type->tuple_elements && col->type->tuple_size > 0) {
        /* Read discriminators (one byte per row) */
        col->discriminators = malloc(row_count);
        if (!col->discriminators) return -1;

        for (size_t i = 0; i < row_count; i++) {
            if (clickhouse_buffer_read_uint8(buf, &col->discriminators[i]) != 0) return -1;
        }

        /* Read offsets for each variant type */
        col->offsets = malloc(row_count * sizeof(uint64_t));
        if (!col->offsets) return -1;

        for (size_t i = 0; i < row_count; i++) {
            if (clickhouse_buffer_read_uint64(buf, &col->offsets[i]) != 0) {
                return -1;
            }
        }

        /* Count rows per variant type */
        size_t *type_counts = calloc(col->type->tuple_size, sizeof(size_t));
        if (!type_counts) return -1;

        for (size_t i = 0; i < row_count; i++) {
            uint8_t discrim = col->discriminators[i];
            if (discrim != 0xFF && discrim < col->type->tuple_size) {
                type_counts[discrim]++;
            }
        }

        /* Read data for each variant type */
        col->tuple_column_count = col->type->tuple_size;
        col->tuple_columns = calloc(col->tuple_column_count, sizeof(clickhouse_column *));
        if (!col->tuple_columns) {
            free(type_counts);
            return -1;
        }

        for (size_t v = 0; v < col->type->tuple_size; v++) {
            if (type_counts[v] > 0) {
                col->tuple_columns[v] = clickhouse_column_create("", col->type->tuple_elements[v]);
                if (!col->tuple_columns[v]) {
                    free(type_counts);
                    return -1;
                }
                col->tuple_columns[v]->type = col->type->tuple_elements[v];
                if (clickhouse_column_read(buf, col->tuple_columns[v], type_counts[v]) != 0) {
                    col->tuple_columns[v]->type = NULL;
                    free(type_counts);
                    return -1;
                }
                col->tuple_columns[v]->type = NULL;
            }
        }

        free(type_counts);
        return 0;
    }

    /* Handle Dynamic type - stores type name + data as string */
    if (col->type->type_id == CH_TYPE_DYNAMIC) {
        /* Dynamic is serialized with a variant-like structure in newer ClickHouse.
         * For simplicity, we read it as strings (JSON representation) */
        col->data = calloc(row_count, sizeof(char *));
        if (!col->data) return -1;

        char **strings = (char **)col->data;
        for (size_t i = 0; i < row_count; i++) {
            size_t len;
            if (clickhouse_buffer_read_string(buf, &strings[i], &len) != 0) return -1;
        }
        return 0;
    }

    /* Fixed size types */
    if (elem_size > 0) {
        size_t total = row_count * elem_size;
        col->data = malloc(total);
        if (!col->data) return -1;

        if (clickhouse_buffer_read_bytes(buf, col->data, total) != 0) return -1;
        return 0;
    }

    /* Return success for types with 0 rows */
    if (row_count == 0) {
        return 0;
    }

    return -1; /* Unknown type */
}

int clickhouse_column_write(clickhouse_buffer *buf, clickhouse_column *col) {
    /* Write column name */
    if (clickhouse_buffer_write_string(buf, col->name, strlen(col->name)) != 0) return -1;

    /* Write column type */
    if (clickhouse_buffer_write_string(buf, col->type->type_name, strlen(col->type->type_name)) != 0) return -1;

    size_t elem_size = clickhouse_type_size(col->type);
    const clickhouse_type_info *data_type = col->type;

    /* Handle Nullable */
    if (col->type->type_id == CH_TYPE_NULLABLE && col->nulls) {
        for (size_t i = 0; i < col->row_count; i++) {
            if (clickhouse_buffer_write_uint8(buf, col->nulls[i]) != 0) return -1;
        }
        elem_size = clickhouse_type_size(col->type->nested);
        data_type = col->type->nested;
    }

    /* Handle String (including Nullable(String)) */
    if (data_type->type_id == CH_TYPE_STRING) {
        char **strings = (char **)col->data;
        for (size_t i = 0; i < col->row_count; i++) {
            const char *str = strings[i] ? strings[i] : "";
            if (clickhouse_buffer_write_string(buf, str, strlen(str)) != 0) return -1;
        }
        return 0;
    }

    /* Handle FixedString (including Nullable(FixedString)) */
    if (data_type->type_id == CH_TYPE_FIXED_STRING) {
        if (clickhouse_buffer_write_bytes(buf, col->data, col->row_count * data_type->fixed_size) != 0) return -1;
        return 0;
    }

    /* Fixed size types */
    if (elem_size > 0 && col->data) {
        if (clickhouse_buffer_write_bytes(buf, col->data, col->row_count * elem_size) != 0) return -1;
        return 0;
    }

    return 0;
}

/* Block operations */

clickhouse_block *clickhouse_block_create(void) {
    return calloc(1, sizeof(clickhouse_block));
}

void clickhouse_block_free(clickhouse_block *block) {
    if (block) {
        for (size_t i = 0; i < block->column_count; i++) {
            clickhouse_column_free(block->columns[i]);
        }
        free(block->columns);
        free(block);
    }
}

int clickhouse_block_add_column(clickhouse_block *block, clickhouse_column *col) {
    clickhouse_column **new_cols = realloc(block->columns,
                                           (block->column_count + 1) * sizeof(clickhouse_column *));
    if (!new_cols) return -1;

    block->columns = new_cols;
    block->columns[block->column_count++] = col;

    if (col->row_count > block->row_count) {
        block->row_count = col->row_count;
    }

    return 0;
}

int clickhouse_block_read_header(clickhouse_buffer *buf, char **table_name,
                                  uint64_t *column_count, uint64_t *row_count) {
    /* Block info (temporary table info) */
    uint64_t field_num;

    /* Read block info fields */
    while (1) {
        if (clickhouse_buffer_read_varint(buf, &field_num) != 0) return -1;
        if (field_num == 0) break;

        if (field_num == 1) {
            /* is_overflows */
            uint8_t val;
            if (clickhouse_buffer_read_uint8(buf, &val) != 0) return -1;
        } else if (field_num == 2) {
            /* bucket_num */
            int32_t val;
            if (clickhouse_buffer_read_int32(buf, &val) != 0) return -1;
        }
    }

    /* Number of columns */
    if (clickhouse_buffer_read_varint(buf, column_count) != 0) return -1;

    /* Number of rows */
    if (clickhouse_buffer_read_varint(buf, row_count) != 0) return -1;

    return 0;
}

int clickhouse_block_write_header(clickhouse_buffer *buf, const char *table_name,
                                   uint64_t column_count, uint64_t row_count) {
    /* Block info */
    if (clickhouse_buffer_write_varint(buf, 1) != 0) return -1;  /* field num */
    if (clickhouse_buffer_write_uint8(buf, 0) != 0) return -1;   /* is_overflows */
    if (clickhouse_buffer_write_varint(buf, 2) != 0) return -1;  /* field num */
    if (clickhouse_buffer_write_int32(buf, -1) != 0) return -1;  /* bucket_num */
    if (clickhouse_buffer_write_varint(buf, 0) != 0) return -1;  /* end of info */

    /* Column count */
    if (clickhouse_buffer_write_varint(buf, column_count) != 0) return -1;

    /* Row count */
    if (clickhouse_buffer_write_varint(buf, row_count) != 0) return -1;

    return 0;
}

int clickhouse_block_read(clickhouse_buffer *buf, clickhouse_block *block) {
    char *table_name = NULL;
    uint64_t column_count, row_count;

    if (clickhouse_block_read_header(buf, &table_name, &column_count, &row_count) != 0) {
        return -1;
    }

    block->row_count = (size_t)row_count;

    for (uint64_t i = 0; i < column_count; i++) {
        char *col_name;
        size_t col_name_len;
        char *type_name;
        size_t type_name_len;

        if (clickhouse_buffer_read_string(buf, &col_name, &col_name_len) != 0) return -1;
        if (clickhouse_buffer_read_string(buf, &type_name, &type_name_len) != 0) {
            free(col_name);
            return -1;
        }

        clickhouse_type_info *type = clickhouse_type_parse(type_name);
        clickhouse_column *col = clickhouse_column_create(col_name, type);

        free(col_name);
        free(type_name);

        if (!col) return -1;

        if (row_count > 0) {
            if (clickhouse_column_read(buf, col, (size_t)row_count) != 0) {
                clickhouse_column_free(col);
                return -1;
            }
        }

        if (clickhouse_block_add_column(block, col) != 0) {
            clickhouse_column_free(col);
            return -1;
        }
    }

    return 0;
}

int clickhouse_block_write(clickhouse_buffer *buf, clickhouse_block *block) {
    if (clickhouse_block_write_header(buf, "", block->column_count, block->row_count) != 0) {
        return -1;
    }

    for (size_t i = 0; i < block->column_count; i++) {
        if (clickhouse_column_write(buf, block->columns[i]) != 0) {
            return -1;
        }
    }

    return 0;
}
