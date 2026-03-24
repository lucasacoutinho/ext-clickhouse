#include "src/column_write.h"
#include "src/common.h"

#include "clickhouse/columns/array.h"
#include "clickhouse/columns/date.h"
#include "clickhouse/columns/decimal.h"
#include "clickhouse/columns/enum.h"
#include "clickhouse/columns/ip4.h"
#include "clickhouse/columns/ip6.h"
#include "clickhouse/columns/nullable.h"
#include "clickhouse/columns/numeric.h"
#include "clickhouse/columns/string.h"
#include "clickhouse/columns/tuple.h"
#include "clickhouse/columns/uuid.h"
#include "clickhouse/columns/geo.h"
#include "clickhouse/columns/lowcardinality.h"
#include "clickhouse/columns/map.h"
#include "clickhouse/columns/factory.h"
#include "clickhouse/types/types.h"

#include "absl/numeric/int128.h"

#include <cstring>
#include <sstream>
#include <string>
#include <ctime>
#include <arpa/inet.h>

using namespace clickhouse;

/* Forward declaration for recursive calls */
void php_clickhouse_zval_to_column(ColumnRef &col, zval *value);

template <typename T>
static void write_numeric(ColumnRef &col, zval *value) {
    auto typed = col->As<ColumnVector<T>>();
    if (!typed) {
        zend_throw_exception(clickhouse_ce_ValidationException, "Column type mismatch", 0);
        return;
    }
    zend_long lval = zval_get_long(value);
    typed->Append(static_cast<T>(lval));
}

template <typename T>
static void write_float(ColumnRef &col, zval *value) {
    auto typed = col->As<ColumnVector<T>>();
    if (!typed) {
        zend_throw_exception(clickhouse_ce_ValidationException, "Column type mismatch", 0);
        return;
    }
    double dval = zval_get_double(value);
    typed->Append(static_cast<T>(dval));
}

static void write_string(ColumnRef &col, zval *value) {
    auto typed = col->As<ColumnString>();
    if (!typed) {
        zend_throw_exception(clickhouse_ce_ValidationException, "Column type mismatch", 0);
        return;
    }
    zend_string *str = zval_get_string(value);
    typed->Append(std::string_view(ZSTR_VAL(str), ZSTR_LEN(str)));
    zend_string_release(str);
}

static void write_fixed_string(ColumnRef &col, zval *value) {
    auto typed = col->As<ColumnFixedString>();
    if (!typed) {
        zend_throw_exception(clickhouse_ce_ValidationException, "Column type mismatch", 0);
        return;
    }
    zend_string *str = zval_get_string(value);
    typed->Append(std::string_view(ZSTR_VAL(str), ZSTR_LEN(str)));
    zend_string_release(str);
}

static void write_date(ColumnRef &col, zval *value) {
    auto typed = col->As<ColumnDate>();
    if (!typed) {
        zend_throw_exception(clickhouse_ce_ValidationException, "Column type mismatch", 0);
        return;
    }

    if (Z_TYPE_P(value) == IS_STRING) {
        /* Parse 'YYYY-MM-DD' */
        struct tm tm_buf = {};
        if (strptime(Z_STRVAL_P(value), "%Y-%m-%d", &tm_buf)) {
            typed->Append(timegm(&tm_buf));
            return;
        }
    }
    /* Fall back to numeric (Unix timestamp) */
    typed->Append(static_cast<std::time_t>(zval_get_long(value)));
}

static void write_date32(ColumnRef &col, zval *value) {
    auto typed = col->As<ColumnDate32>();
    if (!typed) {
        zend_throw_exception(clickhouse_ce_ValidationException, "Column type mismatch", 0);
        return;
    }

    if (Z_TYPE_P(value) == IS_STRING) {
        struct tm tm_buf = {};
        if (strptime(Z_STRVAL_P(value), "%Y-%m-%d", &tm_buf)) {
            typed->Append(timegm(&tm_buf));
            return;
        }
    }
    typed->Append(static_cast<std::time_t>(zval_get_long(value)));
}

static void write_datetime(ColumnRef &col, zval *value) {
    auto typed = col->As<ColumnDateTime>();
    if (!typed) {
        zend_throw_exception(clickhouse_ce_ValidationException, "Column type mismatch", 0);
        return;
    }
    typed->Append(static_cast<std::time_t>(zval_get_long(value)));
}

static void write_nullable(ColumnRef &col, zval *value) {
    auto typed = col->As<ColumnNullable>();
    if (!typed) {
        zend_throw_exception(clickhouse_ce_ValidationException, "Column type mismatch", 0);
        return;
    }

    if (Z_TYPE_P(value) == IS_NULL) {
        typed->Append(true);
        /* Append a type-appropriate default to the nested column to keep sizes in sync */
        auto nested = typed->Nested();
        zval default_val;
        switch (nested->Type()->GetCode()) {
            case Type::String:
            case Type::FixedString:
            case Type::UUID:
            case Type::IPv4:
            case Type::IPv6:
                ZVAL_EMPTY_STRING(&default_val);
                break;
            case Type::Float32:
            case Type::Float64:
                ZVAL_DOUBLE(&default_val, 0.0);
                break;
            default:
                ZVAL_LONG(&default_val, 0);
                break;
        }
        php_clickhouse_zval_to_column(nested, &default_val);
        zval_ptr_dtor(&default_val);
    } else {
        typed->Append(false);
        auto nested = typed->Nested();
        php_clickhouse_zval_to_column(nested, value);
    }
}

static void write_array(ColumnRef &col, zval *value) {
    if (Z_TYPE_P(value) != IS_ARRAY) {
        zend_throw_exception(clickhouse_ce_ValidationException,
            "Expected array for Array column type", 0);
        return;
    }

    auto typed = col->As<ColumnArray>();
    if (!typed) {
        zend_throw_exception(clickhouse_ce_ValidationException, "Column type mismatch", 0);
        return;
    }

    /* ColumnArray::AppendAsColumn converts a column to one array row.
     * We build a temporary column of the nested type, populate it, then append. */
    auto nested_type = col->Type()->As<ArrayType>()->GetItemType();
    auto nested_col = CreateColumnByType(nested_type->GetName());
    if (!nested_col) {
        zend_throw_exception(clickhouse_ce_ValidationException,
            "Cannot create nested column for Array type", 0);
        return;
    }

    HashTable *ht = Z_ARRVAL_P(value);
    zval *entry;
    ZEND_HASH_FOREACH_VAL(ht, entry) {
        php_clickhouse_zval_to_column(nested_col, entry);
        if (EG(exception)) return;
    } ZEND_HASH_FOREACH_END();

    typed->AppendAsColumn(nested_col);
}

static void write_enum8(ColumnRef &col, zval *value) {
    auto typed = col->As<ColumnEnum8>();
    if (!typed) {
        zend_throw_exception(clickhouse_ce_ValidationException, "Column type mismatch", 0);
        return;
    }
    zend_string *str = zval_get_string(value);
    typed->Append(static_cast<int8_t>(
        typed->Type()->As<EnumType>()->GetEnumValue(std::string(ZSTR_VAL(str), ZSTR_LEN(str)))));
    zend_string_release(str);
}

static void write_enum16(ColumnRef &col, zval *value) {
    auto typed = col->As<ColumnEnum16>();
    if (!typed) {
        zend_throw_exception(clickhouse_ce_ValidationException, "Column type mismatch", 0);
        return;
    }
    zend_string *str = zval_get_string(value);
    typed->Append(static_cast<int16_t>(
        typed->Type()->As<EnumType>()->GetEnumValue(std::string(ZSTR_VAL(str), ZSTR_LEN(str)))));
    zend_string_release(str);
}

static void write_uuid(ColumnRef &col, zval *value) {
    auto typed = col->As<ColumnUUID>();
    if (!typed) {
        zend_throw_exception(clickhouse_ce_ValidationException, "Column type mismatch", 0);
        return;
    }

    zend_string *str = zval_get_string(value);
    std::string uuid_str(ZSTR_VAL(str), ZSTR_LEN(str));
    zend_string_release(str);

    /* Parse UUID string: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx */
    uint64_t hi = 0, lo = 0;
    unsigned int p[8];
    if (sscanf(uuid_str.c_str(), "%8x-%4x-%4x-%4x-%4x%8x",
               &p[0], &p[1], &p[2], &p[3], &p[4], &p[5]) == 6) {
        hi = (static_cast<uint64_t>(p[0]) << 32) |
             (static_cast<uint64_t>(p[1]) << 16) |
              static_cast<uint64_t>(p[2]);
        lo = (static_cast<uint64_t>(p[3]) << 48) |
             (static_cast<uint64_t>(p[4]) << 32) |
              static_cast<uint64_t>(p[5]);
    }

    typed->Append(clickhouse::UUID{hi, lo});
}

static void write_datetime64(ColumnRef &col, zval *value) {
    auto typed = col->As<ColumnDateTime64>();
    if (!typed) {
        zend_throw_exception(clickhouse_ce_ValidationException, "Column type mismatch", 0);
        return;
    }

    if (Z_TYPE_P(value) == IS_LONG) {
        typed->Append(static_cast<Int64>(Z_LVAL_P(value)));
        return;
    }

    if (Z_TYPE_P(value) == IS_STRING) {
        /* Parse "YYYY-MM-DD HH:MM:SS.fractional" */
        struct tm tm_buf = {};
        const char *str = Z_STRVAL_P(value);
        const char *rest = strptime(str, "%Y-%m-%d %H:%M:%S", &tm_buf);
        int64_t seconds = static_cast<int64_t>(timegm(&tm_buf));
        int64_t frac = 0;
        size_t precision = typed->GetPrecision();

        if (rest && *rest == '.') {
            rest++;
            /* Parse fractional digits up to precision */
            int64_t multiplier = 1;
            for (size_t i = 0; i < precision; ++i) multiplier *= 10;
            int64_t parsed_frac = 0;
            size_t digits = 0;
            while (*rest >= '0' && *rest <= '9' && digits < precision) {
                parsed_frac = parsed_frac * 10 + (*rest - '0');
                rest++;
                digits++;
            }
            /* Pad remaining precision digits with zeros */
            for (size_t i = digits; i < precision; ++i) {
                parsed_frac *= 10;
            }
            frac = parsed_frac;
        }

        int64_t divisor = 1;
        for (size_t i = 0; i < precision; ++i) divisor *= 10;
        typed->Append(seconds * divisor + frac);
        return;
    }

    /* Fall back: convert to long */
    typed->Append(static_cast<Int64>(zval_get_long(value)));
}

static void write_decimal(ColumnRef &col, zval *value) {
    auto typed = col->As<ColumnDecimal>();
    if (!typed) {
        zend_throw_exception(clickhouse_ce_ValidationException, "Column type mismatch", 0);
        return;
    }
    zend_string *str = zval_get_string(value);
    typed->Append(std::string(ZSTR_VAL(str), ZSTR_LEN(str)));
    zend_string_release(str);
}

static void write_ipv4(ColumnRef &col, zval *value) {
    auto typed = col->As<ColumnIPv4>();
    if (!typed) {
        zend_throw_exception(clickhouse_ce_ValidationException, "Column type mismatch", 0);
        return;
    }

    if (Z_TYPE_P(value) == IS_STRING) {
        typed->Append(std::string(Z_STRVAL_P(value), Z_STRLEN_P(value)));
    } else {
        /* Numeric: treat as host-byte-order uint32 */
        typed->Append(static_cast<uint32_t>(zval_get_long(value)));
    }
}

static void write_ipv6(ColumnRef &col, zval *value) {
    auto typed = col->As<ColumnIPv6>();
    if (!typed) {
        zend_throw_exception(clickhouse_ce_ValidationException, "Column type mismatch", 0);
        return;
    }

    if (Z_TYPE_P(value) == IS_STRING) {
        /* ColumnIPv6::Append(string_view) parses text representation */
        in6_addr addr;
        if (inet_pton(AF_INET6, Z_STRVAL_P(value), &addr) == 1) {
            typed->Append(addr);
        } else {
            zend_throw_exception_ex(clickhouse_ce_ValidationException, 0,
                "Invalid IPv6 address: %s", Z_STRVAL_P(value));
        }
    } else {
        zend_throw_exception(clickhouse_ce_ValidationException,
            "IPv6 column expects string value", 0);
    }
}

static void write_int128(ColumnRef &col, zval *value) {
    auto typed = col->As<ColumnVector<absl::int128>>();
    if (!typed) {
        zend_throw_exception(clickhouse_ce_ValidationException, "Column type mismatch", 0);
        return;
    }

    if (Z_TYPE_P(value) == IS_LONG) {
        typed->Append(static_cast<absl::int128>(Z_LVAL_P(value)));
        return;
    }

    /* Parse string decimal representation */
    zend_string *str = zval_get_string(value);
    std::string s(ZSTR_VAL(str), ZSTR_LEN(str));
    zend_string_release(str);

    bool negative = false;
    size_t pos = 0;
    if (!s.empty() && s[0] == '-') { negative = true; pos = 1; }

    absl::int128 result = 0;
    for (; pos < s.size(); ++pos) {
        if (s[pos] < '0' || s[pos] > '9') break;
        result = result * 10 + (s[pos] - '0');
    }
    if (negative) result = -result;
    typed->Append(result);
}

static void write_uint128(ColumnRef &col, zval *value) {
    auto typed = col->As<ColumnVector<absl::uint128>>();
    if (!typed) {
        zend_throw_exception(clickhouse_ce_ValidationException, "Column type mismatch", 0);
        return;
    }

    if (Z_TYPE_P(value) == IS_LONG && Z_LVAL_P(value) >= 0) {
        typed->Append(static_cast<absl::uint128>(Z_LVAL_P(value)));
        return;
    }

    zend_string *str = zval_get_string(value);
    std::string s(ZSTR_VAL(str), ZSTR_LEN(str));
    zend_string_release(str);

    absl::uint128 result = 0;
    for (size_t pos = 0; pos < s.size(); ++pos) {
        if (s[pos] < '0' || s[pos] > '9') break;
        result = result * 10 + (s[pos] - '0');
    }
    typed->Append(result);
}

static void write_tuple(ColumnRef &col, zval *value) {
    if (Z_TYPE_P(value) != IS_ARRAY) {
        zend_throw_exception(clickhouse_ce_ValidationException,
            "Expected array for Tuple column type", 0);
        return;
    }

    auto typed = col->As<ColumnTuple>();
    if (!typed) {
        zend_throw_exception(clickhouse_ce_ValidationException, "Column type mismatch", 0);
        return;
    }

    size_t tuple_size = typed->TupleSize();
    HashTable *ht = Z_ARRVAL_P(value);

    if (zend_hash_num_elements(ht) != tuple_size) {
        zend_throw_exception_ex(clickhouse_ce_ValidationException, 0,
            "Tuple expects %zu elements, got %u",
            tuple_size, zend_hash_num_elements(ht));
        return;
    }

    /* Append to each element column by index */
    for (size_t i = 0; i < tuple_size; ++i) {
        zval *elem = zend_hash_index_find(ht, i);
        if (!elem) {
            zend_throw_exception_ex(clickhouse_ce_ValidationException, 0,
                "Tuple array must be indexed (missing index %zu)", i);
            return;
        }
        auto elem_col = (*typed)[i];
        php_clickhouse_zval_to_column(elem_col, elem);
        if (EG(exception)) return;
    }
}

static void write_map(ColumnRef &col, zval *value) {
    if (Z_TYPE_P(value) != IS_ARRAY) {
        zend_throw_exception(clickhouse_ce_ValidationException,
            "Expected array for Map column type", 0);
        return;
    }

    auto typed = col->As<ColumnMap>();
    if (!typed) {
        zend_throw_exception(clickhouse_ce_ValidationException, "Column type mismatch", 0);
        return;
    }

    /* Map is Array(Tuple(K,V)). Build a tuple column with key + value columns,
     * then wrap in AppendAsColumn. We use CreateColumnByType for the key/value types. */
    auto map_type = col->Type()->As<MapType>();
    auto key_type = map_type->GetKeyType();
    auto val_type = map_type->GetValueType();

    auto key_col = CreateColumnByType(key_type->GetName());
    auto val_col = CreateColumnByType(val_type->GetName());
    if (!key_col || !val_col) {
        zend_throw_exception(clickhouse_ce_ValidationException,
            "Cannot create key/value columns for Map type", 0);
        return;
    }

    HashTable *ht = Z_ARRVAL_P(value);
    zend_string *key_str;
    zend_ulong key_idx;
    zval *entry;

    ZEND_HASH_FOREACH_KEY_VAL(ht, key_idx, key_str, entry) {
        /* Append key */
        zval key_zv;
        if (key_str) {
            ZVAL_STR_COPY(&key_zv, key_str);
        } else {
            ZVAL_LONG(&key_zv, static_cast<zend_long>(key_idx));
        }
        php_clickhouse_zval_to_column(key_col, &key_zv);
        zval_ptr_dtor(&key_zv);
        if (EG(exception)) return;

        /* Append value */
        php_clickhouse_zval_to_column(val_col, entry);
        if (EG(exception)) return;
    } ZEND_HASH_FOREACH_END();

    /* Build Tuple(K,V) column containing all kv pairs for this map row */
    std::vector<ColumnRef> tuple_cols = {key_col, val_col};
    auto tuple_col = std::make_shared<ColumnTuple>(tuple_cols);

    /* ColumnMap::Append(ColumnRef) only accepts another ColumnMap (checks As<ColumnMap>).
     * So we build a temporary one-row ColumnMap:
     * 1. Create an empty tuple structure for the ColumnArray wrapper
     * 2. Create ColumnArray, AppendAsColumn to add one row of kv pairs
     * 3. Wrap in ColumnMap so the target's Append accepts it */
    auto empty_tuple = std::make_shared<ColumnTuple>(std::vector<ColumnRef>{
        CreateColumnByType(key_type->GetName()),
        CreateColumnByType(val_type->GetName())
    });
    auto temp_array = std::make_shared<ColumnArray>(empty_tuple);
    temp_array->AppendAsColumn(tuple_col);

    auto temp_map = std::make_shared<ColumnMap>(temp_array);
    typed->Append(temp_map);
}

static void write_lowcardinality(ColumnRef &col, zval *value) {
    auto typed = col->As<ColumnLowCardinality>();
    if (!typed) {
        zend_throw_exception(clickhouse_ce_ValidationException, "Column type mismatch", 0);
        return;
    }

    /* Create a temporary one-row LC column of the same type, populate it via
     * the nested column's public Append, then merge into the target via Append(ColumnRef). */
    auto temp_col = CreateColumnByType(col->Type()->GetName());
    if (!temp_col) {
        zend_throw_exception(clickhouse_ce_ValidationException,
            "Cannot create temporary LowCardinality column", 0);
        return;
    }

    auto temp_lc = temp_col->As<ColumnLowCardinality>();
    if (!temp_lc) {
        zend_throw_exception(clickhouse_ce_ValidationException,
            "Temporary column is not LowCardinality", 0);
        return;
    }

    auto nested_type = typed->GetNestedType();
    auto nested_code = nested_type->GetCode();

    /* For Nullable(T) nested in LC, handle null */
    if (nested_code == Type::Nullable && Z_TYPE_P(value) == IS_NULL) {
        /* Create a one-row nested Nullable column with a null value */
        auto nested_col = CreateColumnByType(nested_type->GetName());
        if (nested_col) {
            auto nullable = nested_col->As<ColumnNullable>();
            if (nullable) {
                nullable->Append(true);
                /* Also need a default in the inner nested column */
                auto inner = nullable->Nested();
                auto inner_code = inner->Type()->GetCode();
                switch (inner_code) {
                    case Type::String:
                    case Type::FixedString: {
                        auto str_col = inner->As<ColumnString>();
                        if (str_col) str_col->Append(std::string_view(""));
                        else {
                            auto fs_col = inner->As<ColumnFixedString>();
                            if (fs_col) fs_col->Append(std::string_view(""));
                        }
                        break;
                    }
                    default: {
                        zval default_val;
                        ZVAL_LONG(&default_val, 0);
                        php_clickhouse_zval_to_column(inner, &default_val);
                        break;
                    }
                }
            }
            temp_lc->Append(nested_col);
        }
        typed->Append(temp_col);
        return;
    }

    /* Build a one-row nested column with the value, then append to temp LC */
    Type::Code item_code = nested_code;
    std::string nested_type_name = nested_type->GetName();
    if (nested_code == Type::Nullable) {
        item_code = nested_type->As<NullableType>()->GetNestedType()->GetCode();
        nested_type_name = nested_type->As<NullableType>()->GetNestedType()->GetName();
    }

    /* Create a column of the base nested type, write the value into it */
    auto nested_col = CreateColumnByType(nested_type->GetName());
    if (!nested_col) {
        zend_throw_exception_ex(clickhouse_ce_ValidationException, 0,
            "Cannot create nested column for LowCardinality: %s",
            nested_type->GetName().c_str());
        return;
    }

    if (nested_code == Type::Nullable) {
        /* Wrap in Nullable: not-null */
        auto nullable = nested_col->As<ColumnNullable>();
        if (nullable) {
            nullable->Append(false);
            auto inner = nullable->Nested();
            php_clickhouse_zval_to_column(inner, value);
            if (EG(exception)) return;
        }
    } else {
        php_clickhouse_zval_to_column(nested_col, value);
        if (EG(exception)) return;
    }

    temp_lc->Append(nested_col);
    typed->Append(temp_col);
}

static void write_point(ColumnRef &col, zval *value) {
    if (Z_TYPE_P(value) != IS_ARRAY) {
        zend_throw_exception(clickhouse_ce_ValidationException,
            "Expected [x, y] array for Point column", 0);
        return;
    }

    auto typed = col->As<ColumnPoint>();
    if (!typed) {
        zend_throw_exception(clickhouse_ce_ValidationException, "Column type mismatch", 0);
        return;
    }

    HashTable *ht = Z_ARRVAL_P(value);
    zval *x_zv = zend_hash_index_find(ht, 0);
    zval *y_zv = zend_hash_index_find(ht, 1);
    if (!x_zv || !y_zv) {
        zend_throw_exception(clickhouse_ce_ValidationException,
            "Point expects array with indices 0 and 1", 0);
        return;
    }

    double x = zval_get_double(x_zv);
    double y = zval_get_double(y_zv);
    typed->Append(std::make_tuple(x, y));
}

/* Helper: parse a PHP [x, y] array into a ColumnPoint row */
static bool parse_points_from_zval(zval *value, std::shared_ptr<ColumnPoint> &point_col) {
    if (Z_TYPE_P(value) != IS_ARRAY) {
        zend_throw_exception(clickhouse_ce_ValidationException,
            "Ring element must be [x, y] array", 0);
        return false;
    }
    HashTable *ht = Z_ARRVAL_P(value);
    zval *entry;
    ZEND_HASH_FOREACH_VAL(ht, entry) {
        if (Z_TYPE_P(entry) != IS_ARRAY) {
            zend_throw_exception(clickhouse_ce_ValidationException,
                "Ring element must be [x, y] array", 0);
            return false;
        }
        HashTable *pt = Z_ARRVAL_P(entry);
        zval *x = zend_hash_index_find(pt, 0);
        zval *y = zend_hash_index_find(pt, 1);
        if (!x || !y) {
            zend_throw_exception(clickhouse_ce_ValidationException,
                "Point must have indices 0 and 1", 0);
            return false;
        }
        point_col->Append(std::make_tuple(zval_get_double(x), zval_get_double(y)));
    } ZEND_HASH_FOREACH_END();
    return true;
}

/* Ring, Polygon, MultiPolygon write:
 *
 * ColumnGeo::Append(ColumnRef) checks column->As<ColumnGeo>(), so we must build
 * a temporary column of the SAME geo type with one row, then Append to merge.
 *
 * Ring    = ColumnGeo<ColumnArrayT<ColumnPoint>>       — one row = array of points
 * Polygon = ColumnGeo<ColumnArrayT<ColumnRing>>        — one row = array of rings
 * MultiPo = ColumnGeo<ColumnArrayT<ColumnPolygon>>     — one row = array of polygons
 */
static void write_ring(ColumnRef &col, zval *value) {
    /* Build a ColumnPoint with all the ring's points */
    auto point_col = std::make_shared<ColumnPoint>();
    if (!parse_points_from_zval(value, point_col)) return;

    /* Build a one-row ColumnArrayT<ColumnPoint> containing these points */
    auto point_array = std::make_shared<ColumnArrayT<ColumnPoint>>(std::make_shared<ColumnPoint>());
    point_array->AppendAsColumn(point_col);

    /* Wrap in a temp ColumnRing, then merge into target */
    auto temp_ring = std::make_shared<ColumnRing>(point_array);
    col->Append(temp_ring);
}

static void write_polygon(ColumnRef &col, zval *value) {
    if (Z_TYPE_P(value) != IS_ARRAY) {
        zend_throw_exception(clickhouse_ce_ValidationException,
            "Expected array of rings for Polygon", 0);
        return;
    }

    /* Build a ColumnRing containing all the rings in this polygon */
    auto ring_col = std::make_shared<ColumnRing>();
    HashTable *ht = Z_ARRVAL_P(value);
    zval *ring_entry;
    ZEND_HASH_FOREACH_VAL(ht, ring_entry) {
        /* Each ring_entry is an array of [x,y] points */
        ColumnRef ring_ref = ring_col;
        write_ring(ring_ref, ring_entry);
        if (EG(exception)) return;
    } ZEND_HASH_FOREACH_END();

    /* Build a one-row ColumnArrayT<ColumnRing> containing these rings */
    auto ring_array = std::make_shared<ColumnArrayT<ColumnRing>>(std::make_shared<ColumnRing>());
    ring_array->AppendAsColumn(ring_col);

    /* Wrap in a temp ColumnPolygon, then merge into target */
    auto temp_polygon = std::make_shared<ColumnPolygon>(ring_array);
    col->Append(temp_polygon);
}

static void write_multipolygon(ColumnRef &col, zval *value) {
    if (Z_TYPE_P(value) != IS_ARRAY) {
        zend_throw_exception(clickhouse_ce_ValidationException,
            "Expected array of polygons for MultiPolygon", 0);
        return;
    }

    /* Build a ColumnPolygon containing all polygons */
    auto polygon_col = std::make_shared<ColumnPolygon>();
    HashTable *ht = Z_ARRVAL_P(value);
    zval *poly_entry;
    ZEND_HASH_FOREACH_VAL(ht, poly_entry) {
        ColumnRef poly_ref = polygon_col;
        write_polygon(poly_ref, poly_entry);
        if (EG(exception)) return;
    } ZEND_HASH_FOREACH_END();

    /* Build a one-row ColumnArrayT<ColumnPolygon> containing these polygons */
    auto poly_array = std::make_shared<ColumnArrayT<ColumnPolygon>>(std::make_shared<ColumnPolygon>());
    poly_array->AppendAsColumn(polygon_col);

    /* Wrap in a temp ColumnMultiPolygon, then merge into target */
    auto temp_mp = std::make_shared<ColumnMultiPolygon>(poly_array);
    col->Append(temp_mp);
}

void php_clickhouse_zval_to_column(ColumnRef &col, zval *value)
{
    switch (col->Type()->GetCode()) {
        case Type::Int8:    write_numeric<int8_t>(col, value); break;
        case Type::Int16:   write_numeric<int16_t>(col, value); break;
        case Type::Int32:   write_numeric<int32_t>(col, value); break;
        case Type::Int64:   write_numeric<int64_t>(col, value); break;
        case Type::UInt8:   write_numeric<uint8_t>(col, value); break;
        case Type::UInt16:  write_numeric<uint16_t>(col, value); break;
        case Type::UInt32:  write_numeric<uint32_t>(col, value); break;
        case Type::UInt64:  write_numeric<uint64_t>(col, value); break;
        case Type::Float32: write_float<float>(col, value); break;
        case Type::Float64: write_float<double>(col, value); break;

        case Type::String:      write_string(col, value); break;
        case Type::FixedString: write_fixed_string(col, value); break;

        case Type::Date:       write_date(col, value); break;
        case Type::Date32:     write_date32(col, value); break;
        case Type::DateTime:   write_datetime(col, value); break;
        case Type::DateTime64: write_datetime64(col, value); break;

        case Type::Decimal:
        case Type::Decimal32:
        case Type::Decimal64:
        case Type::Decimal128: write_decimal(col, value); break;

        case Type::Nullable: write_nullable(col, value); break;
        case Type::Array:    write_array(col, value); break;
        case Type::Tuple:    write_tuple(col, value); break;
        case Type::Map:      write_map(col, value); break;

        case Type::Enum8:  write_enum8(col, value); break;
        case Type::Enum16: write_enum16(col, value); break;

        case Type::UUID:  write_uuid(col, value); break;
        case Type::IPv4:  write_ipv4(col, value); break;
        case Type::IPv6:  write_ipv6(col, value); break;

        case Type::Int128:  write_int128(col, value); break;
        case Type::UInt128: write_uint128(col, value); break;

        case Type::LowCardinality: write_lowcardinality(col, value); break;

        case Type::Point:        write_point(col, value); break;
        case Type::Ring:         write_ring(col, value); break;
        case Type::Polygon:      write_polygon(col, value); break;
        case Type::MultiPolygon: write_multipolygon(col, value); break;

        default:
            zend_throw_exception_ex(clickhouse_ce_ValidationException, 0,
                "Write not supported for type: %s",
                col->Type()->GetName().c_str());
            break;
    }
}
