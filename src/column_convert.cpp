#include "src/column_convert.h"
#include "src/common.h"

#include "clickhouse/columns/array.h"
#include "clickhouse/columns/date.h"
#include "clickhouse/columns/decimal.h"
#include "clickhouse/columns/enum.h"
#include "clickhouse/columns/geo.h"
#include "clickhouse/columns/ip4.h"
#include "clickhouse/columns/ip6.h"
#include "clickhouse/columns/lowcardinality.h"
#include "clickhouse/columns/map.h"
#include "clickhouse/columns/nothing.h"
#include "clickhouse/columns/nullable.h"
#include "clickhouse/columns/numeric.h"
#include "clickhouse/columns/string.h"
#include "clickhouse/columns/tuple.h"
#include "clickhouse/columns/uuid.h"
#include "clickhouse/types/types.h"

#include "absl/numeric/int128.h"

#include <cstdio>
#include <cstring>
#include <cinttypes>
#include <sstream>
#include <string>
#include <tuple>
#include <arpa/inet.h> /* ntohl, inet_ntop */

using namespace clickhouse;

void php_clickhouse_column_to_zval(const ColumnRef &col, size_t index, zval *return_value);

template <typename T>
static inline void numeric_to_zval_long(const ColumnRef &col, size_t index, zval *rv) {
    auto typed = col->As<ColumnVector<T>>();
    ZVAL_LONG(rv, static_cast<zend_long>(typed->At(index)));
}

static void uint64_to_zval(const ColumnRef &col, size_t index, zval *rv) {
    auto typed = col->As<ColumnVector<uint64_t>>();
    uint64_t val = typed->At(index);
    if (val <= static_cast<uint64_t>(ZEND_LONG_MAX)) {
        ZVAL_LONG(rv, static_cast<zend_long>(val));
    } else {
        /* Overflow: convert to string */
        char buf[32];
        int len = snprintf(buf, sizeof(buf), "%" PRIu64, val);
        ZVAL_STRINGL(rv, buf, static_cast<size_t>(len));
    }
}

template <typename T>
static inline void numeric_to_zval_double(const ColumnRef &col, size_t index, zval *rv) {
    auto typed = col->As<ColumnVector<T>>();
    ZVAL_DOUBLE(rv, static_cast<double>(typed->At(index)));
}

static void string_to_zval(const ColumnRef &col, size_t index, zval *rv) {
    auto typed = col->As<ColumnString>();
    std::string_view sv = typed->At(index);
    ZVAL_STRINGL(rv, sv.data(), sv.size());
}

static void fixed_string_to_zval(const ColumnRef &col, size_t index, zval *rv) {
    auto typed = col->As<ColumnFixedString>();
    std::string_view sv = typed->At(index);
    ZVAL_STRINGL(rv, sv.data(), sv.size());
}

static void date_to_zval(const ColumnRef &col, size_t index, zval *rv) {
    /* ColumnDate stores days since epoch as uint16_t; convert to 'Y-m-d' */
    auto typed = col->As<ColumnDate>();
    std::time_t t = typed->At(index);
    struct tm tm_buf;
    gmtime_r(&t, &tm_buf);
    char buf[16];
    int len = snprintf(buf, sizeof(buf), "%04d-%02d-%02d",
        tm_buf.tm_year + 1900, tm_buf.tm_mon + 1, tm_buf.tm_mday);
    ZVAL_STRINGL(rv, buf, static_cast<size_t>(len));
}

static void datetime_to_zval(const ColumnRef &col, size_t index, zval *rv) {
    /* DateTime is stored as uint32_t Unix timestamp */
    auto typed = col->As<ColumnDateTime>();
    ZVAL_LONG(rv, static_cast<zend_long>(typed->At(index)));
}

static void datetime64_to_zval(const ColumnRef &col, size_t index, zval *rv) {
    auto typed = col->As<ColumnDateTime64>();
    int64_t val = typed->At(index);
    auto precision = typed->Type()->As<DateTime64Type>()->GetPrecision();

    int64_t divisor = 1;
    for (size_t i = 0; i < precision; ++i) divisor *= 10;

    int64_t seconds = val / divisor;
    int64_t frac = val % divisor;
    if (frac < 0) { frac += divisor; seconds -= 1; }

    struct tm tm_buf;
    time_t t = static_cast<time_t>(seconds);
    gmtime_r(&t, &tm_buf);

    char buf[64];
    int len = snprintf(buf, sizeof(buf), "%04d-%02d-%02d %02d:%02d:%02d.%0*" PRId64,
        tm_buf.tm_year + 1900, tm_buf.tm_mon + 1, tm_buf.tm_mday,
        tm_buf.tm_hour, tm_buf.tm_min, tm_buf.tm_sec,
        static_cast<int>(precision), frac);
    ZVAL_STRINGL(rv, buf, static_cast<size_t>(len));
}

static void date32_to_zval(const ColumnRef &col, size_t index, zval *rv) {
    auto typed = col->As<ColumnDate32>();
    std::time_t t = typed->At(index);
    struct tm tm_buf;
    gmtime_r(&t, &tm_buf);
    char buf[16];
    int len = snprintf(buf, sizeof(buf), "%04d-%02d-%02d",
        tm_buf.tm_year + 1900, tm_buf.tm_mon + 1, tm_buf.tm_mday);
    ZVAL_STRINGL(rv, buf, static_cast<size_t>(len));
}

static void nullable_to_zval(const ColumnRef &col, size_t index, zval *rv) {
    auto typed = col->As<ColumnNullable>();
    if (typed->IsNull(index)) {
        ZVAL_NULL(rv);
    } else {
        php_clickhouse_column_to_zval(typed->Nested(), index, rv);
    }
}

static void array_to_zval(const ColumnRef &col, size_t index, zval *rv) {
    auto typed = col->As<ColumnArray>();
    auto slice = typed->GetAsColumn(index);
    size_t n = slice->Size();

    array_init_size(rv, n);
    for (size_t i = 0; i < n; ++i) {
        zval elem;
        php_clickhouse_column_to_zval(slice, i, &elem);
        add_next_index_zval(rv, &elem);
    }
}

static void tuple_to_zval(const ColumnRef &col, size_t index, zval *rv) {
    auto typed = col->As<ColumnTuple>();
    size_t n = typed->TupleSize();

    array_init_size(rv, n);
    for (size_t i = 0; i < n; ++i) {
        zval elem;
        php_clickhouse_column_to_zval((*typed)[i], index, &elem);
        add_next_index_zval(rv, &elem);
    }
}

static void map_to_zval(const ColumnRef &col, size_t index, zval *rv) {
    auto typed = col->As<ColumnMap>();

    /* Map::GetAsColumn(n) returns a Tuple column representing the key-value pairs at row n */
    auto slice = typed->GetAsColumn(index);
    auto tuple_col = slice->As<ColumnTuple>();

    size_t n = tuple_col ? tuple_col->Size() : 0;
    array_init_size(rv, n);
    if (tuple_col && tuple_col->TupleSize() >= 2) {
        auto k_col = (*tuple_col)[0];
        auto v_col = (*tuple_col)[1];
        for (size_t i = 0; i < tuple_col->Size(); ++i) {
            zval key, val;
            php_clickhouse_column_to_zval(k_col, i, &key);
            php_clickhouse_column_to_zval(v_col, i, &val);

            if (Z_TYPE(key) == IS_STRING) {
                add_assoc_zval_ex(rv, Z_STRVAL(key), Z_STRLEN(key), &val);
            } else if (Z_TYPE(key) == IS_LONG) {
                add_index_zval(rv, Z_LVAL(key), &val);
            } else {
                /* Convert key to string */
                zend_string *key_str = zval_get_string(&key);
                add_assoc_zval_ex(rv, ZSTR_VAL(key_str), ZSTR_LEN(key_str), &val);
                zend_string_release(key_str);
            }
            zval_ptr_dtor(&key);
        }
    }
}

static void enum8_to_zval(const ColumnRef &col, size_t index, zval *rv) {
    auto typed = col->As<ColumnEnum8>();
    auto name = typed->NameAt(index);
    ZVAL_STRINGL(rv, name.data(), name.size());
}

static void enum16_to_zval(const ColumnRef &col, size_t index, zval *rv) {
    auto typed = col->As<ColumnEnum16>();
    auto name = typed->NameAt(index);
    ZVAL_STRINGL(rv, name.data(), name.size());
}

static void uuid_to_zval(const ColumnRef &col, size_t index, zval *rv) {
    auto typed = col->As<ColumnUUID>();
    auto uuid = typed->At(index);

    /* UUID is stored as two uint64_t values. Format as standard UUID string. */
    char buf[37];
    /* clickhouse UUID is { first: high 64 bits, second: low 64 bits } */
    uint64_t hi = uuid.first;
    uint64_t lo = uuid.second;
    snprintf(buf, sizeof(buf),
        "%08x-%04x-%04x-%04x-%012" PRIx64,
        static_cast<uint32_t>(hi >> 32),
        static_cast<uint16_t>(hi >> 16),
        static_cast<uint16_t>(hi),
        static_cast<uint16_t>(lo >> 48),
        lo & 0x0000FFFFFFFFFFFF);
    ZVAL_STRINGL(rv, buf, 36);
}

static void ipv4_to_zval(const ColumnRef &col, size_t index, zval *rv) {
    auto typed = col->As<ColumnIPv4>();
    auto addr = typed->At(index);
    /* in_addr stores in network byte order */
    uint32_t ip = ntohl(addr.s_addr);
    char buf[16];
    int len = snprintf(buf, sizeof(buf), "%u.%u.%u.%u",
        (ip >> 24) & 0xFF, (ip >> 16) & 0xFF,
        (ip >> 8) & 0xFF, ip & 0xFF);
    ZVAL_STRINGL(rv, buf, static_cast<size_t>(len));
}

static void ipv6_to_zval(const ColumnRef &col, size_t index, zval *rv) {
    auto typed = col->As<ColumnIPv6>();
    auto addr = typed->At(index);
    char buf[INET6_ADDRSTRLEN];
    inet_ntop(AF_INET6, &addr, buf, sizeof(buf));
    ZVAL_STRING(rv, buf);
}

static void decimal_to_zval(const ColumnRef &col, size_t index, zval *rv) {
    /* Return as string to preserve precision.
     * ColumnDecimal::At() returns the raw Int128 scaled integer.
     * We format it as "integer_part.fractional_part" using the column's scale. */
    auto typed = col->As<ColumnDecimal>();
    if (!typed) { ZVAL_NULL(rv); return; }

    Int128 raw = typed->At(index);
    size_t scale = typed->GetScale();

    if (scale == 0) {
        std::ostringstream oss;
        oss << raw;
        std::string str = oss.str();
        ZVAL_STRINGL(rv, str.c_str(), str.size());
        return;
    }

    Int128 divisor = 1;
    for (size_t i = 0; i < scale; ++i) divisor *= 10;

    bool negative = (raw < 0);
    if (negative) raw = -raw;

    Int128 integer_part = raw / divisor;
    Int128 frac_part = raw % divisor;

    std::ostringstream oss;
    if (negative) oss << '-';
    oss << integer_part << '.';

    /* Pad fractional part with leading zeros to match scale */
    std::ostringstream foss;
    foss << frac_part;
    std::string frac_str = foss.str();
    for (size_t i = frac_str.size(); i < scale; ++i) oss << '0';
    oss << frac_str;

    std::string str = oss.str();
    ZVAL_STRINGL(rv, str.c_str(), str.size());
}

static void int128_to_zval(const ColumnRef &col, size_t index, zval *rv) {
    auto typed = col->As<ColumnVector<absl::int128>>();
    auto val = typed->At(index);
    /* absl::int128 supports operator<< for proper decimal string output */
    std::ostringstream oss;
    oss << val;
    std::string str = oss.str();
    ZVAL_STRINGL(rv, str.c_str(), str.size());
}

static void uint128_to_zval(const ColumnRef &col, size_t index, zval *rv) {
    auto typed = col->As<ColumnVector<absl::uint128>>();
    auto val = typed->At(index);
    std::ostringstream oss;
    oss << val;
    std::string str = oss.str();
    ZVAL_STRINGL(rv, str.c_str(), str.size());
}

static void lowcardinality_to_zval(const ColumnRef &col, size_t index, zval *rv) {
    /* LowCardinality is transparent — use ItemView for string types,
     * or fall back to string-based conversion for numeric types. */
    auto lc_type = col->Type()->As<LowCardinalityType>();
    auto nested_code = lc_type->GetNestedType()->GetCode();

    switch (nested_code) {
        case Type::String:
        case Type::FixedString: {
            auto item = col->GetItem(index);
            auto sv = item.get<std::string_view>();
            ZVAL_STRINGL(rv, sv.data(), sv.size());
            break;
        }
        case Type::Int8:
        case Type::Int16:
        case Type::Int32:
        case Type::Int64:
        case Type::UInt8:
        case Type::UInt16:
        case Type::UInt32:
        case Type::UInt64:
        case Type::Float32:
        case Type::Float64: {
            /* For numeric LC types, ItemView returns raw bytes.
             * Extract from the underlying dictionary via GetItem's numeric view. */
            auto item = col->GetItem(index);
            auto sv = item.get<std::string_view>();
            /* Interpret raw bytes based on the nested type */
            if (nested_code == Type::Float64 && sv.size() == sizeof(double)) {
                double d;
                std::memcpy(&d, sv.data(), sizeof(double));
                ZVAL_DOUBLE(rv, d);
            } else if (nested_code == Type::Float32 && sv.size() == sizeof(float)) {
                float f;
                std::memcpy(&f, sv.data(), sizeof(float));
                ZVAL_DOUBLE(rv, static_cast<double>(f));
            } else {
                /* Integer types: read raw bytes as little-endian integer */
                int64_t val = 0;
                std::memcpy(&val, sv.data(), std::min(sv.size(), sizeof(int64_t)));
                ZVAL_LONG(rv, static_cast<zend_long>(val));
            }
            break;
        }
        case Type::Date:
        case Type::DateTime:
        default: {
            /* For other nested types, try ItemView string representation */
            try {
                auto item = col->GetItem(index);
                auto sv = item.get<std::string_view>();
                ZVAL_STRINGL(rv, sv.data(), sv.size());
            } catch (...) {
                ZVAL_NULL(rv);
            }
            break;
        }
    }
}

static void point_to_zval(const ColumnRef &col, size_t index, zval *rv) {
    auto typed = col->As<ColumnPoint>();
    auto pt = typed->At(index); /* std::tuple<double, double> */
    array_init_size(rv, 2);
    add_next_index_double(rv, std::get<0>(pt));
    add_next_index_double(rv, std::get<1>(pt));
}

/* Ring = Array(Point), Polygon = Array(Ring), MultiPolygon = Array(Polygon).
 * ColumnGeo<T> inherits from Column (NOT from T), so As<ColumnArray>() fails.
 * Use the typed At() which returns ArrayValueView with size()/iterators. */

static void ring_to_zval(const ColumnRef &col, size_t index, zval *rv) {
    auto typed = col->As<ColumnRing>();
    if (!typed) { ZVAL_NULL(rv); return; }

    auto view = typed->At(index);
    array_init_size(rv, view.size());
    for (size_t i = 0; i < view.size(); ++i) {
        auto pt = view[i]; /* std::tuple<double, double> */
        zval elem;
        array_init_size(&elem, 2);
        add_next_index_double(&elem, std::get<0>(pt));
        add_next_index_double(&elem, std::get<1>(pt));
        add_next_index_zval(rv, &elem);
    }
}

static void polygon_to_zval(const ColumnRef &col, size_t index, zval *rv) {
    auto typed = col->As<ColumnPolygon>();
    if (!typed) { ZVAL_NULL(rv); return; }

    auto view = typed->At(index); /* ArrayValueView of rings */
    array_init_size(rv, view.size());
    for (size_t i = 0; i < view.size(); ++i) {
        auto ring_view = view[i]; /* ArrayValueView of points */
        zval ring_arr;
        array_init_size(&ring_arr, ring_view.size());
        for (size_t j = 0; j < ring_view.size(); ++j) {
            auto pt = ring_view[j];
            zval elem;
            array_init_size(&elem, 2);
            add_next_index_double(&elem, std::get<0>(pt));
            add_next_index_double(&elem, std::get<1>(pt));
            add_next_index_zval(&ring_arr, &elem);
        }
        add_next_index_zval(rv, &ring_arr);
    }
}

static void multipolygon_to_zval(const ColumnRef &col, size_t index, zval *rv) {
    auto typed = col->As<ColumnMultiPolygon>();
    if (!typed) { ZVAL_NULL(rv); return; }

    auto view = typed->At(index); /* ArrayValueView of polygons */
    array_init_size(rv, view.size());
    for (size_t i = 0; i < view.size(); ++i) {
        auto poly_view = view[i]; /* ArrayValueView of rings */
        zval poly_arr;
        array_init_size(&poly_arr, poly_view.size());
        for (size_t j = 0; j < poly_view.size(); ++j) {
            auto ring_view = poly_view[j]; /* ArrayValueView of points */
            zval ring_arr;
            array_init_size(&ring_arr, ring_view.size());
            for (size_t k = 0; k < ring_view.size(); ++k) {
                auto pt = ring_view[k];
                zval elem;
                array_init_size(&elem, 2);
                add_next_index_double(&elem, std::get<0>(pt));
                add_next_index_double(&elem, std::get<1>(pt));
                add_next_index_zval(&ring_arr, &elem);
            }
            add_next_index_zval(&poly_arr, &ring_arr);
        }
        add_next_index_zval(rv, &poly_arr);
    }
}

void php_clickhouse_column_to_zval(const ColumnRef &col, size_t index, zval *return_value)
{
    switch (col->Type()->GetCode()) {
        case Type::Int8:    numeric_to_zval_long<int8_t>(col, index, return_value); break;
        case Type::Int16:   numeric_to_zval_long<int16_t>(col, index, return_value); break;
        case Type::Int32:   numeric_to_zval_long<int32_t>(col, index, return_value); break;
        case Type::Int64:   numeric_to_zval_long<int64_t>(col, index, return_value); break;
        case Type::UInt8:   numeric_to_zval_long<uint8_t>(col, index, return_value); break;
        case Type::UInt16:  numeric_to_zval_long<uint16_t>(col, index, return_value); break;
        case Type::UInt32:  numeric_to_zval_long<uint32_t>(col, index, return_value); break;
        case Type::UInt64:  uint64_to_zval(col, index, return_value); break;
        case Type::Float32: numeric_to_zval_double<float>(col, index, return_value); break;
        case Type::Float64: numeric_to_zval_double<double>(col, index, return_value); break;

        case Type::String:      string_to_zval(col, index, return_value); break;
        case Type::FixedString: fixed_string_to_zval(col, index, return_value); break;

        case Type::Date:        date_to_zval(col, index, return_value); break;
        case Type::Date32:      date32_to_zval(col, index, return_value); break;
        case Type::DateTime:    datetime_to_zval(col, index, return_value); break;
        case Type::DateTime64:  datetime64_to_zval(col, index, return_value); break;

        case Type::Nullable:    nullable_to_zval(col, index, return_value); break;
        case Type::Array:       array_to_zval(col, index, return_value); break;
        case Type::Tuple:       tuple_to_zval(col, index, return_value); break;
        case Type::Map:         map_to_zval(col, index, return_value); break;

        case Type::Enum8:  enum8_to_zval(col, index, return_value); break;
        case Type::Enum16: enum16_to_zval(col, index, return_value); break;

        case Type::UUID: uuid_to_zval(col, index, return_value); break;
        case Type::IPv4: ipv4_to_zval(col, index, return_value); break;
        case Type::IPv6: ipv6_to_zval(col, index, return_value); break;

        case Type::Decimal:
        case Type::Decimal32:
        case Type::Decimal64:
        case Type::Decimal128:
            decimal_to_zval(col, index, return_value); break;

        case Type::Int128:  int128_to_zval(col, index, return_value); break;
        case Type::UInt128: uint128_to_zval(col, index, return_value); break;

        case Type::LowCardinality: lowcardinality_to_zval(col, index, return_value); break;

        case Type::Point:        point_to_zval(col, index, return_value); break;
        case Type::Ring:         ring_to_zval(col, index, return_value); break;
        case Type::Polygon:      polygon_to_zval(col, index, return_value); break;
        case Type::MultiPolygon: multipolygon_to_zval(col, index, return_value); break;

        default:
            /* Unknown type — return string representation via ItemView */
            try {
                auto item = col->GetItem(index);
                auto sv = item.get<std::string_view>();
                ZVAL_STRINGL(return_value, sv.data(), sv.size());
            } catch (...) {
                ZVAL_NULL(return_value);
            }
            break;
    }
}
