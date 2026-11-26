#include "src/column_convert.h"
#include "src/common.h"

#include "clickhouse/columns/array.h"
#include "clickhouse/columns/bool.h"
#include "clickhouse/columns/date.h"
#include "clickhouse/columns/decimal.h"
#include "clickhouse/columns/enum.h"
#include "clickhouse/columns/geo.h"
#include "clickhouse/columns/ip4.h"
#include "clickhouse/columns/ip6.h"
#include "clickhouse/columns/json.h"
#include "clickhouse/columns/lowcardinality.h"
#include "clickhouse/columns/map.h"
#include "clickhouse/columns/nothing.h"
#include "clickhouse/columns/nullable.h"
#include "clickhouse/columns/numeric.h"
#include "clickhouse/columns/string.h"
#include "clickhouse/columns/time.h"
#include "clickhouse/columns/tuple.h"
#include "clickhouse/columns/uuid.h"
#include "clickhouse/types/types.h"

#include "absl/numeric/int128.h"

#include <cstdio>
#include <cstring>
#include <cinttypes>
#include <limits>
#include <type_traits>
#include <string>
#include <tuple>
#include <arpa/inet.h> /* ntohl, inet_ntop */

using namespace clickhouse;

void php_clickhouse_column_to_zval(const ColumnRef &col, size_t index, zval *return_value);
static void datetime64_value_to_zval(int64_t val, size_t precision, zval *rv);
static void uuid_parts_to_zval(uint64_t hi, uint64_t lo, zval *rv);
static void decimal_value_to_zval(Int128 raw, size_t scale, zval *rv);

/* The dispatch in php_clickhouse_column_to_zval has already matched the type code, so the hot
 * paths downcast directly. As<T>() costs a dynamic cast and two atomic refcounts per cell. */
template <typename T> static inline const T *typed_column(const ColumnRef &col)
{
    return static_cast<const T *>(col.get());
}

template <typename T>
static inline void numeric_to_zval_long(const ColumnRef &col, size_t index, zval *rv)
{
    ZVAL_LONG(rv, static_cast<zend_long>(typed_column<ColumnVector<T>>(col)->At(index)));
}

static void uint64_to_zval(const ColumnRef &col, size_t index, zval *rv)
{
    uint64_t val = typed_column<ColumnVector<uint64_t>>(col)->At(index);
    if (val <= static_cast<uint64_t>(ZEND_LONG_MAX)) {
        ZVAL_LONG(rv, static_cast<zend_long>(val));
    } else {
        /* Overflow: convert to string */
        char buf[32];
        int len = snprintf(buf, sizeof(buf), "%" PRIu64, val);
        ZVAL_STRINGL(rv, buf, static_cast<size_t>(len));
    }
}

static void uint64_value_to_zval(uint64_t val, zval *rv)
{
    if (val <= static_cast<uint64_t>(ZEND_LONG_MAX)) {
        ZVAL_LONG(rv, static_cast<zend_long>(val));
    } else {
        /* Overflow: convert to string */
        char buf[32];
        int len = snprintf(buf, sizeof(buf), "%" PRIu64, val);
        ZVAL_STRINGL(rv, buf, static_cast<size_t>(len));
    }
}

static void time64_value_to_zval(int64_t val, zval *rv)
{
    if (val >= static_cast<int64_t>(ZEND_LONG_MIN) && val <= static_cast<int64_t>(ZEND_LONG_MAX)) {
        ZVAL_LONG(rv, static_cast<zend_long>(val));
    } else {
        char buf[32];
        int len = snprintf(buf, sizeof(buf), "%" PRId64, val);
        ZVAL_STRINGL(rv, buf, static_cast<size_t>(len));
    }
}

template <typename T>
static inline void numeric_to_zval_double(const ColumnRef &col, size_t index, zval *rv)
{
    ZVAL_DOUBLE(rv, static_cast<double>(typed_column<ColumnVector<T>>(col)->At(index)));
}

static void bool_to_zval(const ColumnRef &col, size_t index, zval *rv)
{
    ZVAL_BOOL(rv, typed_column<ColumnBool>(col)->At(index));
}

static void string_to_zval(const ColumnRef &col, size_t index, zval *rv)
{
    std::string_view sv = typed_column<ColumnString>(col)->At(index);
    ZVAL_STRINGL(rv, sv.data(), sv.size());
}

static void json_to_zval(const ColumnRef &col, size_t index, zval *rv)
{
    auto typed = col->As<ColumnJSON>();
    std::string_view sv = typed->At(index);
    ZVAL_STRINGL(rv, sv.data(), sv.size());
}

static void fixed_string_to_zval(const ColumnRef &col, size_t index, zval *rv)
{
    std::string_view sv = typed_column<ColumnFixedString>(col)->At(index);
    ZVAL_STRINGL(rv, sv.data(), sv.size());
}

static void date_to_zval(const ColumnRef &col, size_t index, zval *rv)
{
    /* ColumnDate stores days since epoch as uint16_t; convert to 'Y-m-d' */
    auto typed = col->As<ColumnDate>();
    std::time_t t = typed->At(index);
    struct tm tm_buf;
    gmtime_r(&t, &tm_buf);
    char buf[16];
    int len = snprintf(buf, sizeof(buf), "%04d-%02d-%02d", tm_buf.tm_year + 1900, tm_buf.tm_mon + 1,
                       tm_buf.tm_mday);
    ZVAL_STRINGL(rv, buf, static_cast<size_t>(len));
}

static void datetime_to_zval(const ColumnRef &col, size_t index, zval *rv)
{
    /* DateTime is stored as uint32_t Unix timestamp */
    auto typed = col->As<ColumnDateTime>();
    ZVAL_LONG(rv, static_cast<zend_long>(typed->At(index)));
}

static void datetime64_to_zval(const ColumnRef &col, size_t index, zval *rv)
{
    auto typed = col->As<ColumnDateTime64>();
    auto precision = typed->Type()->As<DateTime64Type>()->GetPrecision();
    int64_t val = typed->At(index);
    datetime64_value_to_zval(val, precision, rv);
}

static void time_to_zval(const ColumnRef &col, size_t index, zval *rv)
{
    auto typed = col->As<ColumnTime>();
    ZVAL_LONG(rv, static_cast<zend_long>(typed->At(index)));
}

static void time64_to_zval(const ColumnRef &col, size_t index, zval *rv)
{
    auto typed = col->As<ColumnTime64>();
    time64_value_to_zval(typed->At(index), rv);
}

static void datetime64_value_to_zval(int64_t val, size_t precision, zval *rv)
{

    int64_t divisor = 1;
    for (size_t i = 0; i < precision; ++i)
        divisor *= 10;

    int64_t seconds = val / divisor;
    int64_t frac = val % divisor;
    if (frac < 0) {
        frac += divisor;
        seconds -= 1;
    }

    struct tm tm_buf;
    time_t t = static_cast<time_t>(seconds);
    gmtime_r(&t, &tm_buf);

    char buf[64];
    int len = snprintf(buf, sizeof(buf), "%04d-%02d-%02d %02d:%02d:%02d.%0*" PRId64,
                       tm_buf.tm_year + 1900, tm_buf.tm_mon + 1, tm_buf.tm_mday, tm_buf.tm_hour,
                       tm_buf.tm_min, tm_buf.tm_sec, static_cast<int>(precision), frac);
    ZVAL_STRINGL(rv, buf, static_cast<size_t>(len));
}

static void date32_to_zval(const ColumnRef &col, size_t index, zval *rv)
{
    auto typed = col->As<ColumnDate32>();
    std::time_t t = typed->At(index);
    struct tm tm_buf;
    gmtime_r(&t, &tm_buf);
    char buf[16];
    int len = snprintf(buf, sizeof(buf), "%04d-%02d-%02d", tm_buf.tm_year + 1900, tm_buf.tm_mon + 1,
                       tm_buf.tm_mday);
    ZVAL_STRINGL(rv, buf, static_cast<size_t>(len));
}

static void nullable_to_zval(const ColumnRef &col, size_t index, zval *rv)
{
    auto typed = col->As<ColumnNullable>();
    if (typed->IsNull(index)) {
        ZVAL_NULL(rv);
    } else {
        php_clickhouse_column_to_zval(typed->Nested(), index, rv);
    }
}

template <typename T>
static void fill_numeric_array(const ColumnRef &col, size_t offset, size_t count, zval *rv)
{
    const auto &data = static_cast<ColumnVector<T> *>(col.get())->GetWritableData();
    HashTable *result = Z_ARRVAL_P(rv);
    zend_hash_real_init_packed(result);
    ZEND_HASH_FILL_PACKED(result)
    {
        for (size_t i = offset; i < offset + count; ++i) {
            if constexpr (std::is_same_v<T, uint64_t>) {
                zval value;
                uint64_value_to_zval(data[i], &value);
                ZEND_HASH_FILL_SET(&value);
            } else if constexpr (std::is_floating_point_v<T>) {
                ZEND_HASH_FILL_SET_DOUBLE(data[i]);
            } else {
                ZEND_HASH_FILL_SET_LONG(static_cast<zend_long>(data[i]));
            }
            ZEND_HASH_FILL_NEXT();
        }
    }
    ZEND_HASH_FILL_END();
}

static void column_range_to_array(const ColumnRef &col, size_t offset, size_t count, zval *rv)
{
    if (UNEXPECTED(count > HT_MAX_SIZE)) {
        zend_throw_exception(clickhouse_ce_ValidationException, "Column exceeds PHP array size", 0);
        ZVAL_NULL(rv);
        return;
    }
    array_init_size(rv, static_cast<uint32_t>(count));
    if (!count) {
        return;
    }

    switch (col->GetType().GetCode()) {
    case Type::Int8:
        return fill_numeric_array<int8_t>(col, offset, count, rv);
    case Type::Int16:
        return fill_numeric_array<int16_t>(col, offset, count, rv);
    case Type::Int32:
        return fill_numeric_array<int32_t>(col, offset, count, rv);
    case Type::Int64:
        return fill_numeric_array<int64_t>(col, offset, count, rv);
    case Type::UInt8:
        return fill_numeric_array<uint8_t>(col, offset, count, rv);
    case Type::UInt16:
        return fill_numeric_array<uint16_t>(col, offset, count, rv);
    case Type::UInt32:
        return fill_numeric_array<uint32_t>(col, offset, count, rv);
    case Type::UInt64:
        return fill_numeric_array<uint64_t>(col, offset, count, rv);
    case Type::Float32:
        return fill_numeric_array<float>(col, offset, count, rv);
    case Type::Float64:
        return fill_numeric_array<double>(col, offset, count, rv);
    default:
        for (size_t i = offset; i < offset + count; ++i) {
            zval value;
            php_clickhouse_column_to_zval(col, i, &value);
            add_next_index_zval(rv, &value);
            if (UNEXPECTED(EG(exception))) {
                return;
            }
        }
    }
}

void php_clickhouse_column_to_array(const ColumnRef &col, zval *rv)
{
    column_range_to_array(col, 0, col->Size(), rv);
}

static void array_to_zval(const ColumnRef &col, size_t index, zval *rv)
{
    auto *array = static_cast<ColumnArray *>(col.get());
    const auto &offsets = array->GetOffsets()->GetWritableData();
    const auto data = array->GetData();
    const uint64_t start = index == 0 ? 0 : offsets.at(index - 1);
    const uint64_t end = offsets.at(index);
    if (UNEXPECTED(end < start || end > data->Size())) {
        zend_throw_exception(clickhouse_ce_ValidationException, "Invalid ClickHouse array offsets",
                             0);
        ZVAL_NULL(rv);
        return;
    }

    column_range_to_array(data, static_cast<size_t>(start), static_cast<size_t>(end - start), rv);
}

static void tuple_to_zval(const ColumnRef &col, size_t index, zval *rv)
{
    auto typed = col->As<ColumnTuple>();
    size_t n = typed->TupleSize();

    array_init_size(rv, n);
    for (size_t i = 0; i < n; ++i) {
        zval elem;
        php_clickhouse_column_to_zval((*typed)[i], index, &elem);
        add_next_index_zval(rv, &elem);
    }
}

static void map_to_zval(const ColumnRef &col, size_t index, zval *rv)
{
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

static void enum8_to_zval(const ColumnRef &col, size_t index, zval *rv)
{
    auto typed = col->As<ColumnEnum8>();
    auto name = typed->NameAt(index);
    ZVAL_STRINGL(rv, name.data(), name.size());
}

static void enum16_to_zval(const ColumnRef &col, size_t index, zval *rv)
{
    auto typed = col->As<ColumnEnum16>();
    auto name = typed->NameAt(index);
    ZVAL_STRINGL(rv, name.data(), name.size());
}

static void uuid_to_zval(const ColumnRef &col, size_t index, zval *rv)
{
    auto typed = col->As<ColumnUUID>();
    auto uuid = typed->At(index);
    uuid_parts_to_zval(uuid.first, uuid.second, rv);
}

static void uuid_parts_to_zval(uint64_t hi, uint64_t lo, zval *rv)
{

    /* UUID is stored as two uint64_t values. Format as standard UUID string. */
    char buf[37];
    snprintf(buf, sizeof(buf), "%08x-%04x-%04x-%04x-%012" PRIx64, static_cast<uint32_t>(hi >> 32),
             static_cast<uint16_t>(hi >> 16), static_cast<uint16_t>(hi),
             static_cast<uint16_t>(lo >> 48), lo & 0x0000FFFFFFFFFFFF);
    ZVAL_STRINGL(rv, buf, 36);
}

static void ipv4_to_zval(const ColumnRef &col, size_t index, zval *rv)
{
    auto typed = col->As<ColumnIPv4>();
    auto addr = typed->At(index);
    /* in_addr stores in network byte order */
    uint32_t ip = ntohl(addr.s_addr);
    char buf[16];
    int len = snprintf(buf, sizeof(buf), "%u.%u.%u.%u", (ip >> 24) & 0xFF, (ip >> 16) & 0xFF,
                       (ip >> 8) & 0xFF, ip & 0xFF);
    ZVAL_STRINGL(rv, buf, static_cast<size_t>(len));
}

static void ipv6_to_zval(const ColumnRef &col, size_t index, zval *rv)
{
    auto typed = col->As<ColumnIPv6>();
    auto addr = typed->At(index);
    char buf[INET6_ADDRSTRLEN];
    inet_ntop(AF_INET6, &addr, buf, sizeof(buf));
    ZVAL_STRING(rv, buf);
}

static void decimal_to_zval(const ColumnRef &col, size_t index, zval *rv)
{
    const auto *decimal = typed_column<ColumnDecimal>(col);
    const auto &type = static_cast<const DecimalType &>(col->GetType());
    decimal_value_to_zval(decimal->At(index), type.GetScale(), rv);
}

static void decimal_value_to_zval(Int128 raw, size_t scale, zval *rv)
{
    if (scale <= 18 && raw >= INT64_MIN && raw <= INT64_MAX) {
        const int64_t value = static_cast<int64_t>(raw);
        uint64_t magnitude = static_cast<uint64_t>(value);
        if (value < 0) {
            magnitude = 0 - magnitude;
        }

        // 19 digits, a leading zero for a fraction, a decimal point and a sign.
        char buffer[22];
        const char *end = buffer + sizeof(buffer);
        char *pos = buffer + sizeof(buffer);
        if (scale) {
            for (size_t i = 0; i < scale; ++i) {
                *--pos = static_cast<char>('0' + magnitude % 10);
                magnitude /= 10;
            }
            *--pos = '.';
        }
        do {
            *--pos = static_cast<char>('0' + magnitude % 10);
            magnitude /= 10;
        } while (magnitude);
        if (value < 0) {
            *--pos = '-';
        }
        ZVAL_STRINGL(rv, pos, end - pos);
        return;
    }

    std::string digits = Bignum::Int128ToString(raw);
    if (scale != 0) {
        const size_t sign = digits.front() == '-' ? 1 : 0;
        const size_t digit_count = digits.size() - sign;
        if (digit_count <= scale) {
            digits.insert(sign, scale - digit_count + 1, '0');
        }
        digits.insert(digits.size() - scale, 1, '.');
    }
    ZVAL_STRINGL(rv, digits.data(), digits.size());
}

static Int128 decimal_item_to_int128(const ItemView &item)
{
    switch (item.AsBinaryData().size()) {
    case sizeof(int32_t):
        return static_cast<Int128>(item.get<int32_t>());
    case sizeof(int64_t):
        return static_cast<Int128>(item.get<int64_t>());
    case sizeof(Int128):
        return item.get<Int128>();
    default:
        throw AssertionError("Invalid decimal ItemView size");
    }
}

static void int128_to_zval(const ColumnRef &col, size_t index, zval *rv)
{
    const auto digits = Bignum::Int128ToString(typed_column<ColumnInt128>(col)->At(index));
    ZVAL_STRINGL(rv, digits.data(), digits.size());
}

static void uint128_to_zval(const ColumnRef &col, size_t index, zval *rv)
{
    const auto digits = Bignum::UInt128ToString(typed_column<ColumnUInt128>(col)->At(index));
    ZVAL_STRINGL(rv, digits.data(), digits.size());
}

static void lowcardinality_to_zval(const ColumnRef &col, size_t index, zval *rv)
{
    /* LowCardinality is transparent for userland values. Nullable LC values
     * expose nulls as ItemView::Void, so handle that before looking at the
     * nested physical type. */
    auto item = col->GetItem(index);
    if (item.type == Type::Void) {
        ZVAL_NULL(rv);
        return;
    }

    auto lc_type = col->Type()->As<LowCardinalityType>();
    auto nested_type = lc_type->GetNestedType();
    auto value_type = nested_type;
    auto nested_code = value_type->GetCode();
    if (nested_code == Type::Nullable) {
        value_type = nested_type->As<NullableType>()->GetNestedType();
        nested_code = value_type->GetCode();
    }

    switch (nested_code) {
    case Type::String:
    case Type::FixedString: {
        auto sv = item.get<std::string_view>();
        ZVAL_STRINGL(rv, sv.data(), sv.size());
        break;
    }
    case Type::Int8:
        ZVAL_LONG(rv, static_cast<zend_long>(item.get<int8_t>()));
        break;
    case Type::Int16:
        ZVAL_LONG(rv, static_cast<zend_long>(item.get<int16_t>()));
        break;
    case Type::Int32:
        ZVAL_LONG(rv, static_cast<zend_long>(item.get<int32_t>()));
        break;
    case Type::Int64:
        ZVAL_LONG(rv, static_cast<zend_long>(item.get<int64_t>()));
        break;
    case Type::UInt8:
        ZVAL_LONG(rv, static_cast<zend_long>(item.get<uint8_t>()));
        break;
    case Type::UInt16:
        ZVAL_LONG(rv, static_cast<zend_long>(item.get<uint16_t>()));
        break;
    case Type::UInt32:
        ZVAL_LONG(rv, static_cast<zend_long>(item.get<uint32_t>()));
        break;
    case Type::UInt64:
        uint64_value_to_zval(item.get<uint64_t>(), rv);
        break;
    case Type::Float32:
        ZVAL_DOUBLE(rv, static_cast<double>(item.get<float>()));
        break;
    case Type::Float64:
        ZVAL_DOUBLE(rv, item.get<double>());
        break;
    case Type::Bool:
        ZVAL_BOOL(rv, item.get<uint8_t>() != 0);
        break;

    case Type::Date: {
        std::time_t t = static_cast<std::time_t>(item.get<uint16_t>()) * 86400;
        struct tm tm_buf;
        gmtime_r(&t, &tm_buf);
        char buf[16];
        int len = snprintf(buf, sizeof(buf), "%04d-%02d-%02d", tm_buf.tm_year + 1900,
                           tm_buf.tm_mon + 1, tm_buf.tm_mday);
        ZVAL_STRINGL(rv, buf, static_cast<size_t>(len));
        break;
    }
    case Type::Date32: {
        std::time_t t = static_cast<std::time_t>(item.get<int32_t>()) * 86400;
        struct tm tm_buf;
        gmtime_r(&t, &tm_buf);
        char buf[16];
        int len = snprintf(buf, sizeof(buf), "%04d-%02d-%02d", tm_buf.tm_year + 1900,
                           tm_buf.tm_mon + 1, tm_buf.tm_mday);
        ZVAL_STRINGL(rv, buf, static_cast<size_t>(len));
        break;
    }
    case Type::DateTime:
        ZVAL_LONG(rv, static_cast<zend_long>(item.get<uint32_t>()));
        break;
    case Type::DateTime64:
        datetime64_value_to_zval(item.get<int64_t>(),
                                 value_type->As<DateTime64Type>()->GetPrecision(), rv);
        break;
    case Type::Time:
        ZVAL_LONG(rv, static_cast<zend_long>(item.get<int32_t>()));
        break;
    case Type::Time64:
        time64_value_to_zval(item.get<int64_t>(), rv);
        break;
    case Type::JSON: {
        auto sv = item.get<std::string_view>();
        ZVAL_STRINGL(rv, sv.data(), sv.size());
        break;
    }

    case Type::Enum8: {
        auto name = value_type->As<EnumType>()->GetEnumName(item.get<int8_t>());
        ZVAL_STRINGL(rv, name.data(), name.size());
        break;
    }
    case Type::Enum16: {
        auto name = value_type->As<EnumType>()->GetEnumName(item.get<int16_t>());
        ZVAL_STRINGL(rv, name.data(), name.size());
        break;
    }

    case Type::UUID: {
        auto data = item.AsBinaryData();
        if (data.size() != sizeof(uint64_t) * 2) {
            ZVAL_NULL(rv);
            break;
        }
        uint64_t hi = 0;
        uint64_t lo = 0;
        memcpy(&hi, data.data(), sizeof(hi));
        memcpy(&lo, data.data() + sizeof(hi), sizeof(lo));
        uuid_parts_to_zval(hi, lo, rv);
        break;
    }
    case Type::IPv4: {
        uint32_t ip = item.get<uint32_t>();
        char buf[16];
        int len = snprintf(buf, sizeof(buf), "%u.%u.%u.%u", (ip >> 24) & 0xFF, (ip >> 16) & 0xFF,
                           (ip >> 8) & 0xFF, ip & 0xFF);
        ZVAL_STRINGL(rv, buf, static_cast<size_t>(len));
        break;
    }
    case Type::IPv6: {
        auto data = item.AsBinaryData();
        char buf[INET6_ADDRSTRLEN];
        if (data.size() == 16 && inet_ntop(AF_INET6, data.data(), buf, sizeof(buf))) {
            ZVAL_STRING(rv, buf);
        } else {
            ZVAL_NULL(rv);
        }
        break;
    }

    case Type::Decimal:
    case Type::Decimal32:
    case Type::Decimal64:
    case Type::Decimal128:
        decimal_value_to_zval(decimal_item_to_int128(item),
                              value_type->As<DecimalType>()->GetScale(), rv);
        break;

    case Type::Int128: {
        const std::string str = Bignum::Int128ToString(item.get<Int128>());
        ZVAL_STRINGL(rv, str.c_str(), str.size());
        break;
    }
    case Type::UInt128: {
        const std::string str = Bignum::UInt128ToString(item.get<UInt128>());
        ZVAL_STRINGL(rv, str.c_str(), str.size());
        break;
    }

    default: {
        /* For other nested types, try ItemView string representation */
        try {
            auto sv = item.get<std::string_view>();
            ZVAL_STRINGL(rv, sv.data(), sv.size());
        } catch (...) {
            ZVAL_NULL(rv);
        }
        break;
    }
    }
}

static void point_to_zval(const ColumnRef &col, size_t index, zval *rv)
{
    auto typed = col->As<ColumnPoint>();
    auto pt = typed->At(index); /* std::tuple<double, double> */
    array_init_size(rv, 2);
    add_next_index_double(rv, std::get<0>(pt));
    add_next_index_double(rv, std::get<1>(pt));
}

/* Ring = Array(Point), Polygon = Array(Ring), MultiPolygon = Array(Polygon).
 * ColumnGeo<T> inherits from Column (NOT from T), so As<ColumnArray>() fails.
 * Use the typed At() which returns ArrayValueView with size()/iterators. */

static void ring_to_zval(const ColumnRef &col, size_t index, zval *rv)
{
    auto typed = col->As<ColumnRing>();
    if (!typed) {
        ZVAL_NULL(rv);
        return;
    }

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

static void polygon_to_zval(const ColumnRef &col, size_t index, zval *rv)
{
    auto typed = col->As<ColumnPolygon>();
    if (!typed) {
        ZVAL_NULL(rv);
        return;
    }

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

static void multipolygon_to_zval(const ColumnRef &col, size_t index, zval *rv)
{
    auto typed = col->As<ColumnMultiPolygon>();
    if (!typed) {
        ZVAL_NULL(rv);
        return;
    }

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
    switch (col->GetType().GetCode()) {
    case Type::Int8:
        numeric_to_zval_long<int8_t>(col, index, return_value);
        break;
    case Type::Int16:
        numeric_to_zval_long<int16_t>(col, index, return_value);
        break;
    case Type::Int32:
        numeric_to_zval_long<int32_t>(col, index, return_value);
        break;
    case Type::Int64:
        numeric_to_zval_long<int64_t>(col, index, return_value);
        break;
    case Type::UInt8:
        numeric_to_zval_long<uint8_t>(col, index, return_value);
        break;
    case Type::UInt16:
        numeric_to_zval_long<uint16_t>(col, index, return_value);
        break;
    case Type::UInt32:
        numeric_to_zval_long<uint32_t>(col, index, return_value);
        break;
    case Type::UInt64:
        uint64_to_zval(col, index, return_value);
        break;
    case Type::Float32:
        numeric_to_zval_double<float>(col, index, return_value);
        break;
    case Type::Float64:
        numeric_to_zval_double<double>(col, index, return_value);
        break;
    case Type::Bool:
        bool_to_zval(col, index, return_value);
        break;

    case Type::String:
        string_to_zval(col, index, return_value);
        break;
    case Type::FixedString:
        fixed_string_to_zval(col, index, return_value);
        break;
    case Type::JSON:
        json_to_zval(col, index, return_value);
        break;

    case Type::Date:
        date_to_zval(col, index, return_value);
        break;
    case Type::Date32:
        date32_to_zval(col, index, return_value);
        break;
    case Type::DateTime:
        datetime_to_zval(col, index, return_value);
        break;
    case Type::DateTime64:
        datetime64_to_zval(col, index, return_value);
        break;
    case Type::Time:
        time_to_zval(col, index, return_value);
        break;
    case Type::Time64:
        time64_to_zval(col, index, return_value);
        break;

    case Type::Nullable:
        nullable_to_zval(col, index, return_value);
        break;
    case Type::Array:
        array_to_zval(col, index, return_value);
        break;
    case Type::Tuple:
        tuple_to_zval(col, index, return_value);
        break;
    case Type::Map:
        map_to_zval(col, index, return_value);
        break;

    case Type::Enum8:
        enum8_to_zval(col, index, return_value);
        break;
    case Type::Enum16:
        enum16_to_zval(col, index, return_value);
        break;

    case Type::UUID:
        uuid_to_zval(col, index, return_value);
        break;
    case Type::IPv4:
        ipv4_to_zval(col, index, return_value);
        break;
    case Type::IPv6:
        ipv6_to_zval(col, index, return_value);
        break;

    case Type::Decimal:
    case Type::Decimal32:
    case Type::Decimal64:
    case Type::Decimal128:
        decimal_to_zval(col, index, return_value);
        break;

    case Type::Int128:
        int128_to_zval(col, index, return_value);
        break;
    case Type::UInt128:
        uint128_to_zval(col, index, return_value);
        break;

    case Type::LowCardinality:
        lowcardinality_to_zval(col, index, return_value);
        break;

    case Type::Point:
        point_to_zval(col, index, return_value);
        break;
    case Type::Ring:
        ring_to_zval(col, index, return_value);
        break;
    case Type::Polygon:
        polygon_to_zval(col, index, return_value);
        break;
    case Type::MultiPolygon:
        multipolygon_to_zval(col, index, return_value);
        break;

    default:
        /* Return an unknown type as a string through ItemView. */
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
