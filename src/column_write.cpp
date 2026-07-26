#include "src/column_write.h"
#include "src/common.h"

extern "C" {
#include "ext/date/php_date.h"
}

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

#include <cctype>
#include <cmath>
#include <cstdio>
#include <cstring>
#include <limits>
#include <sstream>
#include <string>
#include <string_view>
#include <type_traits>
#include <ctime>
#include <arpa/inet.h>

using namespace clickhouse;

/* Forward declaration for recursive calls */
void php_clickhouse_zval_to_column(ColumnRef &col, zval *value);
static void append_default_value(ColumnRef &col);

static bool is_decimal_digit(char c)
{
    return c >= '0' && c <= '9';
}

static bool parse_uint64_decimal(std::string_view input, uint64_t max, uint64_t &out)
{
    if (input.empty())
        return false;

    size_t pos = 0;
    if (input[pos] == '+') {
        pos++;
        if (pos == input.size())
            return false;
    } else if (input[pos] == '-') {
        return false;
    }

    uint64_t value = 0;
    for (; pos < input.size(); ++pos) {
        if (!is_decimal_digit(input[pos]))
            return false;
        uint64_t digit = static_cast<uint64_t>(input[pos] - '0');
        if (value > (max - digit) / 10)
            return false;
        value = value * 10 + digit;
    }

    out = value;
    return true;
}

static bool parse_int64_decimal(std::string_view input, int64_t min, int64_t max, int64_t &out)
{
    if (input.empty())
        return false;

    bool negative = false;
    size_t pos = 0;
    if (input[pos] == '+' || input[pos] == '-') {
        negative = input[pos] == '-';
        pos++;
        if (pos == input.size())
            return false;
    }

    uint64_t limit = negative ? static_cast<uint64_t>(max) + 1 : static_cast<uint64_t>(max);

    uint64_t magnitude = 0;
    for (; pos < input.size(); ++pos) {
        if (!is_decimal_digit(input[pos]))
            return false;
        uint64_t digit = static_cast<uint64_t>(input[pos] - '0');
        if (magnitude > (limit - digit) / 10)
            return false;
        magnitude = magnitude * 10 + digit;
    }

    int64_t value;
    if (negative) {
        if (magnitude == static_cast<uint64_t>(std::numeric_limits<int64_t>::max()) + 1) {
            value = std::numeric_limits<int64_t>::min();
        } else {
            value = -static_cast<int64_t>(magnitude);
        }
    } else {
        value = static_cast<int64_t>(magnitude);
    }

    if (value < min || value > max)
        return false;
    out = value;
    return true;
}

static bool is_valid_calendar_date(int year, int month, int day)
{
    if (month < 1 || month > 12 || day < 1)
        return false;

    static constexpr int days_per_month[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    int max_day = days_per_month[month - 1];
    bool leap = (year % 4 == 0 && year % 100 != 0) || year % 400 == 0;
    if (month == 2 && leap)
        max_day++;

    return day <= max_day;
}

static bool parse_two_digits(std::string_view input, size_t offset, int &value)
{
    if (offset + 2 > input.size() || !is_decimal_digit(input[offset]) ||
        !is_decimal_digit(input[offset + 1])) {
        return false;
    }

    value = (input[offset] - '0') * 10 + (input[offset + 1] - '0');
    return true;
}

static bool has_valid_iso_datetime_suffix(std::string_view input)
{
    if (input.size() == 10)
        return true;

    size_t pos = 10;
    if (input[pos] == 'T') {
        pos++;
    } else if (std::isspace(static_cast<unsigned char>(input[pos]))) {
        while (pos < input.size() && std::isspace(static_cast<unsigned char>(input[pos])))
            pos++;
    } else {
        return false;
    }

    int hour = 0;
    int minute = 0;
    int second = 0;
    if (!parse_two_digits(input, pos, hour) || pos + 8 > input.size() || input[pos + 2] != ':' ||
        !parse_two_digits(input, pos + 3, minute) || input[pos + 5] != ':' ||
        !parse_two_digits(input, pos + 6, second) || hour > 23 || minute > 59 || second > 59) {
        return false;
    }
    pos += 8;

    if (pos < input.size() && input[pos] == '.') {
        size_t fractional_start = ++pos;
        while (pos < input.size() && is_decimal_digit(input[pos]))
            pos++;
        if (pos == fractional_start)
            return false;
    }

    if (pos < input.size() && (input[pos] == 'Z' || input[pos] == 'z')) {
        pos++;
    } else if (pos < input.size() && (input[pos] == '+' || input[pos] == '-')) {
        int offset_hour = 0;
        int offset_minute = 0;
        pos++;
        if (!parse_two_digits(input, pos, offset_hour) || pos + 5 > input.size() ||
            input[pos + 2] != ':' || !parse_two_digits(input, pos + 3, offset_minute) ||
            offset_hour > 23 || offset_minute > 59) {
            return false;
        }
        pos += 5;
    }

    while (pos < input.size() && std::isspace(static_cast<unsigned char>(input[pos])))
        pos++;

    return pos == input.size();
}

static bool parse_date_prefix(std::string_view input, struct tm &tm_buf)
{
    if (input.size() < 10 || input[4] != '-' || input[7] != '-')
        return false;

    for (size_t i :
         {size_t{0}, size_t{1}, size_t{2}, size_t{3}, size_t{5}, size_t{6}, size_t{8}, size_t{9}}) {
        if (!is_decimal_digit(input[i]))
            return false;
    }

    int year =
        (input[0] - '0') * 1000 + (input[1] - '0') * 100 + (input[2] - '0') * 10 + (input[3] - '0');
    int month = (input[5] - '0') * 10 + (input[6] - '0');
    int day = (input[8] - '0') * 10 + (input[9] - '0');
    if (!is_valid_calendar_date(year, month, day) || !has_valid_iso_datetime_suffix(input))
        return false;

    tm_buf.tm_year = year - 1900;
    tm_buf.tm_mon = month - 1;
    tm_buf.tm_mday = day;

    return true;
}

static bool is_valid_calendar_time(const struct tm &tm_buf)
{
    return is_valid_calendar_date(tm_buf.tm_year + 1900, tm_buf.tm_mon + 1, tm_buf.tm_mday) &&
           tm_buf.tm_hour >= 0 && tm_buf.tm_hour <= 23 && tm_buf.tm_min >= 0 &&
           tm_buf.tm_min <= 59 && tm_buf.tm_sec >= 0 && tm_buf.tm_sec <= 59;
}

static int64_t days_from_civil(int year, unsigned month, unsigned day)
{
    year -= month <= 2;
    const int era = (year >= 0 ? year : year - 399) / 400;
    const unsigned year_of_era = static_cast<unsigned>(year - era * 400);
    const unsigned adjusted_month =
        static_cast<unsigned>(static_cast<int>(month) + (month > 2 ? -3 : 9));
    const unsigned day_of_year = (153 * adjusted_month + 2) / 5 + day - 1;
    const unsigned day_of_era =
        year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    return static_cast<int64_t>(era) * 146097 + static_cast<int64_t>(day_of_era) - 719468;
}

static int64_t timestamp_to_days(int64_t timestamp)
{
    int64_t days = timestamp / 86400;
    if (timestamp < 0 && timestamp % 86400 != 0)
        days--;
    return days;
}

static bool datetime_in_timezone_to_epoch(const struct tm &tm_buf, const std::string &timezone,
                                          int64_t &seconds)
{
    char local_time[32];
    int len = snprintf(local_time, sizeof(local_time), "%04d-%02d-%02d %02d:%02d:%02d",
                       tm_buf.tm_year + 1900, tm_buf.tm_mon + 1, tm_buf.tm_mday, tm_buf.tm_hour,
                       tm_buf.tm_min, tm_buf.tm_sec);
    if (len < 0 || static_cast<size_t>(len) >= sizeof(local_time))
        return false;

    std::string input(local_time, static_cast<size_t>(len));
    input.push_back(' ');
    input.append(timezone.empty() ? "UTC" : timezone);

    zval datetime;
    ZVAL_UNDEF(&datetime);
    php_date_instantiate(php_date_get_immutable_ce(), &datetime);
    php_date_obj *date_obj = Z_PHPDATE_P(&datetime);

#if PHP_VERSION_ID < 80000
    constexpr int date_init_flags = 0;
#else
    constexpr int date_init_flags = PHP_DATE_INIT_FORMAT;
#endif
    bool initialized =
        php_date_initialize(date_obj, const_cast<char *>(input.c_str()), input.size(),
                            const_cast<char *>("!Y-m-d H:i:s e"), nullptr, date_init_flags);
    if (initialized && date_obj->time) {
        seconds = static_cast<int64_t>(date_obj->time->sse);
    }

    zval_ptr_dtor(&datetime);
    if (!initialized && EG(exception)) {
        zend_clear_exception();
    }
    return initialized;
}

static bool decimal_value_fits_precision(std::string_view input, size_t precision, size_t scale)
{
    if (input.empty())
        return false;

    size_t pos = input[0] == '-' ? 1 : 0;
    if (pos == input.size())
        return false;

    size_t dot = std::string_view::npos;
    size_t digits = 0;
    for (size_t i = pos; i < input.size(); ++i) {
        if (input[i] == '.') {
            if (dot != std::string_view::npos) {
                return false;
            }
            dot = i;
            continue;
        }
        if (!is_decimal_digit(input[i])) {
            return false;
        }
        digits++;
    }
    if (digits == 0)
        return false;

    std::string scaled_digits;
    if (dot == std::string_view::npos) {
        scaled_digits.assign(input.substr(pos));
    } else {
        scaled_digits.assign(input.substr(pos, dot - pos));
        size_t fractional_digits = input.size() - dot - 1;
        size_t copied_fractional_digits = std::min(fractional_digits, scale);
        scaled_digits.append(input.substr(dot + 1, copied_fractional_digits));
        scaled_digits.append(scale - copied_fractional_digits, '0');
    }

    size_t first_nonzero = scaled_digits.find_first_not_of('0');
    size_t significant_digits =
        first_nonzero == std::string::npos ? 0 : scaled_digits.size() - first_nonzero;
    return significant_digits <= precision;
}

template <typename T> static bool zval_to_integral(zval *value, T &out)
{
    if (Z_TYPE_P(value) == IS_TRUE || Z_TYPE_P(value) == IS_FALSE) {
        zend_long bool_value = Z_TYPE_P(value) == IS_TRUE ? 1 : 0;
        if constexpr (std::is_unsigned_v<T>) {
            out = static_cast<T>(bool_value);
        } else {
            out = static_cast<T>(bool_value);
        }
        return true;
    }

    if constexpr (std::is_unsigned_v<T>) {
        uint64_t parsed = 0;
        uint64_t max = static_cast<uint64_t>(std::numeric_limits<T>::max());

        if (Z_TYPE_P(value) == IS_LONG) {
            zend_long lval = Z_LVAL_P(value);
            if (lval < 0 || static_cast<uint64_t>(lval) > max)
                return false;
            parsed = static_cast<uint64_t>(lval);
        } else if (Z_TYPE_P(value) == IS_DOUBLE) {
            double dval = Z_DVAL_P(value);
            long double wide = static_cast<long double>(dval);
            if (!std::isfinite(dval) || std::trunc(wide) != wide || wide < 0.0L ||
                wide > static_cast<long double>(max)) {
                return false;
            }
            parsed = static_cast<uint64_t>(wide);
        } else if (Z_TYPE_P(value) == IS_STRING) {
            if (!parse_uint64_decimal(std::string_view(Z_STRVAL_P(value), Z_STRLEN_P(value)), max,
                                      parsed)) {
                return false;
            }
        } else {
            return false;
        }

        out = static_cast<T>(parsed);
        return true;
    } else {
        int64_t parsed = 0;
        int64_t min = static_cast<int64_t>(std::numeric_limits<T>::min());
        int64_t max = static_cast<int64_t>(std::numeric_limits<T>::max());

        if (Z_TYPE_P(value) == IS_LONG) {
            zend_long lval = Z_LVAL_P(value);
            if (lval < min || lval > max)
                return false;
            parsed = static_cast<int64_t>(lval);
        } else if (Z_TYPE_P(value) == IS_DOUBLE) {
            double dval = Z_DVAL_P(value);
            long double wide = static_cast<long double>(dval);
            if (!std::isfinite(dval) || std::trunc(wide) != wide ||
                wide < static_cast<long double>(min) || wide > static_cast<long double>(max)) {
                return false;
            }
            parsed = static_cast<int64_t>(wide);
        } else if (Z_TYPE_P(value) == IS_STRING) {
            if (!parse_int64_decimal(std::string_view(Z_STRVAL_P(value), Z_STRLEN_P(value)), min,
                                     max, parsed)) {
                return false;
            }
        } else {
            return false;
        }

        out = static_cast<T>(parsed);
        return true;
    }
}

template <typename T> static void write_numeric(ColumnRef &col, zval *value)
{
    auto typed = col->As<ColumnVector<T>>();
    if (!typed) {
        zend_throw_exception(clickhouse_ce_ValidationException, "Column type mismatch", 0);
        return;
    }

    T parsed;
    if (!zval_to_integral<T>(value, parsed)) {
        zend_throw_exception_ex(clickhouse_ce_ValidationException, 0,
                                "Invalid integer value for ClickHouse type %s",
                                col->Type()->GetName().c_str());
        return;
    }

    typed->Append(parsed);
}

template <typename T> static void write_float(ColumnRef &col, zval *value)
{
    auto typed = col->As<ColumnVector<T>>();
    if (!typed) {
        zend_throw_exception(clickhouse_ce_ValidationException, "Column type mismatch", 0);
        return;
    }
    double dval = zval_get_double(value);
    typed->Append(static_cast<T>(dval));
}

static void write_string(ColumnRef &col, zval *value)
{
    auto typed = col->As<ColumnString>();
    if (!typed) {
        zend_throw_exception(clickhouse_ce_ValidationException, "Column type mismatch", 0);
        return;
    }
    zend_string *str = zval_get_string(value);
    typed->Append(std::string_view(ZSTR_VAL(str), ZSTR_LEN(str)));
    zend_string_release(str);
}

static void write_fixed_string(ColumnRef &col, zval *value)
{
    auto typed = col->As<ColumnFixedString>();
    if (!typed) {
        zend_throw_exception(clickhouse_ce_ValidationException, "Column type mismatch", 0);
        return;
    }
    zend_string *str = zval_get_string(value);
    typed->Append(std::string_view(ZSTR_VAL(str), ZSTR_LEN(str)));
    zend_string_release(str);
}

static void write_date(ColumnRef &col, zval *value)
{
    auto typed = col->As<ColumnDate>();
    if (!typed) {
        zend_throw_exception(clickhouse_ce_ValidationException, "Column type mismatch", 0);
        return;
    }

    if (Z_TYPE_P(value) == IS_STRING) {
        /* Parse 'YYYY-MM-DD' */
        struct tm tm_buf = {};
        if (parse_date_prefix(std::string_view(Z_STRVAL_P(value), Z_STRLEN_P(value)), tm_buf)) {
            int64_t days =
                days_from_civil(tm_buf.tm_year + 1900, static_cast<unsigned>(tm_buf.tm_mon + 1),
                                static_cast<unsigned>(tm_buf.tm_mday));
            if (days >= 0 && days <= std::numeric_limits<uint16_t>::max()) {
                typed->AppendRaw(static_cast<uint16_t>(days));
                return;
            }
        }
        zend_throw_exception_ex(clickhouse_ce_ValidationException, 0, "Invalid Date value: %s",
                                Z_STRVAL_P(value));
        return;
    }

    zend_long timestamp;
    if (!zval_to_integral<zend_long>(value, timestamp)) {
        zend_throw_exception(clickhouse_ce_ValidationException,
                             "Date column expects YYYY-MM-DD string or integer timestamp", 0);
        return;
    }
    if (timestamp < 0) {
        zend_throw_exception(clickhouse_ce_ValidationException, "Date timestamp is out of range",
                             0);
        return;
    }
    uint64_t days = static_cast<uint64_t>(timestamp) / 86400;
    if (days > std::numeric_limits<uint16_t>::max()) {
        zend_throw_exception(clickhouse_ce_ValidationException, "Date timestamp is out of range",
                             0);
        return;
    }
    typed->AppendRaw(static_cast<uint16_t>(days));
}

static void write_date32(ColumnRef &col, zval *value)
{
    auto typed = col->As<ColumnDate32>();
    if (!typed) {
        zend_throw_exception(clickhouse_ce_ValidationException, "Column type mismatch", 0);
        return;
    }

    const int64_t min_days = days_from_civil(1900, 1, 1);
    const int64_t max_days = days_from_civil(2299, 12, 31);

    if (Z_TYPE_P(value) == IS_STRING) {
        struct tm tm_buf = {};
        if (parse_date_prefix(std::string_view(Z_STRVAL_P(value), Z_STRLEN_P(value)), tm_buf)) {
            int64_t days =
                days_from_civil(tm_buf.tm_year + 1900, static_cast<unsigned>(tm_buf.tm_mon + 1),
                                static_cast<unsigned>(tm_buf.tm_mday));
            if (days >= min_days && days <= max_days) {
                typed->AppendRaw(static_cast<int32_t>(days));
                return;
            }
        }
        zend_throw_exception_ex(clickhouse_ce_ValidationException, 0, "Invalid Date32 value: %s",
                                Z_STRVAL_P(value));
        return;
    }

    zend_long timestamp;
    if (!zval_to_integral<zend_long>(value, timestamp)) {
        zend_throw_exception(clickhouse_ce_ValidationException,
                             "Date32 column expects YYYY-MM-DD string or integer timestamp", 0);
        return;
    }

    int64_t days = timestamp_to_days(static_cast<int64_t>(timestamp));
    if (days < min_days || days > max_days) {
        zend_throw_exception(clickhouse_ce_ValidationException, "Date32 timestamp is out of range",
                             0);
        return;
    }
    typed->AppendRaw(static_cast<int32_t>(days));
}

static void write_datetime(ColumnRef &col, zval *value)
{
    auto typed = col->As<ColumnDateTime>();
    if (!typed) {
        zend_throw_exception(clickhouse_ce_ValidationException, "Column type mismatch", 0);
        return;
    }
    uint32_t timestamp;
    if (!zval_to_integral<uint32_t>(value, timestamp)) {
        zend_throw_exception(clickhouse_ce_ValidationException,
                             "DateTime timestamp is out of range", 0);
        return;
    }
    typed->AppendRaw(timestamp);
}

static void append_default_value(ColumnRef &col)
{
    switch (col->Type()->GetCode()) {
    case Type::Int8:
        col->As<ColumnInt8>()->Append(0);
        break;
    case Type::Int16:
        col->As<ColumnInt16>()->Append(0);
        break;
    case Type::Int32:
        col->As<ColumnInt32>()->Append(0);
        break;
    case Type::Int64:
        col->As<ColumnInt64>()->Append(0);
        break;
    case Type::UInt8:
        col->As<ColumnUInt8>()->Append(0);
        break;
    case Type::UInt16:
        col->As<ColumnUInt16>()->Append(0);
        break;
    case Type::UInt32:
        col->As<ColumnUInt32>()->Append(0);
        break;
    case Type::UInt64:
        col->As<ColumnUInt64>()->Append(0);
        break;
    case Type::Float32:
        col->As<ColumnFloat32>()->Append(0.0f);
        break;
    case Type::Float64:
        col->As<ColumnFloat64>()->Append(0.0);
        break;

    case Type::String:
        col->As<ColumnString>()->Append(std::string_view(""));
        break;
    case Type::FixedString:
        col->As<ColumnFixedString>()->Append(std::string_view(""));
        break;

    case Type::Date:
        col->As<ColumnDate>()->Append(static_cast<std::time_t>(0));
        break;
    case Type::Date32:
        col->As<ColumnDate32>()->Append(static_cast<std::time_t>(0));
        break;
    case Type::DateTime:
        col->As<ColumnDateTime>()->Append(static_cast<std::time_t>(0));
        break;
    case Type::DateTime64:
        col->As<ColumnDateTime64>()->Append(0);
        break;

    case Type::Decimal:
    case Type::Decimal32:
    case Type::Decimal64:
    case Type::Decimal128:
        col->As<ColumnDecimal>()->Append(std::string("0"));
        break;

    case Type::Nullable: {
        auto nullable = col->As<ColumnNullable>();
        nullable->Append(true);
        auto nested = nullable->Nested();
        append_default_value(nested);
        break;
    }

    case Type::Array: {
        auto typed = col->As<ColumnArray>();
        auto nested_type = col->Type()->As<ArrayType>()->GetItemType();
        auto nested_col = CreateColumnByType(nested_type->GetName());
        typed->AppendAsColumn(nested_col);
        break;
    }

    case Type::Tuple: {
        auto typed = col->As<ColumnTuple>();
        for (size_t i = 0; i < typed->TupleSize(); ++i) {
            auto elem_col = (*typed)[i];
            append_default_value(elem_col);
        }
        break;
    }

    case Type::Map: {
        auto typed = col->As<ColumnMap>();
        auto map_type = col->Type()->As<MapType>();
        auto key_col = CreateColumnByType(map_type->GetKeyType()->GetName());
        auto val_col = CreateColumnByType(map_type->GetValueType()->GetName());
        auto tuple_col = std::make_shared<ColumnTuple>(std::vector<ColumnRef>{key_col, val_col});
        auto empty_tuple = std::make_shared<ColumnTuple>(
            std::vector<ColumnRef>{CreateColumnByType(map_type->GetKeyType()->GetName()),
                                   CreateColumnByType(map_type->GetValueType()->GetName())});
        auto temp_array = std::make_shared<ColumnArray>(empty_tuple);
        temp_array->AppendAsColumn(tuple_col);
        auto temp_map = std::make_shared<ColumnMap>(temp_array);
        typed->Append(temp_map);
        break;
    }

    case Type::Enum8: {
        auto enum_type = col->Type()->As<EnumType>();
        auto it = enum_type->BeginValueToName();
        if (it == enum_type->EndValueToName()) {
            zend_throw_exception(clickhouse_ce_ValidationException,
                                 "Cannot append default value for empty Enum8", 0);
            return;
        }
        col->As<ColumnEnum8>()->Append(static_cast<int8_t>(it->first));
        break;
    }
    case Type::Enum16: {
        auto enum_type = col->Type()->As<EnumType>();
        auto it = enum_type->BeginValueToName();
        if (it == enum_type->EndValueToName()) {
            zend_throw_exception(clickhouse_ce_ValidationException,
                                 "Cannot append default value for empty Enum16", 0);
            return;
        }
        col->As<ColumnEnum16>()->Append(static_cast<int16_t>(it->first));
        break;
    }

    case Type::UUID:
        col->As<ColumnUUID>()->Append(clickhouse::UUID{0, 0});
        break;
    case Type::IPv4:
        col->As<ColumnIPv4>()->Append(static_cast<uint32_t>(0));
        break;
    case Type::IPv6: {
        in6_addr addr = {};
        col->As<ColumnIPv6>()->Append(addr);
        break;
    }

    case Type::Int128:
        col->As<ColumnInt128>()->Append(absl::int128(0));
        break;
    case Type::UInt128:
        col->As<ColumnUInt128>()->Append(absl::uint128(0));
        break;

    case Type::Point:
        col->As<ColumnPoint>()->Append(std::make_tuple(0.0, 0.0));
        break;
    case Type::Ring: {
        auto point_col = std::make_shared<ColumnPoint>();
        auto point_array =
            std::make_shared<ColumnArrayT<ColumnPoint>>(std::make_shared<ColumnPoint>());
        point_array->AppendAsColumn(point_col);
        col->Append(std::make_shared<ColumnRing>(point_array));
        break;
    }
    case Type::Polygon: {
        auto ring_col = std::make_shared<ColumnRing>();
        auto ring_array =
            std::make_shared<ColumnArrayT<ColumnRing>>(std::make_shared<ColumnRing>());
        ring_array->AppendAsColumn(ring_col);
        col->Append(std::make_shared<ColumnPolygon>(ring_array));
        break;
    }
    case Type::MultiPolygon: {
        auto polygon_col = std::make_shared<ColumnPolygon>();
        auto polygon_array =
            std::make_shared<ColumnArrayT<ColumnPolygon>>(std::make_shared<ColumnPolygon>());
        polygon_array->AppendAsColumn(polygon_col);
        col->Append(std::make_shared<ColumnMultiPolygon>(polygon_array));
        break;
    }
    case Type::LowCardinality: {
        auto typed = col->As<ColumnLowCardinality>();
        auto nested_type = typed->GetNestedType();
        auto nested_col = CreateColumnByType(nested_type->GetName());
        if (!nested_col) {
            zend_throw_exception_ex(clickhouse_ce_ValidationException, 0,
                                    "Cannot create default LowCardinality nested column: %s",
                                    nested_type->GetName().c_str());
            return;
        }
        append_default_value(nested_col);
        if (EG(exception))
            return;
        typed->Append(nested_col);
        break;
    }

    default:
        zend_throw_exception_ex(clickhouse_ce_ValidationException, 0,
                                "Cannot append default value for ClickHouse type: %s",
                                col->Type()->GetName().c_str());
        break;
    }
}

static void write_nullable(ColumnRef &col, zval *value)
{
    auto typed = col->As<ColumnNullable>();
    if (!typed) {
        zend_throw_exception(clickhouse_ce_ValidationException, "Column type mismatch", 0);
        return;
    }

    if (Z_TYPE_P(value) == IS_NULL) {
        typed->Append(true);
        auto nested = typed->Nested();
        append_default_value(nested);
    } else {
        typed->Append(false);
        auto nested = typed->Nested();
        php_clickhouse_zval_to_column(nested, value);
    }
}

static void write_array(ColumnRef &col, zval *value)
{
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
    ZEND_HASH_FOREACH_VAL(ht, entry)
    {
        php_clickhouse_zval_to_column(nested_col, entry);
        if (EG(exception))
            return;
    }
    ZEND_HASH_FOREACH_END();

    typed->AppendAsColumn(nested_col);
}

static void write_enum8(ColumnRef &col, zval *value)
{
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

static void write_enum16(ColumnRef &col, zval *value)
{
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

static void write_uuid(ColumnRef &col, zval *value)
{
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
    auto hex_value = [](char c) -> int {
        if (c >= '0' && c <= '9')
            return c - '0';
        if (c >= 'a' && c <= 'f')
            return c - 'a' + 10;
        if (c >= 'A' && c <= 'F')
            return c - 'A' + 10;
        return -1;
    };
    auto parse_hex = [&](size_t offset, size_t len, uint64_t &out) -> bool {
        uint64_t value = 0;
        for (size_t i = 0; i < len; ++i) {
            int digit = hex_value(uuid_str[offset + i]);
            if (digit < 0)
                return false;
            value = (value << 4) | static_cast<uint64_t>(digit);
        }
        out = value;
        return true;
    };

    uint64_t p0, p1, p2, p3, p4, p5;
    if (uuid_str.size() != 36 || uuid_str[8] != '-' || uuid_str[13] != '-' || uuid_str[18] != '-' ||
        uuid_str[23] != '-' || !parse_hex(0, 8, p0) || !parse_hex(9, 4, p1) ||
        !parse_hex(14, 4, p2) || !parse_hex(19, 4, p3) || !parse_hex(24, 4, p4) ||
        !parse_hex(28, 8, p5)) {
        zend_throw_exception_ex(clickhouse_ce_ValidationException, 0, "Invalid UUID value: %s",
                                uuid_str.c_str());
        return;
    }

    hi = (p0 << 32) | (p1 << 16) | p2;
    lo = (p3 << 48) | (p4 << 32) | p5;
    typed->Append(clickhouse::UUID{hi, lo});
}

static void write_datetime64(ColumnRef &col, zval *value)
{
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
        if (!rest) {
            zend_throw_exception_ex(clickhouse_ce_ValidationException, 0,
                                    "Invalid DateTime64 value: %s", str);
            return;
        }
        if (!is_valid_calendar_time(tm_buf)) {
            zend_throw_exception_ex(clickhouse_ce_ValidationException, 0,
                                    "Invalid DateTime64 value: %s", str);
            return;
        }

        int64_t seconds = 0;
        if (!datetime_in_timezone_to_epoch(tm_buf, typed->Timezone(), seconds)) {
            zend_throw_exception_ex(clickhouse_ce_ValidationException, 0,
                                    "Invalid DateTime64 timezone or value: %s", str);
            return;
        }
        int64_t frac = 0;
        size_t precision = typed->GetPrecision();

        if (rest && *rest == '.') {
            rest++;
            if (precision == 0) {
                size_t digits = 0;
                bool non_zero = false;
                while (*rest >= '0' && *rest <= '9') {
                    non_zero = non_zero || *rest != '0';
                    rest++;
                    digits++;
                }
                if (digits == 0 || non_zero) {
                    zend_throw_exception_ex(clickhouse_ce_ValidationException, 0,
                                            "Invalid DateTime64 fractional precision for value: %s",
                                            str);
                    return;
                }
            } else {
                /* Parse fractional digits up to precision */
                int64_t parsed_frac = 0;
                size_t digits = 0;
                while (*rest >= '0' && *rest <= '9' && digits < precision) {
                    parsed_frac = parsed_frac * 10 + (*rest - '0');
                    rest++;
                    digits++;
                }
                if (digits == 0 || (*rest >= '0' && *rest <= '9')) {
                    zend_throw_exception_ex(clickhouse_ce_ValidationException, 0,
                                            "Invalid DateTime64 fractional precision for value: %s",
                                            str);
                    return;
                }
                /* Pad remaining precision digits with zeros */
                for (size_t i = digits; i < precision; ++i) {
                    parsed_frac *= 10;
                }
                frac = parsed_frac;
            }
        }

        if (*rest != '\0') {
            zend_throw_exception_ex(clickhouse_ce_ValidationException, 0,
                                    "Invalid DateTime64 value: %s", str);
            return;
        }

        int64_t divisor = 1;
        for (size_t i = 0; i < precision; ++i)
            divisor *= 10;
        Int128 ticks =
            static_cast<Int128>(seconds) * static_cast<Int128>(divisor) + static_cast<Int128>(frac);
        if (ticks < static_cast<Int128>(std::numeric_limits<int64_t>::min()) ||
            ticks > static_cast<Int128>(std::numeric_limits<int64_t>::max())) {
            zend_throw_exception_ex(clickhouse_ce_ValidationException, 0,
                                    "DateTime64 value is out of range: %s", str);
            return;
        }
        typed->Append(static_cast<int64_t>(ticks));
        return;
    }

    zend_throw_exception(clickhouse_ce_ValidationException,
                         "DateTime64 column expects YYYY-MM-DD HH:MM:SS string or integer ticks",
                         0);
}

static void write_decimal(ColumnRef &col, zval *value)
{
    auto typed = col->As<ColumnDecimal>();
    if (!typed) {
        zend_throw_exception(clickhouse_ce_ValidationException, "Column type mismatch", 0);
        return;
    }
    zend_string *str = zval_get_string(value);
    std::string decimal(ZSTR_VAL(str), ZSTR_LEN(str));
    zend_string_release(str);

    if (!decimal_value_fits_precision(decimal, typed->GetPrecision(), typed->GetScale())) {
        zend_throw_exception_ex(clickhouse_ce_ValidationException, 0,
                                "Decimal value is invalid or exceeds precision %zu: %s",
                                typed->GetPrecision(), decimal.c_str());
        return;
    }

    typed->Append(decimal);
}

static void write_ipv4(ColumnRef &col, zval *value)
{
    auto typed = col->As<ColumnIPv4>();
    if (!typed) {
        zend_throw_exception(clickhouse_ce_ValidationException, "Column type mismatch", 0);
        return;
    }

    if (Z_TYPE_P(value) == IS_STRING) {
        typed->Append(std::string(Z_STRVAL_P(value), Z_STRLEN_P(value)));
    } else {
        /* Numeric: treat as host-byte-order uint32 */
        uint32_t parsed = 0;
        if (!zval_to_integral<uint32_t>(value, parsed)) {
            zend_throw_exception(
                clickhouse_ce_ValidationException,
                "IPv4 column expects an address string or integer in the UInt32 range", 0);
            return;
        }
        typed->Append(parsed);
    }
}

static void write_ipv6(ColumnRef &col, zval *value)
{
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
        zend_throw_exception(clickhouse_ce_ValidationException, "IPv6 column expects string value",
                             0);
    }
}

static void write_int128(ColumnRef &col, zval *value)
{
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
    if (!s.empty() && (s[0] == '-' || s[0] == '+')) {
        negative = s[0] == '-';
        pos = 1;
    }
    if (pos == s.size()) {
        zend_throw_exception_ex(clickhouse_ce_ValidationException, 0, "Invalid Int128 value: %s",
                                s.c_str());
        return;
    }

    absl::int128 result = 0;
    for (; pos < s.size(); ++pos) {
        if (!is_decimal_digit(s[pos])) {
            zend_throw_exception_ex(clickhouse_ce_ValidationException, 0,
                                    "Invalid Int128 value: %s", s.c_str());
            return;
        }

        absl::int128 digit = s[pos] - '0';
        if (negative) {
            absl::int128 min = std::numeric_limits<absl::int128>::min();
            if (result < (min + digit) / 10) {
                zend_throw_exception_ex(clickhouse_ce_ValidationException, 0,
                                        "Int128 value out of range: %s", s.c_str());
                return;
            }
            result = result * 10 - digit;
        } else {
            absl::int128 max = std::numeric_limits<absl::int128>::max();
            if (result > (max - digit) / 10) {
                zend_throw_exception_ex(clickhouse_ce_ValidationException, 0,
                                        "Int128 value out of range: %s", s.c_str());
                return;
            }
            result = result * 10 + digit;
        }
    }
    typed->Append(result);
}

static void write_uint128(ColumnRef &col, zval *value)
{
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

    size_t pos = 0;
    if (!s.empty() && s[0] == '+') {
        pos = 1;
    }
    if (pos == s.size() || (!s.empty() && s[0] == '-')) {
        zend_throw_exception_ex(clickhouse_ce_ValidationException, 0, "Invalid UInt128 value: %s",
                                s.c_str());
        return;
    }

    absl::uint128 result = 0;
    absl::uint128 max = std::numeric_limits<absl::uint128>::max();
    for (; pos < s.size(); ++pos) {
        if (!is_decimal_digit(s[pos])) {
            zend_throw_exception_ex(clickhouse_ce_ValidationException, 0,
                                    "Invalid UInt128 value: %s", s.c_str());
            return;
        }
        absl::uint128 digit = s[pos] - '0';
        if (result > (max - digit) / 10) {
            zend_throw_exception_ex(clickhouse_ce_ValidationException, 0,
                                    "UInt128 value out of range: %s", s.c_str());
            return;
        }
        result = result * 10 + digit;
    }
    typed->Append(result);
}

static void write_tuple(ColumnRef &col, zval *value)
{
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
                                "Tuple expects %zu elements, got %u", tuple_size,
                                zend_hash_num_elements(ht));
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
        if (EG(exception))
            return;
    }
}

static void write_map(ColumnRef &col, zval *value)
{
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

    ZEND_HASH_FOREACH_KEY_VAL(ht, key_idx, key_str, entry)
    {
        /* Append key */
        zval key_zv;
        if (key_str) {
            ZVAL_STR_COPY(&key_zv, key_str);
        } else {
            ZVAL_LONG(&key_zv, static_cast<zend_long>(key_idx));
        }
        php_clickhouse_zval_to_column(key_col, &key_zv);
        zval_ptr_dtor(&key_zv);
        if (EG(exception))
            return;

        /* Append value */
        php_clickhouse_zval_to_column(val_col, entry);
        if (EG(exception))
            return;
    }
    ZEND_HASH_FOREACH_END();

    /* Build Tuple(K,V) column containing all kv pairs for this map row */
    std::vector<ColumnRef> tuple_cols = {key_col, val_col};
    auto tuple_col = std::make_shared<ColumnTuple>(tuple_cols);

    /* ColumnMap::Append(ColumnRef) only accepts another ColumnMap (checks As<ColumnMap>).
     * So we build a temporary one-row ColumnMap:
     * 1. Create an empty tuple structure for the ColumnArray wrapper
     * 2. Create ColumnArray, AppendAsColumn to add one row of kv pairs
     * 3. Wrap in ColumnMap so the target's Append accepts it */
    auto empty_tuple = std::make_shared<ColumnTuple>(std::vector<ColumnRef>{
        CreateColumnByType(key_type->GetName()), CreateColumnByType(val_type->GetName())});
    auto temp_array = std::make_shared<ColumnArray>(empty_tuple);
    temp_array->AppendAsColumn(tuple_col);

    auto temp_map = std::make_shared<ColumnMap>(temp_array);
    typed->Append(temp_map);
}

static void write_lowcardinality(ColumnRef &col, zval *value)
{
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
                auto inner = nullable->Nested();
                append_default_value(inner);
            }
            temp_lc->Append(nested_col);
        }
        typed->Append(temp_col);
        return;
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
            if (EG(exception))
                return;
        }
    } else {
        php_clickhouse_zval_to_column(nested_col, value);
        if (EG(exception))
            return;
    }

    temp_lc->Append(nested_col);
    typed->Append(temp_col);
}

static void write_point(ColumnRef &col, zval *value)
{
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
static bool parse_points_from_zval(zval *value, std::shared_ptr<ColumnPoint> &point_col)
{
    if (Z_TYPE_P(value) != IS_ARRAY) {
        zend_throw_exception(clickhouse_ce_ValidationException, "Ring element must be [x, y] array",
                             0);
        return false;
    }
    HashTable *ht = Z_ARRVAL_P(value);
    zval *entry;
    ZEND_HASH_FOREACH_VAL(ht, entry)
    {
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
    }
    ZEND_HASH_FOREACH_END();
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
static void write_ring(ColumnRef &col, zval *value)
{
    /* Build a ColumnPoint with all the ring's points */
    auto point_col = std::make_shared<ColumnPoint>();
    if (!parse_points_from_zval(value, point_col))
        return;

    /* Build a one-row ColumnArrayT<ColumnPoint> containing these points */
    auto point_array = std::make_shared<ColumnArrayT<ColumnPoint>>(std::make_shared<ColumnPoint>());
    point_array->AppendAsColumn(point_col);

    /* Wrap in a temp ColumnRing, then merge into target */
    auto temp_ring = std::make_shared<ColumnRing>(point_array);
    col->Append(temp_ring);
}

static void write_polygon(ColumnRef &col, zval *value)
{
    if (Z_TYPE_P(value) != IS_ARRAY) {
        zend_throw_exception(clickhouse_ce_ValidationException,
                             "Expected array of rings for Polygon", 0);
        return;
    }

    /* Build a ColumnRing containing all the rings in this polygon */
    auto ring_col = std::make_shared<ColumnRing>();
    HashTable *ht = Z_ARRVAL_P(value);
    zval *ring_entry;
    ZEND_HASH_FOREACH_VAL(ht, ring_entry)
    {
        /* Each ring_entry is an array of [x,y] points */
        ColumnRef ring_ref = ring_col;
        write_ring(ring_ref, ring_entry);
        if (EG(exception))
            return;
    }
    ZEND_HASH_FOREACH_END();

    /* Build a one-row ColumnArrayT<ColumnRing> containing these rings */
    auto ring_array = std::make_shared<ColumnArrayT<ColumnRing>>(std::make_shared<ColumnRing>());
    ring_array->AppendAsColumn(ring_col);

    /* Wrap in a temp ColumnPolygon, then merge into target */
    auto temp_polygon = std::make_shared<ColumnPolygon>(ring_array);
    col->Append(temp_polygon);
}

static void write_multipolygon(ColumnRef &col, zval *value)
{
    if (Z_TYPE_P(value) != IS_ARRAY) {
        zend_throw_exception(clickhouse_ce_ValidationException,
                             "Expected array of polygons for MultiPolygon", 0);
        return;
    }

    /* Build a ColumnPolygon containing all polygons */
    auto polygon_col = std::make_shared<ColumnPolygon>();
    HashTable *ht = Z_ARRVAL_P(value);
    zval *poly_entry;
    ZEND_HASH_FOREACH_VAL(ht, poly_entry)
    {
        ColumnRef poly_ref = polygon_col;
        write_polygon(poly_ref, poly_entry);
        if (EG(exception))
            return;
    }
    ZEND_HASH_FOREACH_END();

    /* Build a one-row ColumnArrayT<ColumnPolygon> containing these polygons */
    auto poly_array =
        std::make_shared<ColumnArrayT<ColumnPolygon>>(std::make_shared<ColumnPolygon>());
    poly_array->AppendAsColumn(polygon_col);

    /* Wrap in a temp ColumnMultiPolygon, then merge into target */
    auto temp_mp = std::make_shared<ColumnMultiPolygon>(poly_array);
    col->Append(temp_mp);
}

void php_clickhouse_zval_to_column(ColumnRef &col, zval *value)
{
    switch (col->Type()->GetCode()) {
    case Type::Int8:
        write_numeric<int8_t>(col, value);
        break;
    case Type::Int16:
        write_numeric<int16_t>(col, value);
        break;
    case Type::Int32:
        write_numeric<int32_t>(col, value);
        break;
    case Type::Int64:
        write_numeric<int64_t>(col, value);
        break;
    case Type::UInt8:
        write_numeric<uint8_t>(col, value);
        break;
    case Type::UInt16:
        write_numeric<uint16_t>(col, value);
        break;
    case Type::UInt32:
        write_numeric<uint32_t>(col, value);
        break;
    case Type::UInt64:
        write_numeric<uint64_t>(col, value);
        break;
    case Type::Float32:
        write_float<float>(col, value);
        break;
    case Type::Float64:
        write_float<double>(col, value);
        break;

    case Type::String:
        write_string(col, value);
        break;
    case Type::FixedString:
        write_fixed_string(col, value);
        break;

    case Type::Date:
        write_date(col, value);
        break;
    case Type::Date32:
        write_date32(col, value);
        break;
    case Type::DateTime:
        write_datetime(col, value);
        break;
    case Type::DateTime64:
        write_datetime64(col, value);
        break;

    case Type::Decimal:
    case Type::Decimal32:
    case Type::Decimal64:
    case Type::Decimal128:
        write_decimal(col, value);
        break;

    case Type::Nullable:
        write_nullable(col, value);
        break;
    case Type::Array:
        write_array(col, value);
        break;
    case Type::Tuple:
        write_tuple(col, value);
        break;
    case Type::Map:
        write_map(col, value);
        break;

    case Type::Enum8:
        write_enum8(col, value);
        break;
    case Type::Enum16:
        write_enum16(col, value);
        break;

    case Type::UUID:
        write_uuid(col, value);
        break;
    case Type::IPv4:
        write_ipv4(col, value);
        break;
    case Type::IPv6:
        write_ipv6(col, value);
        break;

    case Type::Int128:
        write_int128(col, value);
        break;
    case Type::UInt128:
        write_uint128(col, value);
        break;

    case Type::LowCardinality:
        write_lowcardinality(col, value);
        break;

    case Type::Point:
        write_point(col, value);
        break;
    case Type::Ring:
        write_ring(col, value);
        break;
    case Type::Polygon:
        write_polygon(col, value);
        break;
    case Type::MultiPolygon:
        write_multipolygon(col, value);
        break;

    default:
        zend_throw_exception_ex(clickhouse_ce_ValidationException, 0,
                                "Write not supported for type: %s", col->Type()->GetName().c_str());
        break;
    }
}
