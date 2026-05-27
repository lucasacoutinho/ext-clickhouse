<?php

function clickhouse_enum_like_exists($class)
{
    return (function_exists('enum_exists') && enum_exists($class)) || class_exists($class);
}
function clickhouse_case_value($case)
{
    return is_object($case) && property_exists($case, 'value') ? $case->value : $case;
}

function clickhouse_enum_like_from($class, $value)
{
    return method_exists($class, 'from') ? $class::from($value) : $value;
}

function clickhouse_type_name($case)
{
    if (is_object($case) && property_exists($case, 'name')) {
        return $case->name;
    }

    switch ((int) $case) {
        case 0: return 'Void';
        case 1: return 'Int8';
        case 2: return 'Int16';
        case 3: return 'Int32';
        case 4: return 'Int64';
        case 5: return 'UInt8';
        case 6: return 'UInt16';
        case 7: return 'UInt32';
        case 8: return 'UInt64';
        case 9: return 'Float32';
        case 10: return 'Float64';
        case 11: return 'String';
        case 12: return 'FixedString';
        case 13: return 'DateTime';
        case 14: return 'Date';
        case 15: return 'Array';
        case 16: return 'Nullable';
        case 17: return 'Tuple';
        case 18: return 'Enum8';
        case 19: return 'Enum16';
        case 20: return 'UUID';
        case 21: return 'IPv4';
        case 22: return 'IPv6';
        case 23: return 'Int128';
        case 24: return 'UInt128';
        case 25: return 'Decimal';
        case 26: return 'Decimal32';
        case 27: return 'Decimal64';
        case 28: return 'Decimal128';
        case 29: return 'LowCardinality';
        case 30: return 'DateTime64';
        case 31: return 'Date32';
        case 32: return 'Map';
        case 33: return 'Point';
        case 34: return 'Ring';
        case 35: return 'Polygon';
        case 36: return 'MultiPolygon';
        default: return 'unknown';
    }
}
