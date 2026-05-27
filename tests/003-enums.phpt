--TEST--
CompressionMethod and Type enum values
--EXTENSIONS--
clickhouse
--FILE--
<?php
require __DIR__ . '/clickhouse_compat.inc';

use ClickHouse\Driver\CompressionMethod;
use ClickHouse\Driver\Type;

// CompressionMethod
var_dump(clickhouse_case_value(CompressionMethod::None));
var_dump(clickhouse_case_value(CompressionMethod::LZ4));
var_dump(clickhouse_case_value(CompressionMethod::ZSTD));

// Type enum — spot check key values
var_dump(clickhouse_case_value(Type::Int8));
var_dump(clickhouse_case_value(Type::UInt64));
var_dump(clickhouse_case_value(Type::String));
var_dump(clickhouse_case_value(Type::Nullable));
var_dump(clickhouse_case_value(Type::Array));
var_dump(clickhouse_case_value(Type::Map));
var_dump(clickhouse_case_value(Type::DateTime64));

// Backed enum from value on PHP 8.1+, compatible constant fallback on older PHP.
var_dump(clickhouse_enum_like_from(Type::class, 11) === Type::String);
var_dump(clickhouse_enum_like_from(CompressionMethod::class, 1) === CompressionMethod::LZ4);
?>
--EXPECT--
int(-1)
int(1)
int(2)
int(1)
int(8)
int(11)
int(16)
int(15)
int(32)
int(30)
bool(true)
bool(true)
