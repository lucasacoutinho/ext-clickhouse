--TEST--
Client::select() type mapping for all basic types
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php
require __DIR__ . '/clickhouse_test.inc';
clickhouse_test_skip();
?>
--FILE--
<?php
require __DIR__ . '/clickhouse_test.inc';

$client = clickhouse_test_client();

$rows = $client->select("
    SELECT
        toUInt8(255) AS u8,
        toInt8(-128) AS i8,
        toUInt64(18446744073709551615) AS u64_big,
        toFloat32(2.5) AS f32,
        toString('hello') AS s,
        toDate('2024-01-15') AS d,
        toDateTime('2024-01-15 10:30:00') AS dt,
        toNullable(toUInt32(42)) AS nullable_val,
        toNullable(CAST(NULL AS Nullable(UInt32))) AS nullable_null
");

$row = $rows[0];

// Integer types
var_dump($row['u8']);
var_dump($row['i8']);

// UInt64 > PHP_INT_MAX → string
var_dump(is_string($row['u64_big']));

// Float
var_dump($row['f32']);

// String
var_dump($row['s']);

// Date → 'Y-m-d' string
var_dump($row['d']);

// DateTime → unix timestamp int
var_dump(is_int($row['dt']));

// Nullable(UInt32) with value
var_dump($row['nullable_val']);

// Nullable(UInt32) null
var_dump($row['nullable_null']);

echo "OK\n";
?>
--EXPECTF--
int(255)
int(-128)
bool(true)
float(2.5)
string(5) "hello"
string(10) "2024-01-15"
bool(true)
int(42)
NULL
OK
