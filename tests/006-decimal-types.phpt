--TEST--
ClickHouse: Decimal types
--SKIPIF--
<?php
if (!extension_loaded('clickhouse')) die('skip clickhouse extension not loaded');
if (!getenv('CLICKHOUSE_HOST')) die('skip CLICKHOUSE_HOST not set');
?>
--FILE--
<?php
$host = getenv('CLICKHOUSE_HOST') ?: 'clickhouse';
$port = (int)(getenv('CLICKHOUSE_PORT') ?: 9000);

$client = new ClickHouse\Client($host, $port);

// Test Decimal32 - returns string with decimal point
$result = $client->query("SELECT toDecimal32(123.45, 2) as d32");
var_dump($result[0]['d32'] === '123.45');

// Test Decimal64
$result = $client->query("SELECT toDecimal64(12345.6789, 4) as d64");
var_dump($result[0]['d64'] === '12345.6789');

// Test Decimal128
$result = $client->query("SELECT toDecimal128(99999.99999, 5) as d128");
var_dump($result[0]['d128'] === '99999.99999');

// Test negative decimal
$result = $client->query("SELECT toDecimal64(-123.45, 2) as neg");
var_dump($result[0]['neg'] === '-123.45');

// Test small decimal (leading zeros after decimal)
$result = $client->query("SELECT toDecimal32(0.01, 2) as small");
var_dump($result[0]['small'] === '0.01');

echo "OK\n";
?>
--EXPECT--
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
OK
