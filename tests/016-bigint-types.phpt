--TEST--
ClickHouse: Int128/UInt128/Int256/UInt256 types
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

// Test Int128 with varied digits
$result = $client->query("SELECT CAST('123456789012345678901234567890' as Int128) as val");
var_dump($result[0]['val'] === '123456789012345678901234567890');

// Test UInt128 max value
$result = $client->query("SELECT CAST('340282366920938463463374607431768211455' as UInt128) as val");
var_dump($result[0]['val'] === '340282366920938463463374607431768211455');

// Test UInt128 with varied digits
$result = $client->query("SELECT CAST('98765432109876543210987654321' as UInt128) as val");
var_dump($result[0]['val'] === '98765432109876543210987654321');

// Test negative Int128 with varied digits
$result = $client->query("SELECT CAST('-123456789012345678901234567890' as Int128) as val");
var_dump($result[0]['val'] === '-123456789012345678901234567890');

// Test Int256 with many varied digits
$result = $client->query("SELECT CAST('12345678901234567890123456789012345678901234567890' as Int256) as val");
var_dump($result[0]['val'] === '12345678901234567890123456789012345678901234567890');

// Test UInt256 with varied digits
$result = $client->query("SELECT CAST('99999999999999999999999999999999999999999999999999' as UInt256) as val");
var_dump($result[0]['val'] === '99999999999999999999999999999999999999999999999999');

// Test negative Int256 with varied digits
$result = $client->query("SELECT CAST('-98765432109876543210987654321098765432109876543210' as Int256) as val");
var_dump($result[0]['val'] === '-98765432109876543210987654321098765432109876543210');

// Test small values (edge cases)
$result = $client->query("SELECT CAST('42' as Int128) as val");
var_dump($result[0]['val'] === '42');

$result = $client->query("SELECT CAST('0' as UInt256) as val");
var_dump($result[0]['val'] === '0');

$result = $client->query("SELECT CAST('1' as Int256) as val");
var_dump($result[0]['val'] === '1');

// Test numbers with mixed patterns
$result = $client->query("SELECT CAST('111222333444555666777888999' as UInt128) as val");
var_dump($result[0]['val'] === '111222333444555666777888999');

echo "OK\n";
?>
--EXPECT--
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
OK
