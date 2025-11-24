--TEST--
ClickHouse: Auto-detect Decimal types
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

// Clean up
try {
    $client->execute("DROP TABLE IF EXISTS test_decimal_autodetect");
} catch (Exception $e) {}

// Create table
$client->execute("
    CREATE TABLE test_decimal_autodetect (
        id UInt32,
        price Decimal(18,2),
        rate Decimal(10,4),
        precision_val Decimal(18,6)
    ) ENGINE = Memory
");

// Test 1: Auto-detect Decimal(18,2) from currency format
$client->execute("INSERT INTO test_decimal_autodetect VALUES ({id}, {price}, {rate}, {precision_val})", [
    'id' => 1,
    'price' => '99.99',          // Auto-detect as Decimal(18,2)
    'rate' => '0.0525',          // Auto-detect as Decimal(10,4)
    'precision_val' => '999.123456'  // Auto-detect as Decimal(18,6)
]);
var_dump(true);

// Test 2: Negative decimal
$client->execute("INSERT INTO test_decimal_autodetect VALUES ({id}, {price}, {rate}, {precision_val})", [
    'id' => 2,
    'price' => '-1234.56',
    'rate' => '0.1250',
    'precision_val' => '0.000001'
]);
var_dump(true);

// Test 3: Explicit positive sign
$client->execute("INSERT INTO test_decimal_autodetect VALUES ({id}, {price}, {rate}, {precision_val})", [
    'id' => 3,
    'price' => '+123.45',
    'rate' => '0.0001',
    'precision_val' => '1.123'
]);
var_dump(true);

// Query back and verify
$result = $client->query("SELECT id, price, rate, precision_val FROM test_decimal_autodetect ORDER BY id");
var_dump(count($result) === 3);
var_dump($result[0]['id'] === 1);
var_dump(abs($result[0]['price'] - 99.99) < 0.01);
var_dump(abs($result[0]['rate'] - 0.0525) < 0.0001);
var_dump(abs($result[0]['precision_val'] - 999.123456) < 0.000001);
var_dump($result[1]['id'] === 2);
var_dump(abs($result[1]['price'] - (-1234.56)) < 0.01);
var_dump($result[2]['id'] === 3);
var_dump(abs($result[2]['price'] - 123.45) < 0.01);

// Clean up
$client->execute("DROP TABLE test_decimal_autodetect");

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
bool(true)
OK
