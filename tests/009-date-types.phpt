--TEST--
ClickHouse: Date and DateTime types
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

// Test Date - returns formatted string YYYY-MM-DD
$result = $client->query("SELECT toDate('2024-01-15') as d");
var_dump($result[0]['d'] === '2024-01-15');

// Test Date32
$result = $client->query("SELECT toDate32('2024-01-15') as d32");
var_dump($result[0]['d32'] === '2024-01-15');

// Test DateTime - returns formatted string YYYY-MM-DD HH:MM:SS
$result = $client->query("SELECT toDateTime('2024-01-15 10:30:45') as dt");
var_dump($result[0]['dt'] === '2024-01-15 10:30:45');

// Test DateTime64 with millisecond precision
$result = $client->query("SELECT toDateTime64('2024-01-15 10:30:45.123', 3) as dt64");
var_dump($result[0]['dt64'] === '2024-01-15 10:30:45.123');

// Test DateTime64 with microsecond precision
$result = $client->query("SELECT toDateTime64('2024-01-15 10:30:45.123456', 6) as dt64");
var_dump($result[0]['dt64'] === '2024-01-15 10:30:45.123456');

echo "OK\n";
?>
--EXPECT--
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
OK
