--TEST--
ClickHouse: Nullable types
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

// Test Nullable with value
$result = $client->query("SELECT toNullable(42) as num");
var_dump($result[0]['num']);

// Test Nullable with NULL
$result = $client->query("SELECT NULL as n");
var_dump($result[0]['n']);

echo "OK\n";
?>
--EXPECT--
int(42)
NULL
OK
