--TEST--
ClickHouse: Client::fromDSN() factory method
--SKIPIF--
<?php
if (!extension_loaded('clickhouse')) die('skip clickhouse extension not loaded');
if (!getenv('CLICKHOUSE_HOST')) die('skip CLICKHOUSE_HOST not set');
?>
--FILE--
<?php
$host = getenv('CLICKHOUSE_HOST') ?: 'clickhouse';
$port = (int)(getenv('CLICKHOUSE_PORT') ?: 9000);

// Test basic DSN
$dsn = "clickhouse://{$host}:{$port}";
$client = ClickHouse\Client::fromDSN($dsn);
var_dump($client->ping());

// Test with database
$dsn = "clickhouse://{$host}:{$port}/default";
$client = ClickHouse\Client::fromDSN($dsn);
var_dump($client->ping());

// Test with user
$dsn = "clickhouse://default@{$host}:{$port}/default";
$client = ClickHouse\Client::fromDSN($dsn);
var_dump($client->ping());

// Test short scheme (ch://)
$dsn = "ch://{$host}:{$port}";
$client = ClickHouse\Client::fromDSN($dsn);
var_dump($client->ping());

// Test with compression option
$dsn = "clickhouse://{$host}:{$port}?compression=lz4";
$client = ClickHouse\Client::fromDSN($dsn);
var_dump($client->ping());
var_dump($client->getCompression() === 1);

// Test query via DSN-created client
$result = $client->query("SELECT 42 as answer");
var_dump($result[0]['answer'] === 42);

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
OK
