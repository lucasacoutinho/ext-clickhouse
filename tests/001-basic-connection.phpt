--TEST--
ClickHouse: Basic connection and ping
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

// Test ping
$ping = $client->ping();
var_dump($ping);

// Test isConnected
$connected = $client->isConnected();
var_dump($connected);

echo "OK\n";
?>
--EXPECT--
bool(true)
bool(true)
OK
