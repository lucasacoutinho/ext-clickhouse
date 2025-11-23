--TEST--
ClickHouse: Reconnection support
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

// Test initial auto-reconnect state
echo "Initial auto-reconnect: ";
var_dump($client->getAutoReconnect() === false);

// Test setAutoReconnect
$client->setAutoReconnect(true);
echo "After setAutoReconnect(true): ";
var_dump($client->getAutoReconnect() === true);

$client->setAutoReconnect(false);
echo "After setAutoReconnect(false): ";
var_dump($client->getAutoReconnect() === false);

// Test manual reconnect
echo "Manual reconnect(): ";
$result = $client->reconnect();
var_dump($result === true);

// Test connection still works after reconnect
echo "Query after reconnect: ";
$r = $client->query("SELECT 1 as num");
var_dump($r[0]['num'] === 1);

// Test isConnected after reconnect
echo "isConnected after reconnect: ";
var_dump($client->isConnected() === true);

// Test ping after reconnect
echo "Ping after reconnect: ";
var_dump($client->ping() === true);

echo "OK\n";
?>
--EXPECT--
Initial auto-reconnect: bool(true)
After setAutoReconnect(true): bool(true)
After setAutoReconnect(false): bool(true)
Manual reconnect(): bool(true)
Query after reconnect: bool(true)
isConnected after reconnect: bool(true)
Ping after reconnect: bool(true)
OK
