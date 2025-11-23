--TEST--
ClickHouse: getServerInfo and connection methods
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

// Test isConnected()
var_dump($client->isConnected());

// Test ping()
var_dump($client->ping());

// Test getServerInfo()
$info = $client->getServerInfo();
var_dump(is_array($info));
var_dump(isset($info['name']));
var_dump(isset($info['version_major']));
var_dump(isset($info['version_minor']));
var_dump(isset($info['revision']));
var_dump($info['name'] === 'ClickHouse');

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
OK
