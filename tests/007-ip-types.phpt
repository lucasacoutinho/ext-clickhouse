--TEST--
ClickHouse: IPv4 and IPv6 types
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

// Test IPv4
$result = $client->query("SELECT toIPv4('192.168.1.1') as ip");
var_dump($result[0]['ip']);

// Test IPv6
$result = $client->query("SELECT toIPv6('::1') as ip");
var_dump(strlen($result[0]['ip']) > 0); // Just check it's non-empty

echo "OK\n";
?>
--EXPECT--
string(11) "192.168.1.1"
bool(true)
OK
