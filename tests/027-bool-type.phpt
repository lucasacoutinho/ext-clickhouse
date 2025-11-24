--TEST--
ClickHouse: Bool type returns PHP bool
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

// Test Bool type returns actual PHP bool
$result = $client->query("SELECT true as t, false as f, toBool(1) as b1, toBool(0) as b0");

// Verify types are bool, not int
var_dump(is_bool($result[0]['t']));
var_dump(is_bool($result[0]['f']));
var_dump(is_bool($result[0]['b1']));
var_dump(is_bool($result[0]['b0']));

// Verify values
var_dump($result[0]['t'] === true);
var_dump($result[0]['f'] === false);
var_dump($result[0]['b1'] === true);
var_dump($result[0]['b0'] === false);

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
