--TEST--
ClickHouse: Map types
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

// Test Map(String, Int64)
$result = $client->query("SELECT map('a', 1, 'b', 2) as m");
var_dump(is_array($result[0]['m']));
var_dump($result[0]['m']['a']);
var_dump($result[0]['m']['b']);

echo "OK\n";
?>
--EXPECT--
bool(true)
int(1)
int(2)
OK
