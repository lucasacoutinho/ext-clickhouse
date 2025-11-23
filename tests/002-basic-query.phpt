--TEST--
ClickHouse: Basic query with scalar types
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

$result = $client->query("SELECT 1 as num, 'hello' as str, 3.14 as flt");

var_dump(count($result));
var_dump($result[0]['num']);
var_dump($result[0]['str']);
var_dump($result[0]['flt']);

echo "OK\n";
?>
--EXPECT--
int(1)
int(1)
string(5) "hello"
float(3.14)
OK
