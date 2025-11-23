--TEST--
ClickHouse: Enum8 and Enum16 string mapping
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

// Test Enum8
$result = $client->query("SELECT CAST(1 AS Enum8('hello' = 1, 'world' = 2)) AS e");
var_dump($result[0]['e']);

// Test Enum16
$result = $client->query("SELECT CAST(200 AS Enum16('first' = 100, 'second' = 200)) AS e");
var_dump($result[0]['e']);

// Test Enum8 with negative value
$result = $client->query("SELECT CAST(-1 AS Enum8('neg' = -1, 'pos' = 1)) AS e");
var_dump($result[0]['e']);

echo "OK\n";
?>
--EXPECT--
string(5) "hello"
string(6) "second"
string(3) "neg"
OK
