--TEST--
ClickHouse: Prepared statements with parameters
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

// Test prepared statement with integer parameter
// Note: Parameters use {name:Type} format
$stmt = $client->prepare("SELECT {num:Int32} as value");
$stmt->bind("num", 42, "Int32");
$result = $stmt->execute();
var_dump($result[0]['value'] === 42);

// Test prepared statement with string parameter
$stmt = $client->prepare("SELECT {str:String} as value");
$stmt->bind("str", "hello", "String");
$result = $stmt->execute();
var_dump($result[0]['value'] === 'hello');

// Test prepared statement with multiple parameters
$stmt = $client->prepare("SELECT {a:Int32} + {b:Int32} as sum");
$stmt->bind("a", 10, "Int32");
$stmt->bind("b", 20, "Int32");
$result = $stmt->execute();
var_dump($result[0]['sum'] === 30);

echo "OK\n";
?>
--EXPECT--
bool(true)
bool(true)
bool(true)
OK
