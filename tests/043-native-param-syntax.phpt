--TEST--
ClickHouse: Native parameter syntax {param:Type} support
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

// Test {param:Type} syntax - native ClickHouse format
$stmt = $client->prepare("SELECT {num:Int32} as value");
$stmt->bind("num", 42, "Int32");
$result = $stmt->execute();
var_dump($result[0]['value'] === 42);

// Test {param} syntax without type
$stmt = $client->prepare("SELECT {str} as value");
$stmt->bind("str", "hello", "String");
$result = $stmt->execute();
var_dump($result[0]['value'] === 'hello');

// Test multiple parameters
$stmt = $client->prepare("SELECT {a:Int32} + {b:Int32} as sum, {msg:String} as message");
$stmt->bind("a", 10, "Int32");
$stmt->bind("b", 20, "Int32");
$stmt->bind("msg", "result", "String");
$result = $stmt->execute();
var_dump($result[0]['sum'] === 30);
var_dump($result[0]['message'] === 'result');

echo "OK\n";
?>
--EXPECT--
bool(true)
bool(true)
bool(true)
bool(true)
OK