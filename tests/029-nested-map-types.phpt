--TEST--
ClickHouse: Nested Map types
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

// Test simple Map
$result = $client->query("SELECT map('name', 'John', 'city', 'NYC') as m");
var_dump(is_array($result[0]['m']));
var_dump($result[0]['m']['name'] === 'John');
var_dump($result[0]['m']['city'] === 'NYC');

// Test nested Map
$result = $client->query("SELECT map('user', map('name', 'Alice', 'age', '25')) as nested");
var_dump(is_array($result[0]['nested']));
var_dump(is_array($result[0]['nested']['user']));
var_dump($result[0]['nested']['user']['name'] === 'Alice');
var_dump($result[0]['nested']['user']['age'] === '25');

// Test nested Tuple with arrays
$result = $client->query("SELECT tuple(1, tuple(2, 3), [4, 5, 6]) as nested_tuple");
var_dump(is_array($result[0]['nested_tuple']));
var_dump($result[0]['nested_tuple'][0] === 1);
var_dump(is_array($result[0]['nested_tuple'][1]));
var_dump($result[0]['nested_tuple'][1][0] === 2);
var_dump($result[0]['nested_tuple'][1][1] === 3);
var_dump(is_array($result[0]['nested_tuple'][2]));
var_dump(count($result[0]['nested_tuple'][2]) === 3);

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
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
OK
