--TEST--
ClickHouse: Geo types (Point, Ring, Polygon, MultiPolygon)
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

// Test Point
$result = $client->query("SELECT (1.5, 2.5)::Point as p");
var_dump(count($result[0]['p']) === 2);
var_dump($result[0]['p'][0] === 1.5);
var_dump($result[0]['p'][1] === 2.5);

// Test Ring (array of points)
$result = $client->query("SELECT [(0.0,0.0),(1.0,0.0),(1.0,1.0)]::Ring as r");
var_dump(count($result[0]['r']) === 3);
var_dump($result[0]['r'][0][0] === 0.0);

// Test Polygon (array of rings)
$result = $client->query("SELECT [[(0.0,0.0),(1.0,0.0),(1.0,1.0),(0.0,0.0)]]::Polygon as p");
var_dump(count($result[0]['p']) === 1);
var_dump(count($result[0]['p'][0]) === 4);

// Test SimpleAggregateFunction
$result = $client->query("SELECT CAST(42 AS SimpleAggregateFunction(max, UInt64)) as v");
var_dump($result[0]['v'] === 42);

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
