--TEST--
ClickHouse: LowCardinality types
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

// Test LowCardinality(String)
$result = $client->query("SELECT toLowCardinality('hello') as lc");
var_dump($result[0]['lc'] === 'hello');

// Test LowCardinality(Nullable(String)) with value
$result = $client->query("SELECT toLowCardinality(toNullable('test')) as lc");
var_dump($result[0]['lc'] === 'test');

// Test LowCardinality(Nullable(String)) with NULL
$result = $client->query("SELECT toLowCardinality(toNullable(NULL)::Nullable(String)) as lc");
var_dump($result[0]['lc'] === null);

// Test LowCardinality with deduplication (multiple same values)
$result = $client->query("SELECT toLowCardinality(s) as lc FROM (SELECT arrayJoin(['a', 'b', 'a', 'c', 'b']) as s)");
var_dump(count($result) === 5);
var_dump($result[0]['lc'] === 'a');
var_dump($result[2]['lc'] === 'a');

echo "OK\n";
?>
--EXPECT--
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
OK
