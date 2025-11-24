--TEST--
ClickHouse: queryWithMeta returns totals, extremes, and profile info
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

// Test queryWithMeta returns expected structure
$result = $client->queryWithMeta("SELECT number FROM numbers(5)");
var_dump(isset($result['data']));
var_dump(array_key_exists('totals', $result));
var_dump(array_key_exists('extremes', $result));
var_dump(isset($result['progress']));
var_dump(isset($result['profile']));
var_dump(count($result['data']) === 5);

// Test WITH TOTALS
$result = $client->queryWithMeta("SELECT number % 2 as grp, count() as cnt FROM numbers(10) GROUP BY grp WITH TOTALS");
var_dump(isset($result['data']));
var_dump(count($result['data']) === 2);  // Two groups: 0 and 1
var_dump($result['totals'] !== null);
var_dump(count($result['totals']) === 1);  // One totals row

// Verify totals values (total count = 10)
var_dump($result['totals'][0]['cnt'] === 10);

// Test profile info
var_dump(isset($result['profile']['rows']));
var_dump(isset($result['profile']['bytes']));
var_dump(isset($result['profile']['blocks']));

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
