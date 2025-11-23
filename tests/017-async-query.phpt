--TEST--
ClickHouse: Async query execution
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

// Test 1: Basic async query
$async = $client->queryAsync("SELECT 1 as num, 'hello' as str");
var_dump($async instanceof ClickHouse\AsyncResult);

// Test 2: isReady returns bool
var_dump(is_bool($async->isReady()));

// Test 3: poll returns bool
var_dump(is_bool($async->poll(100)));

// Test 4: wait returns array
$result = $async->wait();
var_dump(is_array($result));
var_dump($result[0]['num'] === 1);
var_dump($result[0]['str'] === 'hello');

// Test 5: getResult returns cached result
$cached = $async->getResult();
var_dump($cached === $result);

// Test 6: Multiple concurrent async queries
$async1 = $client->queryAsync("SELECT 10 as a");
$async2 = $client->queryAsync("SELECT 20 as b");
$async3 = $client->queryAsync("SELECT 30 as c");

$r1 = $async1->wait();
$r2 = $async2->wait();
$r3 = $async3->wait();

var_dump($r1[0]['a'] === 10);
var_dump($r2[0]['b'] === 20);
var_dump($r3[0]['c'] === 30);

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
OK
