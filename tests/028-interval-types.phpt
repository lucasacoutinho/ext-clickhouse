--TEST--
ClickHouse: Interval types
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

// Test various Interval types
$result = $client->query("
    SELECT
        toIntervalSecond(30) as sec,
        toIntervalMinute(5) as min,
        toIntervalHour(2) as hour,
        toIntervalDay(7) as day,
        toIntervalWeek(4) as week,
        toIntervalMonth(6) as month,
        toIntervalQuarter(2) as quarter,
        toIntervalYear(1) as year
");

// Intervals are returned as int64 values
var_dump($result[0]['sec'] === 30);
var_dump($result[0]['min'] === 5);
var_dump($result[0]['hour'] === 2);
var_dump($result[0]['day'] === 7);
var_dump($result[0]['week'] === 4);
var_dump($result[0]['month'] === 6);
var_dump($result[0]['quarter'] === 2);
var_dump($result[0]['year'] === 1);

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
