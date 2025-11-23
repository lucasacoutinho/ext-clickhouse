--TEST--
ClickHouse: Extreme performance test (100k+ rows)
--SKIPIF--
<?php
if (!extension_loaded('clickhouse')) die('skip clickhouse extension not loaded');
if (!getenv('CLICKHOUSE_HOST')) die('skip CLICKHOUSE_HOST not set');
if (getenv('SKIP_SLOW_TESTS')) die('skip slow tests disabled');
?>
--INI--
memory_limit=512M
--FILE--
<?php
$host = getenv('CLICKHOUSE_HOST') ?: 'clickhouse';
$port = (int)(getenv('CLICKHOUSE_PORT') ?: 9000);

$client = new ClickHouse\Client($host, $port);

// Test 100,000 rows
echo "Testing 100,000 rows: ";
$start = microtime(true);
$r = $client->query("SELECT number, toString(number) as str FROM system.numbers LIMIT 100000");
$elapsed = microtime(true) - $start;
$count = count($r);
echo ($count === 100000 ? "PASS" : "FAIL (got $count)");
echo sprintf(" (%.2fs)\n", $elapsed);

// Verify data integrity
if ($count === 100000) {
    $firstOk = $r[0]['number'] === 0;
    $lastOk = $r[99999]['number'] === 99999;
    echo "Data integrity: " . ($firstOk && $lastOk ? "PASS" : "FAIL") . "\n";
}
unset($r);

// Test 250,000 rows
echo "Testing 250,000 rows: ";
$start = microtime(true);
$r = $client->query("SELECT number FROM system.numbers LIMIT 250000");
$elapsed = microtime(true) - $start;
$count = count($r);
echo ($count === 250000 ? "PASS" : "FAIL (got $count)");
echo sprintf(" (%.2fs)\n", $elapsed);
unset($r);

echo "OK\n";
?>
--EXPECTF--
Testing 100,000 rows: PASS (%fs)
Data integrity: PASS
Testing 250,000 rows: PASS (%fs)
OK
