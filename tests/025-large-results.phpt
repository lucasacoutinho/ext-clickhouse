--TEST--
ClickHouse: Large result sets
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

// Test various result sizes
$sizes = [100, 1000, 5000, 10000, 50000];

foreach ($sizes as $size) {
    $r = $client->query("SELECT number FROM system.numbers LIMIT $size");
    $count = count($r);
    $pass = $count === $size;
    echo "LIMIT $size: " . ($pass ? "PASS" : "FAIL (got $count)") . "\n";

    // Verify first and last values
    if ($pass) {
        $firstOk = $r[0]['number'] === 0;
        $lastOk = $r[$size - 1]['number'] === ($size - 1);
        if (!$firstOk || !$lastOk) {
            echo "  Data integrity: FAIL\n";
        }
    }
}

// Test with multiple columns
echo "Multi-column 10000 rows: ";
$r = $client->query("SELECT number, toString(number) as str, number * 2 as doubled FROM system.numbers LIMIT 10000");
$pass = count($r) === 10000 && isset($r[0]['number']) && isset($r[0]['str']) && isset($r[0]['doubled']);
echo ($pass ? "PASS" : "FAIL") . "\n";

echo "OK\n";
?>
--EXPECT--
LIMIT 100: PASS
LIMIT 1000: PASS
LIMIT 5000: PASS
LIMIT 10000: PASS
LIMIT 50000: PASS
Multi-column 10000 rows: PASS
OK
