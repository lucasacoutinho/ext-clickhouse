#!/usr/bin/env php
<?php
/**
 * Extreme performance benchmark for ClickHouse native extension
 * Tests with 1M+ rows - run manually as this requires significant memory/time
 *
 * Usage: php extreme_benchmark.php [host] [port]
 */

if (!extension_loaded('clickhouse')) {
    die("Error: clickhouse extension not loaded\n");
}

$host = $argv[1] ?? getenv('CLICKHOUSE_HOST') ?: 'clickhouse';
$port = (int)($argv[2] ?? getenv('CLICKHOUSE_PORT') ?: 9000);

echo "ClickHouse Extreme Performance Benchmark\n";
echo "========================================\n";
echo "Host: $host:$port\n";
echo "PHP Memory Limit: " . ini_get('memory_limit') . "\n\n";

$client = new ClickHouse\Client($host, $port);

// Test configurations
$tests = [
    ['rows' => 100000, 'description' => '100K rows'],
    ['rows' => 250000, 'description' => '250K rows'],
    ['rows' => 500000, 'description' => '500K rows'],
    ['rows' => 1000000, 'description' => '1M rows'],
    ['rows' => 2000000, 'description' => '2M rows'],
    ['rows' => 5000000, 'description' => '5M rows'],
];

echo "Single Column Tests (number only)\n";
echo "---------------------------------\n";

foreach ($tests as $test) {
    $rows = $test['rows'];
    $desc = $test['description'];

    echo sprintf("%-15s: ", $desc);

    $memBefore = memory_get_usage(true);
    $start = microtime(true);

    try {
        $result = $client->query("SELECT number FROM system.numbers LIMIT $rows");
        $elapsed = microtime(true) - $start;
        $memAfter = memory_get_usage(true);
        $memUsed = ($memAfter - $memBefore) / 1024 / 1024;

        $count = count($result);
        $rowsPerSec = $count / $elapsed;

        if ($count === $rows) {
            echo sprintf("PASS - %.2fs (%.0f rows/s, %.1f MB)\n", $elapsed, $rowsPerSec, $memUsed);
        } else {
            echo sprintf("FAIL - got %d rows (expected %d)\n", $count, $rows);
        }

        unset($result);
    } catch (Exception $e) {
        echo "FAIL - " . $e->getMessage() . "\n";
    }

    // Force garbage collection
    gc_collect_cycles();
}

echo "\nMulti-Column Tests (number, string, doubled)\n";
echo "--------------------------------------------\n";

$multiTests = [
    ['rows' => 100000, 'description' => '100K rows'],
    ['rows' => 250000, 'description' => '250K rows'],
    ['rows' => 500000, 'description' => '500K rows'],
];

foreach ($multiTests as $test) {
    $rows = $test['rows'];
    $desc = $test['description'];

    echo sprintf("%-15s: ", $desc);

    $memBefore = memory_get_usage(true);
    $start = microtime(true);

    try {
        $result = $client->query("SELECT number, toString(number) as str, number * 2 as doubled FROM system.numbers LIMIT $rows");
        $elapsed = microtime(true) - $start;
        $memAfter = memory_get_usage(true);
        $memUsed = ($memAfter - $memBefore) / 1024 / 1024;

        $count = count($result);
        $rowsPerSec = $count / $elapsed;

        if ($count === $rows) {
            echo sprintf("PASS - %.2fs (%.0f rows/s, %.1f MB)\n", $elapsed, $rowsPerSec, $memUsed);
        } else {
            echo sprintf("FAIL - got %d rows (expected %d)\n", $count, $rows);
        }

        unset($result);
    } catch (Exception $e) {
        echo "FAIL - " . $e->getMessage() . "\n";
    }

    gc_collect_cycles();
}

echo "\nData Integrity Check (1M rows)\n";
echo "------------------------------\n";
try {
    $result = $client->query("SELECT number FROM system.numbers LIMIT 1000000");
    $count = count($result);

    if ($count === 1000000) {
        $firstOk = $result[0]['number'] === 0;
        $lastOk = $result[999999]['number'] === 999999;
        $midOk = $result[500000]['number'] === 500000;

        echo "First row (0): " . ($firstOk ? "OK" : "FAIL") . "\n";
        echo "Middle row (500000): " . ($midOk ? "OK" : "FAIL") . "\n";
        echo "Last row (999999): " . ($lastOk ? "OK" : "FAIL") . "\n";
    } else {
        echo "FAIL - only got $count rows\n";
    }
} catch (Exception $e) {
    echo "FAIL - " . $e->getMessage() . "\n";
}

echo "\nPeak memory usage: " . (memory_get_peak_usage(true) / 1024 / 1024) . " MB\n";
echo "Done!\n";
