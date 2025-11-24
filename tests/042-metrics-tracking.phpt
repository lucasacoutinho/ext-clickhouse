--TEST--
ClickHouse: Metrics tracking and monitoring
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

// Test 1: Metrics disabled by default
$metrics = $client->getMetrics();
echo "Metrics enabled by default: " . ($metrics['enabled'] ? 'yes' : 'no') . "\n";
echo "Initial queries_executed: " . $metrics['queries_executed'] . "\n";
echo "Initial queries_failed: " . $metrics['queries_failed'] . "\n";

// Test 2: Enable metrics
$client->enableMetrics();
$metrics = $client->getMetrics();
echo "Metrics enabled after enableMetrics(): " . ($metrics['enabled'] ? 'yes' : 'no') . "\n";

// Clean up and create test table
try {
    $client->execute("DROP TABLE IF EXISTS test_metrics_phpt");
} catch (Exception $e) {
    // Ignore
}

$client->execute("
    CREATE TABLE test_metrics_phpt (
        id UInt32,
        value String
    ) ENGINE = Memory
");

// Test 3: Track successful queries
for ($i = 0; $i < 5; $i++) {
    $client->execute("INSERT INTO test_metrics_phpt VALUES ({id}, {value})", [
        'id' => $i,
        'value' => "test_{$i}"
    ]);
}

$result = $client->query("SELECT COUNT(*) as count FROM test_metrics_phpt");

$metrics = $client->getMetrics();
echo "Queries executed (should be > 5): " . ($metrics['queries_executed'] > 5 ? 'yes' : 'no') . "\n";
echo "Queries failed: " . $metrics['queries_failed'] . "\n";
echo "Total query time > 0: " . ($metrics['total_query_time'] > 0 ? 'yes' : 'no') . "\n";
echo "Average query time > 0: " . ($metrics['avg_query_time'] > 0 ? 'yes' : 'no') . "\n";
echo "Total rows read >= 0: " . ($metrics['total_rows_read'] >= 0 ? 'yes' : 'no') . "\n";
echo "Total bytes read >= 0: " . ($metrics['total_bytes_read'] >= 0 ? 'yes' : 'no') . "\n";

// Test 4: Track failed query
try {
    $client->query("SELECT * FROM nonexistent_table_phpt");
} catch (Exception $e) {
    // Expected
}

$metrics = $client->getMetrics();
echo "Queries failed after error: " . $metrics['queries_failed'] . "\n";

// Test 5: Slow query threshold
$client->setSlowQueryThreshold(0.001); // 1ms - very low for testing
$metrics = $client->getMetrics();
echo "Slow query threshold set: " . ($metrics['slow_query_threshold'] == 0.001 ? 'yes' : 'no') . "\n";

// Execute some queries that will likely be "slow"
for ($i = 0; $i < 3; $i++) {
    $client->query("SELECT * FROM test_metrics_phpt WHERE id = {$i}");
}

$metrics = $client->getMetrics();
echo "Slow queries detected: " . ($metrics['slow_queries'] > 0 ? 'yes' : 'no') . "\n";

// Test 6: Reset metrics
$beforeReset = $client->getMetrics();
$hadQueries = $beforeReset['queries_executed'] > 0;

$client->resetMetrics();
$afterReset = $client->getMetrics();

echo "Had queries before reset: " . ($hadQueries ? 'yes' : 'no') . "\n";
echo "Queries executed after reset: " . $afterReset['queries_executed'] . "\n";
echo "Queries failed after reset: " . $afterReset['queries_failed'] . "\n";
echo "Total query time after reset: " . ($afterReset['total_query_time'] == 0.0 ? '0.0' : 'non-zero') . "\n";
echo "Slow queries after reset: " . $afterReset['slow_queries'] . "\n";

// Test 7: Disable metrics
$client->disableMetrics();
$client->query("SELECT 1");
$metrics = $client->getMetrics();
echo "Metrics enabled after disableMetrics(): " . ($metrics['enabled'] ? 'yes' : 'no') . "\n";
echo "Queries executed (should still be 0): " . $metrics['queries_executed'] . "\n";

// Test 8: Re-enable and verify tracking works again
$client->enableMetrics();
$client->resetMetrics();
$client->execute("INSERT INTO test_metrics_phpt VALUES ({id}, {value})", [
    'id' => 100,
    'value' => 'final_test'
]);
$metrics = $client->getMetrics();
echo "Tracking works after re-enable: " . ($metrics['queries_executed'] > 0 ? 'yes' : 'no') . "\n";

// Cleanup
$client->execute("DROP TABLE test_metrics_phpt");

echo "OK\n";
?>
--EXPECT--
Metrics enabled by default: no
Initial queries_executed: 0
Initial queries_failed: 0
Metrics enabled after enableMetrics(): yes
Queries executed (should be > 5): yes
Queries failed: 0
Total query time > 0: yes
Average query time > 0: yes
Total rows read >= 0: yes
Total bytes read >= 0: yes
Queries failed after error: 1
Slow query threshold set: yes
Slow queries detected: yes
Had queries before reset: yes
Queries executed after reset: 0
Queries failed after reset: 0
Total query time after reset: 0.0
Slow queries after reset: 0
Metrics enabled after disableMetrics(): no
Queries executed (should still be 0): 0
Tracking works after re-enable: yes
OK
