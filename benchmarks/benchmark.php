<?php
/**
 * ClickHouse PHP Extension - Comprehensive Performance Benchmarks
 *
 * This script benchmarks various aspects of the ClickHouse PHP extension:
 * - Connection performance
 * - Query performance with varying row counts
 * - Insert performance
 * - Iterator vs Array comparison
 * - Compression comparison
 */

// Configuration
$config = [
    'host' => getenv('CLICKHOUSE_HOST') ?: 'localhost',
    'port' => (int)(getenv('CLICKHOUSE_PORT') ?: 9000),
    'user' => getenv('CLICKHOUSE_USER') ?: 'default',
    'password' => getenv('CLICKHOUSE_PASSWORD') ?: '',
    'database' => getenv('CLICKHOUSE_DATABASE') ?: 'default',
];

// Global client reference for reconnection
$GLOBALS['benchmarkConfig'] = $config;

// Create a new client connection
function createClient(): ClickHouse\Client {
    $config = $GLOBALS['benchmarkConfig'];
    return new ClickHouse\Client(
        $config['host'],
        $config['port'],
        $config['user'],
        $config['password'],
        $config['database']
    );
}

// Benchmark helper function with error handling
function benchmark(string $name, callable $callback, int $iterations = 10, bool $reconnectOnError = true): array {
    global $client;
    $times = [];
    gc_collect_cycles();

    for ($i = 0; $i < $iterations; $i++) {
        try {
            $start = microtime(true);
            $callback();
            $times[] = (microtime(true) - $start) * 1000;
        } catch (Exception $e) {
            if ($reconnectOnError && $i < $iterations - 1) {
                try {
                    $client = createClient();
                } catch (Exception $e2) {
                    // Reconnect failed
                }
            }
            continue;
        }
    }

    if (empty($times)) {
        return [
            'name' => $name,
            'avg' => 0,
            'min' => 0,
            'max' => 0,
            'iterations' => 0,
            'error' => true,
        ];
    }

    return [
        'name' => $name,
        'avg' => array_sum($times) / count($times),
        'min' => min($times),
        'max' => max($times),
        'iterations' => count($times),
        'error' => false,
    ];
}

// Safe benchmark that creates a fresh connection for each iteration
function safeBenchmark(string $name, callable $callback, int $iterations = 10): array {
    $times = [];
    gc_collect_cycles();

    for ($i = 0; $i < $iterations; $i++) {
        try {
            $client = createClient();
            $start = microtime(true);
            $callback($client);
            $times[] = (microtime(true) - $start) * 1000;
            $client->close();
        } catch (Exception $e) {
            continue;
        }
    }

    if (empty($times)) {
        return [
            'name' => $name,
            'avg' => 0,
            'min' => 0,
            'max' => 0,
            'iterations' => 0,
            'error' => true,
        ];
    }

    return [
        'name' => $name,
        'avg' => array_sum($times) / count($times),
        'min' => min($times),
        'max' => max($times),
        'iterations' => count($times),
        'error' => false,
    ];
}

// Memory benchmark helper
function memoryBenchmark(string $name, callable $callback): array {
    gc_collect_cycles();
    $memStart = memory_get_usage(true);
    $peakBefore = memory_get_peak_usage(true);

    try {
        $start = microtime(true);
        $result = $callback();
        $time = (microtime(true) - $start) * 1000;

        $memEnd = memory_get_usage(true);
        $peakAfter = memory_get_peak_usage(true);

        unset($result);
        gc_collect_cycles();

        return [
            'name' => $name,
            'time_ms' => $time,
            'memory_used' => $memEnd - $memStart,
            'peak_memory' => $peakAfter,
            'peak_delta' => $peakAfter - $peakBefore,
            'error' => false,
        ];
    } catch (Exception $e) {
        return [
            'name' => $name,
            'time_ms' => 0,
            'memory_used' => 0,
            'peak_memory' => 0,
            'peak_delta' => 0,
            'error' => true,
            'error_msg' => $e->getMessage(),
        ];
    }
}

// Format bytes to human readable
function formatBytes(int $bytes): string {
    if ($bytes < 1024) return $bytes . ' B';
    if ($bytes < 1024 * 1024) return round($bytes / 1024, 2) . ' KB';
    return round($bytes / (1024 * 1024), 2) . ' MB';
}

// Print result
function printResult(array $result, ?int $rowCount = null): void {
    if (!empty($result['error'])) {
        echo "  {$result['name']}: FAILED\n";
        return;
    }

    $avgFormatted = number_format($result['avg'], 3);
    $minFormatted = number_format($result['min'], 3);
    $maxFormatted = number_format($result['max'], 3);

    if ($rowCount !== null && $result['avg'] > 0) {
        $rowsPerSec = number_format($rowCount / ($result['avg'] / 1000), 0);
        echo "  {$result['name']}: {$avgFormatted} ms (min: {$minFormatted}, max: {$maxFormatted}, {$rowsPerSec} rows/sec)\n";
    } else {
        echo "  {$result['name']}: {$avgFormatted} ms (min: {$minFormatted}, max: {$maxFormatted})\n";
    }
}

// Print memory result
function printMemoryResult(array $result, ?int $rowCount = null): void {
    if (!empty($result['error'])) {
        $msg = isset($result['error_msg']) ? " ({$result['error_msg']})" : "";
        echo "  {$result['name']}: FAILED{$msg}\n";
        return;
    }

    $timeFormatted = number_format($result['time_ms'], 3);
    $peakFormatted = formatBytes($result['peak_memory']);

    echo "  {$result['name']}: {$timeFormatted} ms, peak: {$peakFormatted}";
    if ($rowCount !== null && $result['time_ms'] > 0) {
        $rowsPerSec = number_format($rowCount / ($result['time_ms'] / 1000), 0);
        echo " ({$rowsPerSec} rows/sec)";
    }
    echo "\n";
}

// Print header
function printHeader(string $title): void {
    echo "\n{$title}\n";
    echo str_repeat('-', strlen($title)) . "\n";
}

// ============================================================================
// Main Benchmark Script
// ============================================================================

echo "=== ClickHouse PHP Extension Benchmarks ===\n";
echo "Date: " . date('Y-m-d H:i:s') . "\n";
echo "PHP Version: " . PHP_VERSION . "\n";

// Check extension
if (!extension_loaded('clickhouse')) {
    die("Error: clickhouse extension is not loaded\n");
}

echo "Extension Version: " . phpversion('clickhouse') . "\n";
echo "Host: {$config['host']}:{$config['port']}\n";

// ============================================================================
// 1. Connection Benchmarks
// ============================================================================

printHeader("1. Connection Benchmarks");

try {
    // Connect time (new connection each time)
    $connectResult = benchmark('Connect (new)', function() use ($config) {
        $client = new ClickHouse\Client(
            $config['host'],
            $config['port'],
            $config['user'],
            $config['password'],
            $config['database']
        );
        $client->close();
    }, 10, false);
    printResult($connectResult);

    // Create a connection for further tests
    $client = createClient();

    // Get server info
    $serverInfo = $client->getServerInfo();
    echo "  Server: {$serverInfo['name']} {$serverInfo['version_major']}.{$serverInfo['version_minor']}.{$serverInfo['version_patch']}\n";

    // Ping time
    $pingResult = benchmark('Ping', function() use ($client) {
        $client->ping();
    }, 50, false);
    printResult($pingResult);

    // Reconnect time
    $reconnectResult = benchmark('Reconnect', function() use ($client) {
        $client->reconnect();
    }, 10, false);
    printResult($reconnectResult);

    // Connection reuse vs new (persistent-like simulation)
    $reuseResult = safeBenchmark('Query (reused conn)', function($c) {
        $c->query("SELECT 1");
    }, 30);
    printResult($reuseResult);

    $newConnResult = benchmark('Query (new conn each)', function() use ($config) {
        $c = new ClickHouse\Client(
            $config['host'],
            $config['port'],
            $config['user'],
            $config['password'],
            $config['database']
        );
        $c->query("SELECT 1");
        $c->close();
    }, 10, false);
    printResult($newConnResult);

} catch (Exception $e) {
    echo "Connection error: " . $e->getMessage() . "\n";
    exit(1);
}

// ============================================================================
// 2. Query Benchmarks
// ============================================================================

printHeader("2. Query Benchmarks");

// Simple SELECT with fresh connection
$select1Result = safeBenchmark('SELECT 1', function($c) {
    $c->query("SELECT 1");
}, 50);
printResult($select1Result);

// SELECT with varying row counts - use fresh connections
$rowCounts = [100, 1000, 5000];

foreach ($rowCounts as $rowCount) {
    $result = safeBenchmark("SELECT {$rowCount} rows", function($c) use ($rowCount) {
        $c->query("SELECT number FROM system.numbers LIMIT {$rowCount}");
    }, 10);
    printResult($result, $rowCount);
}

// Query with different column types
printHeader("2.1 Query with Different Column Types");

// Int columns
$intResult = safeBenchmark('Int columns (1000 rows)', function($c) {
    $c->query("SELECT
        toInt8(number % 100) as i8,
        toInt16(number % 10000) as i16,
        toInt32(number) as i32,
        toInt64(number) as i64,
        toUInt64(number * 2) as u64
    FROM system.numbers LIMIT 1000");
}, 10);
printResult($intResult, 1000);

// String columns
$stringResult = safeBenchmark('String columns (1000 rows)', function($c) {
    $c->query("SELECT
        toString(number) as str
    FROM system.numbers LIMIT 1000");
}, 10);
printResult($stringResult, 1000);

// Date columns
$dateResult = safeBenchmark('Date/DateTime columns (1000 rows)', function($c) {
    $c->query("SELECT
        toDate('2024-01-01') + number as d,
        toDateTime('2024-01-01 00:00:00') + number as dt
    FROM system.numbers LIMIT 1000");
}, 10);
printResult($dateResult, 1000);

// Float columns
$floatResult = safeBenchmark('Float columns (1000 rows)', function($c) {
    $c->query("SELECT
        toFloat32(number / 100.0) as f32,
        toFloat64(number / 1000.0) as f64
    FROM system.numbers LIMIT 1000");
}, 10);
printResult($floatResult, 1000);

// Nullable columns
$nullableResult = safeBenchmark('Nullable columns (1000 rows)', function($c) {
    $c->query("SELECT
        if(number % 2 = 0, number, NULL) as nullable_int,
        if(number % 3 = 0, toString(number), NULL) as nullable_str
    FROM system.numbers LIMIT 1000");
}, 10);
printResult($nullableResult, 1000);

// UUID columns
$uuidResult = safeBenchmark('UUID columns (1000 rows)', function($c) {
    $c->query("SELECT
        generateUUIDv4() as uuid
    FROM system.numbers LIMIT 1000");
}, 10);
printResult($uuidResult, 1000);

// ============================================================================
// 3. Insert Benchmarks
// ============================================================================

printHeader("3. Insert Benchmarks");

// Create test table
$client = createClient();
try {
    $client->execute("DROP TABLE IF EXISTS benchmark_test");
    $client->execute("CREATE TABLE benchmark_test (
        id UInt64,
        name String,
        value Float64,
        created Date
    ) ENGINE = MergeTree() ORDER BY id");

    echo "  Test table created\n";
} catch (Exception $e) {
    echo "  Warning: Could not create test table: " . $e->getMessage() . "\n";
}
$client->close();

// Single row insert - proper format: insert($table, $columns, $rows)
// $columns = array of column names
// $rows = array of arrays with values indexed by position
$singleInsertResult = safeBenchmark('Insert 1 row', function($c) {
    $c->insert('benchmark_test',
        ['id', 'name', 'value', 'created'],
        [[rand(1, 1000000), 'test', 3.14, '2024-01-01']]
    );
}, 20);
printResult($singleInsertResult, 1);

// Batch inserts
$batchSizes = [100, 1000, 5000];

foreach ($batchSizes as $batchSize) {
    $rows = [];
    $baseId = rand(1, 1000000);
    for ($i = 0; $i < $batchSize; $i++) {
        $rows[] = [$baseId + $i, "test_name_{$i}", $i / 100.0, '2024-01-01'];
    }

    $batchResult = safeBenchmark("Insert {$batchSize} rows", function($c) use ($rows) {
        $c->insert('benchmark_test',
            ['id', 'name', 'value', 'created'],
            $rows
        );
    }, 5);
    printResult($batchResult, $batchSize);
}

// Insert with different data types
printHeader("3.1 Insert with Different Data Types");

// Create complex table
$client = createClient();
try {
    $client->execute("DROP TABLE IF EXISTS benchmark_complex");
    $client->execute("CREATE TABLE benchmark_complex (
        id UInt64,
        int8_col Int8,
        int64_col Int64,
        float_col Float64,
        string_col String,
        date_col Date,
        datetime_col DateTime,
        nullable_col Nullable(String)
    ) ENGINE = MergeTree() ORDER BY id");
} catch (Exception $e) {
    echo "  Warning: Could not create complex table: " . $e->getMessage() . "\n";
}
$client->close();

// Complex data insert
$complexRows = [];
$baseId = rand(1, 1000000);
for ($i = 0; $i < 1000; $i++) {
    $complexRows[] = [
        $baseId + $i,                                       // id
        $i % 127,                                            // int8_col
        $i * 1000000,                                        // int64_col
        $i / 3.14159,                                        // float_col
        "A fairly long string with some content {$i}",       // string_col
        '2024-01-01',                                        // date_col
        '2024-01-01 12:00:00',                               // datetime_col
        $i % 2 == 0 ? "value_{$i}" : null                    // nullable_col
    ];
}

$complexInsertResult = safeBenchmark('Insert complex types (1000 rows)', function($c) use ($complexRows) {
    $c->insert('benchmark_complex',
        ['id', 'int8_col', 'int64_col', 'float_col', 'string_col', 'date_col', 'datetime_col', 'nullable_col'],
        $complexRows
    );
}, 5);
printResult($complexInsertResult, 1000);

// ============================================================================
// 4. Iterator vs Array Comparison
// ============================================================================

printHeader("4. Iterator vs Array Comparison");

echo "  Comparing query() (loads all) vs queryIterator() (streaming)\n";

$memoryRowCounts = [500, 1000];

foreach ($memoryRowCounts as $rowCount) {
    echo "\n  === {$rowCount} rows ===\n";

    // Force garbage collection
    gc_collect_cycles();
    gc_disable();

    // query() - loads all into memory (simpler query to avoid protocol issues)
    $queryMemResult = memoryBenchmark("query() {$rowCount} rows", function() use ($rowCount) {
        $c = createClient();
        $result = $c->query("SELECT number FROM system.numbers LIMIT {$rowCount}");
        $count = 0;
        foreach ($result as $row) {
            $count++;
        }
        $c->close();
        return $count;
    });
    printMemoryResult($queryMemResult, $rowCount);

    gc_collect_cycles();

    // queryIterator() - streaming
    $iteratorMemResult = memoryBenchmark("queryIterator() {$rowCount} rows", function() use ($rowCount) {
        $c = createClient();
        $iterator = $c->queryIterator("SELECT number FROM system.numbers LIMIT {$rowCount}");
        $count = 0;
        foreach ($iterator as $row) {
            $count++;
        }
        $c->close();
        return $count;
    });
    printMemoryResult($iteratorMemResult, $rowCount);

    gc_enable();
}

// ============================================================================
// 5. Compression Comparison
// ============================================================================

printHeader("5. Compression Comparison");

$compressionMethods = [
    0 => 'None',
    1 => 'LZ4',
    2 => 'ZSTD'
];

// Test with a moderate result set
$compressionRowCount = 1000;

echo "  Query benchmark ({$compressionRowCount} rows)\n";

foreach ($compressionMethods as $method => $name) {
    $result = safeBenchmark("Query with {$name}", function($c) use ($compressionRowCount, $method) {
        $c->setCompression($method);
        $c->query("SELECT number FROM system.numbers LIMIT {$compressionRowCount}");
    }, 5);
    if ($result['error']) {
        echo "  {$name} compression not available or failed\n";
    } else {
        printResult($result, $compressionRowCount);
    }
}

// Insert compression comparison
echo "\n  Insert benchmark (5000 rows)\n";

// Prepare data for insert test
$insertRows = [];
$baseId = rand(1, 10000000);
for ($i = 0; $i < 5000; $i++) {
    $insertRows[] = [$baseId + $i, str_repeat('x', 100) . "_{$i}", $i / 100.0, '2024-01-01'];
}

foreach ($compressionMethods as $method => $name) {
    $result = safeBenchmark("Insert with {$name}", function($c) use ($insertRows, $method) {
        $c->setCompression($method);
        $c->insert('benchmark_test',
            ['id', 'name', 'value', 'created'],
            $insertRows
        );
    }, 3);
    if ($result['error']) {
        echo "  {$name} compression not available or failed\n";
    } else {
        printResult($result, 5000);
    }
}

// ============================================================================
// 6. Advanced Benchmarks
// ============================================================================

printHeader("6. Advanced Benchmarks");

// Prepared statement benchmark
echo "  Prepared Statements\n";

$prepareResult = safeBenchmark('Prepare + Execute (10 times)', function($c) {
    $stmt = $c->prepare("SELECT number FROM system.numbers WHERE number > {num:UInt64} LIMIT 100");
    for ($i = 0; $i < 10; $i++) {
        $stmt->bind('num', $i * 100, 'UInt64');
        $stmt->fetchAll();
    }
}, 5);
printResult($prepareResult);

// Execute (DDL) benchmark
$executeResult = safeBenchmark('Execute DDL', function($c) {
    $c->execute("SELECT 1"); // Simple execute
}, 30);
printResult($executeResult);

// Multiple queries in sequence
$sequenceResult = safeBenchmark('10 sequential queries', function($c) {
    for ($i = 0; $i < 10; $i++) {
        $c->query("SELECT {$i}");
    }
}, 10);
printResult($sequenceResult);

// ============================================================================
// Summary
// ============================================================================

printHeader("7. Summary Statistics");

// Clean up
$client = createClient();
try {
    $client->execute("DROP TABLE IF EXISTS benchmark_test");
    $client->execute("DROP TABLE IF EXISTS benchmark_complex");
    echo "  Test tables cleaned up\n";
} catch (Exception $e) {
    echo "  Warning: Could not clean up: " . $e->getMessage() . "\n";
}

// Calculate and display throughput estimates
echo "\n  Estimated throughput:\n";

// Query throughput (based on SELECT 1 test)
if (!$select1Result['error'] && $select1Result['avg'] > 0) {
    $queryThroughput = 1000 / $select1Result['avg'];
    echo "    Simple query: " . number_format($queryThroughput, 0) . " queries/sec\n";
}

// Ping throughput
if (!$pingResult['error'] && $pingResult['avg'] > 0) {
    $pingThroughput = 1000 / $pingResult['avg'];
    echo "    Ping: " . number_format($pingThroughput, 0) . " pings/sec\n";
}

echo "\n  Memory usage:\n";
echo "    Peak memory: " . formatBytes(memory_get_peak_usage(true)) . "\n";
echo "    Current memory: " . formatBytes(memory_get_usage(true)) . "\n";

$client->close();

echo "\n=== Benchmark Complete ===\n";
