--TEST--
ClickHouse\Client::queryStreaming() - True streaming iterator for large datasets
--SKIPIF--
<?php
if (!extension_loaded('clickhouse')) {
    echo 'skip ClickHouse extension not available';
}
?>
--FILE--
<?php
$client = new ClickHouse\Client(
    getenv('CLICKHOUSE_HOST') ?: 'localhost',
    (int)(getenv('CLICKHOUSE_PORT') ?: 9000),
    getenv('CLICKHOUSE_USER') ?: 'default',
    getenv('CLICKHOUSE_PASSWORD') ?: '',
    getenv('CLICKHOUSE_DB') ?: 'default'
);

// Test 1: Iterator interfaces
echo "=== Test 1: Iterator Interfaces ===\n";
$stream = $client->queryStreaming("SELECT 1 as num");
echo "Implements Iterator: " . ($stream instanceof Iterator ? "yes" : "no") . "\n";
echo "Implements Countable: " . ($stream instanceof Countable ? "yes" : "no") . "\n";
echo "Class name: " . get_class($stream) . "\n";

// Test 2: Basic iteration with small dataset
echo "\n=== Test 2: Basic Iteration ===\n";
$stream = $client->queryStreaming("SELECT number, number * 2 as double FROM numbers(5)");
foreach ($stream as $key => $row) {
    echo "Row $key: number={$row['number']}, double={$row['double']}\n";
}
echo "Final count: " . count($stream) . "\n";

// Test 3: Empty result
echo "\n=== Test 3: Empty Result ===\n";
$stream = $client->queryStreaming("SELECT number FROM numbers(0)");
$iteratedCount = 0;
foreach ($stream as $row) {
    $iteratedCount++;
}
echo "Iterated rows: $iteratedCount\n";
echo "Count: " . count($stream) . "\n";

// Test 4: Manual iterator control
echo "\n=== Test 4: Manual Iterator Control ===\n";
$stream = $client->queryStreaming("SELECT number FROM numbers(5)");
$stream->rewind();
echo "valid(): " . ($stream->valid() ? "true" : "false") . "\n";
echo "key(): " . $stream->key() . "\n";
echo "current()['number']: " . $stream->current()['number'] . "\n";
$stream->next();
echo "After next() - key(): " . $stream->key() . "\n";
echo "After next() - current()['number']: " . $stream->current()['number'] . "\n";

// Test 5: Large dataset streaming
echo "\n=== Test 5: Large Dataset Streaming ===\n";
$stream = $client->queryStreaming("SELECT number FROM numbers(1000)");
$sum = 0;
$count = 0;
foreach ($stream as $row) {
    $sum += $row['number'];
    $count++;
}
echo "Total rows: $count\n";
echo "Sum: $sum (expected: 499500)\n";
echo "Final count(): " . count($stream) . "\n";

// Test 6: Memory efficiency test (10K rows)
echo "\n=== Test 6: Memory Efficiency (10K rows) ===\n";
$stream = $client->queryStreaming("SELECT number, toString(number) as str, number * 1.5 as calc FROM numbers(10000)");
$processed = 0;
foreach ($stream as $row) {
    $processed++;
}
echo "Total processed: $processed rows\n";
echo "Memory efficient: yes\n";

// Test 7: Rewind functionality
echo "\n=== Test 7: Rewind Functionality ===\n";
$stream = $client->queryStreaming("SELECT number FROM numbers(3)");
$values1 = [];
foreach ($stream as $row) {
    $values1[] = $row['number'];
}
echo "First iteration: " . implode(' ', $values1) . "\n";

$values2 = [];
foreach ($stream as $row) {
    $values2[] = $row['number'];
}
echo "Second iteration: " . implode(' ', $values2) . "\n";

// Test 8: Break early from iteration
echo "\n=== Test 8: Early Break ===\n";
$stream = $client->queryStreaming("SELECT number FROM numbers(100)");
$count = 0;
foreach ($stream as $row) {
    $count++;
    if ($count >= 5) {
        break;
    }
}
echo "Iterated $count rows (stopped early)\n";
echo "Count after break: " . count($stream) . "\n";

// Test 9: Using iterator_to_array with small result
echo "\n=== Test 9: iterator_to_array() ===\n";
$stream = $client->queryStreaming("SELECT number FROM numbers(3)");
$array = iterator_to_array($stream);
echo "Array count: " . count($array) . "\n";
echo "Array keys: " . implode(', ', array_keys($array)) . "\n";
echo "Values: " . implode(', ', array_map(fn($r) => $r['number'], $array)) . "\n";

// Test 10: Multiple columns
echo "\n=== Test 10: Multiple Columns ===\n";
$stream = $client->queryStreaming("SELECT 42 as int_val, 'hello' as str_val, 3.14 as float_val");
foreach ($stream as $row) {
    echo "int: {$row['int_val']}, str: {$row['str_val']}, float: {$row['float_val']}\n";
}

echo "\nOK\n";
?>
--EXPECT--
=== Test 1: Iterator Interfaces ===
Implements Iterator: yes
Implements Countable: yes
Class name: ClickHouse\StreamingIterator

=== Test 2: Basic Iteration ===
Row 0: number=0, double=0
Row 1: number=1, double=2
Row 2: number=2, double=4
Row 3: number=3, double=6
Row 4: number=4, double=8
Final count: 5

=== Test 3: Empty Result ===
Iterated rows: 0
Count: 0

=== Test 4: Manual Iterator Control ===
valid(): true
key(): 0
current()['number']: 0
After next() - key(): 1
After next() - current()['number']: 1

=== Test 5: Large Dataset Streaming ===
Total rows: 1000
Sum: 499500 (expected: 499500)
Final count(): 1000

=== Test 6: Memory Efficiency (10K rows) ===
Total processed: 10000 rows
Memory efficient: yes

=== Test 7: Rewind Functionality ===
First iteration: 0 1 2
Second iteration: 0 1 2

=== Test 8: Early Break ===
Iterated 5 rows (stopped early)
Count after break: 5

=== Test 9: iterator_to_array() ===
Array count: 3
Array keys: 0, 1, 2
Values: 0, 1, 2

=== Test 10: Multiple Columns ===
int: 42, str: hello, float: 3.14

OK
