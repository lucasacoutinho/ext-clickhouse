--TEST--
ClickHouse\Client::queryIterator() - Iterator interface for query results
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
$iter = $client->queryIterator("SELECT 1 as num");
echo "Implements Iterator: ";
var_dump($iter instanceof Iterator);
echo "Implements Countable: ";
var_dump($iter instanceof Countable);
echo "Class name: " . get_class($iter) . "\n";

// Test 2: Basic iteration
echo "\n=== Test 2: Basic Iteration ===\n";
$iter = $client->queryIterator("SELECT number, number * 2 as double FROM numbers(5)");
echo "Row count: " . count($iter) . "\n";
foreach ($iter as $key => $row) {
    echo "Row $key: number={$row['number']}, double={$row['double']}\n";
}

// Test 3: Empty result
echo "\n=== Test 3: Empty Result ===\n";
$iter = $client->queryIterator("SELECT number FROM numbers(0)");
echo "Count: " . count($iter) . "\n";
$iteratedCount = 0;
foreach ($iter as $row) {
    $iteratedCount++;
}
echo "Iterated rows: $iteratedCount\n";

// Test 4: Multiple data types
echo "\n=== Test 4: Multiple Data Types ===\n";
$iter = $client->queryIterator("SELECT
    42 as int_val,
    'hello' as str_val,
    3.14 as float_val,
    true as bool_val,
    [1,2,3] as arr_val
");
foreach ($iter as $row) {
    echo "int: {$row['int_val']}, ";
    echo "str: {$row['str_val']}, ";
    echo "float: {$row['float_val']}, ";
    echo "bool: " . ($row['bool_val'] ? 'true' : 'false') . ", ";
    echo "arr: [" . implode(',', $row['arr_val']) . "]\n";
}

// Test 5: Rewind functionality
echo "\n=== Test 5: Rewind Functionality ===\n";
$iter = $client->queryIterator("SELECT number FROM numbers(3)");
echo "First iteration: ";
$values = [];
foreach ($iter as $row) {
    $values[] = $row['number'];
}
echo implode(' ', $values) . "\n";

echo "Second iteration: ";
$values = [];
foreach ($iter as $row) {
    $values[] = $row['number'];
}
echo implode(' ', $values) . "\n";

// Test 6: Manual iterator control
echo "\n=== Test 6: Manual Iterator Control ===\n";
$iter = $client->queryIterator("SELECT number FROM numbers(5)");
$iter->rewind();
echo "valid(): " . ($iter->valid() ? "true" : "false") . "\n";
echo "key(): " . $iter->key() . "\n";
echo "current()['number']: " . $iter->current()['number'] . "\n";
$iter->next();
echo "After next() - key(): " . $iter->key() . "\n";
echo "After next() - current()['number']: " . $iter->current()['number'] . "\n";

// Test 7: Large dataset
echo "\n=== Test 7: Large Dataset ===\n";
$iter = $client->queryIterator("SELECT number FROM numbers(1000)");
echo "Total rows: " . count($iter) . "\n";
$sum = 0;
foreach ($iter as $row) {
    $sum += $row['number'];
}
echo "Sum: $sum (expected: 499500)\n";

// Test 8: Iterator with WHERE clause
echo "\n=== Test 8: Iterator with WHERE Clause ===\n";
$iter = $client->queryIterator("SELECT number FROM numbers(20) WHERE number % 2 = 0");
echo "Even numbers count: " . count($iter) . "\n";
echo "Even numbers: ";
$values = [];
foreach ($iter as $row) {
    $values[] = $row['number'];
}
echo implode(' ', $values) . "\n";

// Test 9: Iterator with ORDER BY
echo "\n=== Test 9: Iterator with ORDER BY ===\n";
$iter = $client->queryIterator("SELECT number FROM numbers(5) ORDER BY number DESC");
echo "Descending order: ";
$values = [];
foreach ($iter as $row) {
    $values[] = $row['number'];
}
echo implode(' ', $values) . "\n";

// Test 10: Complex query with joins (using system tables)
echo "\n=== Test 10: Complex Query ===\n";
$iter = $client->queryIterator("
    SELECT name, engine
    FROM system.tables
    WHERE database = 'system'
    LIMIT 3
");
echo "System tables count: " . count($iter) . "\n";
foreach ($iter as $key => $row) {
    echo "Table $key: {$row['name']} ({$row['engine']})\n";
}

// Test 11: Break early from iteration
echo "\n=== Test 11: Early Break ===\n";
$iter = $client->queryIterator("SELECT number FROM numbers(100)");
$count = 0;
foreach ($iter as $row) {
    $count++;
    if ($count >= 5) {
        break;
    }
}
echo "Iterated $count rows (stopped early)\n";

// Test 12: Using iterator_to_array
echo "\n=== Test 12: iterator_to_array() ===\n";
$iter = $client->queryIterator("SELECT number FROM numbers(3)");
$array = iterator_to_array($iter);
echo "Array count: " . count($array) . "\n";
echo "Array keys: " . implode(', ', array_keys($array)) . "\n";

echo "\nOK\n";
?>
--EXPECTF--
=== Test 1: Iterator Interfaces ===
Implements Iterator: bool(true)
Implements Countable: bool(true)
Class name: ClickHouse\ResultIterator

=== Test 2: Basic Iteration ===
Row count: 5
Row 0: number=0, double=0
Row 1: number=1, double=2
Row 2: number=2, double=4
Row 3: number=3, double=6
Row 4: number=4, double=8

=== Test 3: Empty Result ===
Count: 0
Iterated rows: 0

=== Test 4: Multiple Data Types ===
int: 42, str: hello, float: 3.14, bool: true, arr: [1,2,3]

=== Test 5: Rewind Functionality ===
First iteration: 0 1 2
Second iteration: 0 1 2

=== Test 6: Manual Iterator Control ===
valid(): true
key(): 0
current()['number']: 0
After next() - key(): 1
After next() - current()['number']: 1

=== Test 7: Large Dataset ===
Total rows: 1000
Sum: 499500 (expected: 499500)

=== Test 8: Iterator with WHERE Clause ===
Even numbers count: 10
Even numbers: 0 2 4 6 8 10 12 14 16 18

=== Test 9: Iterator with ORDER BY ===
Descending order: 4 3 2 1 0

=== Test 10: Complex Query ===
System tables count: 3
Table 0: %s (%s)
Table 1: %s (%s)
Table 2: %s (%s)

=== Test 11: Early Break ===
Iterated 5 rows (stopped early)

=== Test 12: iterator_to_array() ===
Array count: 3
Array keys: 0, 1, 2

OK
