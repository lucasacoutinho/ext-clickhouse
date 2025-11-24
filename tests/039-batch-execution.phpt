--TEST--
Batch Query Execution
--SKIPIF--
<?php
if (!extension_loaded('clickhouse')) die('skip ClickHouse extension not loaded');
if (!getenv('CLICKHOUSE_HOST')) die('skip CLICKHOUSE_HOST not set');
?>
--FILE--
<?php
$client = new ClickHouse\Client(getenv('CLICKHOUSE_HOST') ?: 'localhost', 9000, 'default', '', 'default');

echo "=== Test 1: Simple Batch INSERT ===\n";
$results = $client->executeBatch([
    "CREATE TABLE IF NOT EXISTS test_batch (id Int32, name String) ENGINE = Memory",
    "INSERT INTO test_batch VALUES (1, 'Alice')",
    "INSERT INTO test_batch VALUES (2, 'Bob')",
    "INSERT INTO test_batch VALUES (3, 'Charlie')"
]);
echo "Query count: " . count($results) . "\n";
for ($i = 0; $i < count($results); $i++) {
    echo "Result $i: " . (is_bool($results[$i]) && $results[$i] ? "Success" : "Other") . "\n";
}

echo "\n=== Test 2: Mixed Queries with Results ===\n";
$results = $client->executeBatch([
    "INSERT INTO test_batch VALUES (4, 'David')",
    "SELECT * FROM test_batch WHERE id > 2 ORDER BY id",
    "SELECT count() as cnt FROM test_batch"
]);
echo "Result 0 (INSERT): " . ($results[0] === true ? "Success" : "Failed") . "\n";
echo "Result 1 (SELECT WHERE): " . count($results[1]) . " rows\n";
foreach ($results[1] as $row) {
    echo "  - ID: {$row['id']}, Name: {$row['name']}\n";
}
echo "Result 2 (SELECT COUNT): Count = " . $results[2][0]['cnt'] . "\n";

echo "\n=== Test 3: Continue on Error (stopOnError=false) ===\n";
$results = $client->executeBatch([
    "INSERT INTO test_batch VALUES (5, 'Eve')",
    "INSERT INTO invalid_table VALUES (1, 'Bob')",
    "INSERT INTO test_batch VALUES (6, 'Frank')"
], ['stopOnError' => false]);
echo "Results count: " . count($results) . "\n";
echo "Result 0: " . ($results[0] === true ? "Success" : "Failed") . "\n";
echo "Result 1: " . ($results[1] instanceof Exception ? "Exception" : "Other") . "\n";
echo "Result 2: " . ($results[2] === true ? "Success" : "Failed") . "\n";

echo "\n=== Test 4: With Parameters (Auto-Type Detection) ===\n";
$results = $client->executeBatch([
    ['query' => "INSERT INTO test_batch VALUES ({id}, {name})", 'params' => ['id' => 7, 'name' => 'George']],
    ['query' => "SELECT * FROM test_batch WHERE id = {id}", 'params' => ['id' => 7]]
]);
echo "Result 0 (INSERT): " . ($results[0] === true ? "Success" : "Failed") . "\n";
echo "Result 1 (SELECT): " . count($results[1]) . " rows\n";
if (count($results[1]) > 0) {
    echo "  - ID: {$results[1][0]['id']}, Name: {$results[1][0]['name']}\n";
}

echo "\n=== Test 5: Stop on Error (stopOnError=true, default) ===\n";
try {
    $results = $client->executeBatch([
        "INSERT INTO test_batch VALUES (8, 'Helen')",
        "INSERT INTO invalid_table VALUES (1, 'Bob')",
        "INSERT INTO test_batch VALUES (9, 'Ivan')"
    ]);
    echo "Should not reach here\n";
} catch (Exception $e) {
    echo "Exception caught: " . (strpos($e->getMessage(), 'invalid_table') !== false ? "Contains 'invalid_table'" : "Other") . "\n";
}

echo "\n=== Test 6: Empty Batch ===\n";
$results = $client->executeBatch([]);
echo "Empty batch result count: " . count($results) . "\n";

echo "\n=== Test 7: returnResults Option ===\n";
$results = $client->executeBatch([
    "SELECT * FROM test_batch WHERE id < 3 ORDER BY id"
], ['returnResults' => true]);
echo "With returnResults=true: " . count($results[0]) . " rows\n";

$results = $client->executeBatch([
    "SELECT * FROM test_batch WHERE id < 3 ORDER BY id"
], ['returnResults' => false]);
echo "With returnResults=false: " . (is_bool($results[0]) ? "Boolean" : "Array") . "\n";

echo "\n=== Test 8: Invalid Query Item ===\n";
try {
    $results = $client->executeBatch([
        ['params' => ['id' => 1]]  // Missing 'query' field
    ]);
    echo "Should not reach here\n";
} catch (Exception $e) {
    echo "Exception caught: " . (strpos($e->getMessage(), 'missing') !== false ? "Contains 'missing'" : "Other") . "\n";
}

echo "\n=== Test 9: Invalid Query Item with stopOnError=false ===\n";
$results = $client->executeBatch([
    "INSERT INTO test_batch VALUES (10, 'John')",
    ['params' => ['id' => 1]],  // Missing 'query' field
    "INSERT INTO test_batch VALUES (11, 'Kate')"
], ['stopOnError' => false]);
echo "Results count: " . count($results) . "\n";
echo "Result 0: " . ($results[0] === true ? "Success" : "Failed") . "\n";
echo "Result 1: " . ($results[1] instanceof Exception ? "Exception" : "Other") . "\n";
echo "Result 2: " . ($results[2] === true ? "Success" : "Failed") . "\n";

echo "\n=== Test 10: Read-Only Mode ===\n";
$client->setReadOnly(true);
try {
    $results = $client->executeBatch([
        "INSERT INTO test_batch VALUES (12, 'Lucy')"
    ]);
    echo "Should not reach here\n";
} catch (Exception $e) {
    echo "Exception caught: " . (strpos($e->getMessage(), 'read-only') !== false ? "Contains 'read-only'" : "Other") . "\n";
}
$client->setReadOnly(false);

echo "\n=== Test 11: Read-Only Mode with stopOnError=false ===\n";
$client->setReadOnly(true);
$results = $client->executeBatch([
    "INSERT INTO test_batch VALUES (12, 'Lucy')"
], ['stopOnError' => false]);
echo "Result 0: " . ($results[0] instanceof Exception ? "Exception" : "Other") . "\n";
$client->setReadOnly(false);

echo "\n=== Cleanup ===\n";
$client->execute("DROP TABLE IF EXISTS test_batch");
echo "Cleanup complete\n";

echo "\nDone\n";
?>
--EXPECT--
=== Test 1: Simple Batch INSERT ===
Query count: 4
Result 0: Success
Result 1: Success
Result 2: Success
Result 3: Success

=== Test 2: Mixed Queries with Results ===
Result 0 (INSERT): Success
Result 1 (SELECT WHERE): 2 rows
  - ID: 3, Name: Charlie
  - ID: 4, Name: David
Result 2 (SELECT COUNT): Count = 4

=== Test 3: Continue on Error (stopOnError=false) ===
Results count: 3
Result 0: Success
Result 1: Exception
Result 2: Success

=== Test 4: With Parameters (Auto-Type Detection) ===
Result 0 (INSERT): Success
Result 1 (SELECT): 1 rows
  - ID: 7, Name: George

=== Test 5: Stop on Error (stopOnError=true, default) ===
Exception caught: Contains 'invalid_table'

=== Test 6: Empty Batch ===
Empty batch result count: 0

=== Test 7: returnResults Option ===
With returnResults=true: 2 rows
With returnResults=false: Boolean

=== Test 8: Invalid Query Item ===
Exception caught: Contains 'missing'

=== Test 9: Invalid Query Item with stopOnError=false ===
Results count: 3
Result 0: Success
Result 1: Exception
Result 2: Success

=== Test 10: Read-Only Mode ===
Exception caught: Contains 'read-only'

=== Test 11: Read-Only Mode with stopOnError=false ===
Result 0: Exception

=== Cleanup ===
Cleanup complete

Done
