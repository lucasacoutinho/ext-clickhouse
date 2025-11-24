--TEST--
ClickHouse\Client::setReadOnly() / isReadOnly() - Read-only mode functionality
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

// Test default state
echo "Initial read-only state: ";
var_dump($client->isReadOnly());

// Test enabling read-only mode
$client->setReadOnly(true);
echo "After setReadOnly(true): ";
var_dump($client->isReadOnly());

// Test SELECT queries work in read-only mode
echo "SELECT query in read-only mode: ";
try {
    $result = $client->query("SELECT 1 as num");
    echo "Success\n";
} catch (Exception $e) {
    echo "Failed: " . $e->getMessage() . "\n";
}

// Test INSERT query fails in read-only mode via query()
echo "INSERT via query() in read-only mode: ";
try {
    $client->query("INSERT INTO test VALUES (1)");
    echo "Should not succeed\n";
} catch (Exception $e) {
    echo "Blocked correctly\n";
}

// Test CREATE query fails in read-only mode via query()
echo "CREATE via query() in read-only mode: ";
try {
    $client->query("CREATE TABLE test (x UInt32) ENGINE = Memory");
    echo "Should not succeed\n";
} catch (Exception $e) {
    echo "Blocked correctly\n";
}

// Test DROP query fails in read-only mode via query()
echo "DROP via query() in read-only mode: ";
try {
    $client->query("DROP TABLE IF EXISTS test");
    echo "Should not succeed\n";
} catch (Exception $e) {
    echo "Blocked correctly\n";
}

// Test INSERT via execute() fails in read-only mode
echo "INSERT via execute() in read-only mode: ";
try {
    $client->execute("INSERT INTO test VALUES (1)");
    echo "Should not succeed\n";
} catch (Exception $e) {
    echo "Blocked correctly\n";
}

// Test insert() method fails in read-only mode
echo "insert() method in read-only mode: ";
try {
    $client->insert("test", ["x"], [[1]]);
    echo "Should not succeed\n";
} catch (Exception $e) {
    echo "Blocked correctly\n";
}

// Test disabling read-only mode
$client->setReadOnly(false);
echo "After setReadOnly(false): ";
var_dump($client->isReadOnly());

// Test write operations work after disabling read-only mode
echo "CREATE after disabling read-only: ";
try {
    $client->execute("DROP TABLE IF EXISTS test_readonly");
    $client->execute("CREATE TABLE test_readonly (x UInt32) ENGINE = Memory");
    echo "Success\n";
} catch (Exception $e) {
    echo "Failed: " . $e->getMessage() . "\n";
}

echo "INSERT after disabling read-only: ";
try {
    $client->execute("INSERT INTO test_readonly VALUES (42)");
    echo "Success\n";
} catch (Exception $e) {
    echo "Failed: " . $e->getMessage() . "\n";
}

// Verify data was inserted
echo "Verify data: ";
try {
    $result = $client->query("SELECT x FROM test_readonly");
    echo $result[0]['x'] === 42 ? "Success\n" : "Failed\n";
} catch (Exception $e) {
    echo "Failed: " . $e->getMessage() . "\n";
}

// Cleanup
$client->execute("DROP TABLE IF EXISTS test_readonly");

echo "OK\n";
?>
--EXPECT--
Initial read-only state: bool(false)
After setReadOnly(true): bool(true)
SELECT query in read-only mode: Success
INSERT via query() in read-only mode: Blocked correctly
CREATE via query() in read-only mode: Blocked correctly
DROP via query() in read-only mode: Blocked correctly
INSERT via execute() in read-only mode: Blocked correctly
insert() method in read-only mode: Blocked correctly
After setReadOnly(false): bool(false)
CREATE after disabling read-only: Success
INSERT after disabling read-only: Success
Verify data: Success
OK
