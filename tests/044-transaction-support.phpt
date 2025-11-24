--TEST--
ClickHouse: Transaction support (EXPERIMENTAL)
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

// Test 1: inTransaction() should return false initially
echo "Test 1: Check initial transaction state\n";
echo "In transaction: " . ($client->inTransaction() ? 'yes' : 'no') . "\n";

// Test 2: Try to commit without transaction (should fail)
echo "\nTest 2: Commit without transaction\n";
try {
    $client->commit();
    echo "ERROR: Should have thrown exception\n";
} catch (Exception $e) {
    echo "Correctly rejected: No active transaction\n";
}

// Test 3: Try to rollback without transaction (should fail)
echo "\nTest 3: Rollback without transaction\n";
try {
    $client->rollback();
    echo "ERROR: Should have thrown exception\n";
} catch (Exception $e) {
    echo "Correctly rejected: No active transaction\n";
}

// Test 4: Check beginTransaction() method exists and callable
echo "\nTest 4: Check transaction methods\n";
echo "beginTransaction exists: " . (method_exists($client, 'beginTransaction') ? 'yes' : 'no') . "\n";
echo "commit exists: " . (method_exists($client, 'commit') ? 'yes' : 'no') . "\n";
echo "rollback exists: " . (method_exists($client, 'rollback') ? 'yes' : 'no') . "\n";
echo "inTransaction exists: " . (method_exists($client, 'inTransaction') ? 'yes' : 'no') . "\n";

// Test 5: Attempt beginTransaction (may fail if database not configured for transactions)
echo "\nTest 5: Attempt to begin transaction\n";
try {
    $result = $client->beginTransaction();
    echo "Transaction started: " . ($result ? 'yes' : 'no') . "\n";
    echo "In transaction: " . ($client->inTransaction() ? 'yes' : 'no') . "\n";

    // Try to start another transaction (should fail)
    try {
        $client->beginTransaction();
        echo "ERROR: Should not allow nested transactions\n";
    } catch (Exception $e) {
        echo "Correctly rejected nested transaction\n";
    }

    // Test rollback
    echo "\nTest 6: Rollback transaction\n";
    $rollback_result = $client->rollback();
    echo "Rollback result: " . ($rollback_result ? 'success' : 'failed') . "\n";
    echo "In transaction after rollback: " . ($client->inTransaction() ? 'yes' : 'no') . "\n";

} catch (Exception $e) {
    // Transactions may not be supported on this ClickHouse version/config
    echo "Transaction not supported (expected for most setups)\n";
    echo "Error message contains 'EXPERIMENTAL': " . (strpos($e->getMessage(), 'EXPERIMENTAL') !== false ? 'yes' : 'no') . "\n";
    echo "Error message contains 'Atomic': " . (strpos($e->getMessage(), 'Atomic') !== false ? 'yes' : 'no') . "\n";
}

// Test 7: Ensure we're not in transaction state
echo "\nTest 7: Final state check\n";
echo "In transaction: " . ($client->inTransaction() ? 'yes' : 'no') . "\n";

echo "\nOK\n";
?>
--EXPECT--
Test 1: Check initial transaction state
In transaction: no

Test 2: Commit without transaction
Correctly rejected: No active transaction

Test 3: Rollback without transaction
Correctly rejected: No active transaction

Test 4: Check transaction methods
beginTransaction exists: yes
commit exists: yes
rollback exists: yes
inTransaction exists: yes

Test 5: Attempt to begin transaction
Transaction not supported (expected for most setups)
Error message contains 'EXPERIMENTAL': yes
Error message contains 'Atomic': yes

Test 7: Final state check
In transaction: no

OK
