--TEST--
Client methods: resetConnection, getCurrentEndpoint, selectByBlock callbacks
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php
require __DIR__ . '/clickhouse_test.inc';
clickhouse_test_skip();
?>
--FILE--
<?php
require __DIR__ . '/clickhouse_test.inc';

$client = clickhouse_test_client();

// --- getCurrentEndpoint ---
$ep = $client->getCurrentEndpoint();
echo "Endpoint is array: " . (is_array($ep) ? 'yes' : 'no') . "\n";
echo "Has host: " . (isset($ep['host']) && is_string($ep['host']) ? 'yes' : 'no') . "\n";
echo "Has port: " . (isset($ep['port']) && is_int($ep['port']) ? 'yes' : 'no') . "\n";
echo "Port > 0: " . ($ep['port'] > 0 ? 'yes' : 'no') . "\n";

// --- resetConnection ---
$client->resetConnection();
// Verify the connection still works after reset
$client->ping();
echo "OK: ping after resetConnection\n";

// --- selectByBlock with onProgress callback ---
$progressCalled = false;
$progressData = null;

$client->selectByBlock(
    'SELECT number FROM system.numbers LIMIT 1000',
    function($block) {
        // Just consume blocks
    },
    null, // params
    null, // settings
    null, // queryId
    function($progress) use (&$progressCalled, &$progressData) {
        $progressCalled = true;
        $progressData = $progress;
    }
);

echo "Progress callback called: " . ($progressCalled ? 'yes' : 'no') . "\n";
if ($progressData !== null) {
    echo "Progress has 'rows': " . (isset($progressData['rows']) ? 'yes' : 'no') . "\n";
    echo "Progress has 'bytes': " . (isset($progressData['bytes']) ? 'yes' : 'no') . "\n";
    echo "Progress has 'total_rows': " . (isset($progressData['total_rows']) ? 'yes' : 'no') . "\n";
}

// --- selectByBlock with onProfile callback ---
$profileCalled = false;
$profileData = null;

$client->selectByBlock(
    'SELECT number FROM system.numbers LIMIT 100',
    function($block) {
        // consume
    },
    null, null, null,
    null, // onProgress
    function($profile) use (&$profileCalled, &$profileData) {
        $profileCalled = true;
        $profileData = $profile;
    }
);

echo "Profile callback called: " . ($profileCalled ? 'yes' : 'no') . "\n";
if ($profileData !== null) {
    echo "Profile has 'rows': " . (isset($profileData['rows']) ? 'yes' : 'no') . "\n";
    echo "Profile has 'blocks': " . (isset($profileData['blocks']) ? 'yes' : 'no') . "\n";
    echo "Profile has 'bytes': " . (isset($profileData['bytes']) ? 'yes' : 'no') . "\n";
    echo "Profile has 'rows_before_limit': " . (isset($profileData['rows_before_limit']) ? 'yes' : 'no') . "\n";
    echo "Profile has 'applied_limit': " . (isset($profileData['applied_limit']) ? 'yes' : 'no') . "\n";
}

// --- selectByBlock cancellation via return false ---
$blocksBeforeCancel = 0;
$client->selectByBlock(
    'SELECT number FROM system.numbers LIMIT 100000',
    function($block) use (&$blocksBeforeCancel) {
        $blocksBeforeCancel++;
        return false; // Cancel after first block
    }
);
echo "Blocks before cancel: " . $blocksBeforeCancel . "\n";

// --- execute with queryId ---
$client->execute('SELECT 1', null, null, 'test-query-id-001');
echo "OK: execute with queryId\n";

// --- insert with queryId ---
$client->execute('DROP TABLE IF EXISTS _test_ext_qid');
$client->execute('CREATE TABLE _test_ext_qid (n UInt8) ENGINE = Memory');

$block = new ClickHouse\Driver\Block();
$block->appendColumn('n', ClickHouse\Driver\Column::create('UInt8', [1]));
$client->insert('_test_ext_qid', $block, 'test-insert-qid-001');

$rows = $client->select('SELECT * FROM _test_ext_qid');
echo "Insert with queryId row count: " . count($rows) . "\n";

echo "Done\n";
?>
--CLEAN--
<?php
require __DIR__ . '/clickhouse_test.inc';
$client = clickhouse_test_client();
try { $client->execute('DROP TABLE IF EXISTS _test_ext_qid'); } catch (\Throwable $e) {}
?>
--EXPECT--
Endpoint is array: yes
Has host: yes
Has port: yes
Port > 0: yes
OK: ping after resetConnection
Progress callback called: yes
Progress has 'rows': yes
Progress has 'bytes': yes
Progress has 'total_rows': yes
Profile callback called: yes
Profile has 'rows': yes
Profile has 'blocks': yes
Profile has 'bytes': yes
Profile has 'rows_before_limit': yes
Profile has 'applied_limit': yes
Blocks before cancel: 1
OK: execute with queryId
Insert with queryId row count: 1
Done
