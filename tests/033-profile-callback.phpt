--TEST--
ClickHouse: setProfileCallback for query profiling events
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

// Test profile callback is called
$profileCalls = 0;
$profileData = [];

$client->setProfileCallback(function($rows, $blocks, $bytes, $appliedLimit, $rowsBeforeLimit, $calculatedRowsBeforeLimit) use (&$profileCalls, &$profileData) {
    $profileCalls++;
    $profileData = [
        'rows' => $rows,
        'blocks' => $blocks,
        'bytes' => $bytes,
        'applied_limit' => $appliedLimit,
        'rows_before_limit' => $rowsBeforeLimit,
    ];
});

// Test basic query
$result = $client->query("SELECT number FROM numbers(100)");
var_dump($profileCalls === 1);
var_dump($profileData['rows'] === 100);
var_dump($profileData['blocks'] >= 1);
var_dump($profileData['bytes'] > 0);

// Test with LIMIT
$profileCalls = 0;
$result = $client->query("SELECT number FROM numbers(100) LIMIT 10");
var_dump($profileCalls === 1);
var_dump($profileData['rows'] === 10);
var_dump($profileData['applied_limit'] === true);

// Test clearing callback
$client->setProfileCallback(null);
$profileCalls = 0;
$result = $client->query("SELECT 1");
var_dump($profileCalls === 0);

echo "OK\n";
?>
--EXPECT--
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
OK
