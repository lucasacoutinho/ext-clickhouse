--TEST--
ClickHouse: setProgressCallback method exists and can be set
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

// Test that setProgressCallback method exists and accepts callable
$called = false;
$client->setProgressCallback(function($rows, $bytes, $totalRows, $writtenRows, $writtenBytes) use (&$called) {
    $called = true;
});
var_dump(method_exists($client, 'setProgressCallback'));

// Test clearing callback with null
$client->setProgressCallback(null);
var_dump(method_exists($client, 'setProgressCallback'));

// Test setting callback again
$client->setProgressCallback(function($rows, $bytes, $totalRows, $writtenRows, $writtenBytes) {
    // Callback body
});
var_dump(method_exists($client, 'setProgressCallback'));

echo "OK\n";
?>
--EXPECT--
bool(true)
bool(true)
bool(true)
OK
