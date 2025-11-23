--TEST--
ClickHouse: Query settings support
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

// Test setQuerySetting exists
echo "setQuerySetting exists: ";
var_dump(method_exists($client, 'setQuerySetting'));

// Test clearQuerySettings exists
echo "clearQuerySettings exists: ";
var_dump(method_exists($client, 'clearQuerySettings'));

// Test setting max_result_rows
$client->setQuerySetting("max_result_rows", "5");
echo "Setting applied: ";
try {
    $r = $client->query("SELECT number FROM system.numbers LIMIT 100");
    // If we get here without exception, setting didn't work
    echo "bool(false)\n";
} catch (ClickHouse\Exception $e) {
    // Exception means the setting was applied
    $applied = strpos($e->getMessage(), "max rows: 5") !== false;
    var_dump($applied);
}

// Test clearing settings
$client->clearQuerySettings();
echo "Settings cleared: ";
$r = $client->query("SELECT number FROM system.numbers LIMIT 100");
var_dump(count($r) === 100);

// Test max_threads setting (doesn't throw, just affects execution)
$client->setQuerySetting("max_threads", "1");
echo "max_threads setting: ";
$r = $client->query("SELECT 1 as num");
var_dump($r[0]['num'] === 1);

echo "OK\n";
?>
--EXPECT--
setQuerySetting exists: bool(true)
clearQuerySettings exists: bool(true)
Setting applied: bool(true)
Settings cleared: bool(true)
max_threads setting: bool(true)
OK
