--TEST--
Client::selectByBlock() streaming callback
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
$totalRows = 0;
$blocks = 0;

$client->selectByBlock('SELECT number FROM system.numbers LIMIT 100', function($block) use (&$totalRows, &$blocks) {
    $totalRows += $block->getRowCount();
    $blocks++;
});

var_dump($totalRows);
var_dump($blocks > 0);

// Test cancellation via return false
$seen = 0;
$client->selectByBlock('SELECT number FROM system.numbers LIMIT 10000', function($block) use (&$seen) {
    $seen += $block->getRowCount();
    return false; // cancel after first block
});

var_dump($seen > 0);
var_dump($seen < 10000);

echo "OK\n";
?>
--EXPECT--
int(100)
bool(true)
bool(true)
bool(true)
OK
