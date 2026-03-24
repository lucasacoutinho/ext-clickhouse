--TEST--
Client::selectWithExternalData() basic usage and edge cases
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
use ClickHouse\Driver\{Block, Column};

$client = clickhouse_test_client();

// Build an external table with some IDs
$block = new Block();
$block->appendColumn('id', Column::create('UInt64', [1, 3, 5, 7]));

// Use a subquery to bound the scan — system.numbers is infinite
$rows = $client->selectWithExternalData(
    'SELECT number FROM (SELECT number FROM system.numbers LIMIT 100) sub WHERE number IN (SELECT id FROM _ext_ids)',
    [['name' => '_ext_ids', 'data' => $block]]
);

// We should get numbers that are in our external table AND < 100
$ids = array_column($rows, 'number');
sort($ids);
var_dump($ids);

// --- Edge case: malformed entries are silently skipped ---
// All these pass invalid external table entries, but the query itself
// doesn't reference any external table, so it should just return results.

// Missing 'data' key
$rows = $client->selectWithExternalData(
    'SELECT 1 AS n',
    [['name' => 'test']]
);
echo "Missing data key: count=" . count($rows) . "\n";

// 'data' is not a Block object (tests the fix for Z_TYPE_P check)
$rows = $client->selectWithExternalData(
    'SELECT 1 AS n',
    [['name' => 'test', 'data' => 'not_a_block']]
);
echo "Non-Block data: count=" . count($rows) . "\n";

// 'data' is an integer
$rows = $client->selectWithExternalData(
    'SELECT 1 AS n',
    [['name' => 'test', 'data' => 42]]
);
echo "Integer data: count=" . count($rows) . "\n";

// Entry is not an array
$rows = $client->selectWithExternalData(
    'SELECT 1 AS n',
    ['not_an_array']
);
echo "Non-array entry: count=" . count($rows) . "\n";

// Missing 'name' key
$rows = $client->selectWithExternalData(
    'SELECT 1 AS n',
    [['data' => $block]]
);
echo "Missing name key: count=" . count($rows) . "\n";

// 'name' is not a string
$rows = $client->selectWithExternalData(
    'SELECT 1 AS n',
    [['name' => 42, 'data' => $block]]
);
echo "Non-string name: count=" . count($rows) . "\n";

echo "Done\n";
?>
--EXPECT--
array(4) {
  [0]=>
  int(1)
  [1]=>
  int(3)
  [2]=>
  int(5)
  [3]=>
  int(7)
}
Missing data key: count=1
Non-Block data: count=1
Integer data: count=1
Non-array entry: count=1
Missing name key: count=1
Non-string name: count=1
Done
