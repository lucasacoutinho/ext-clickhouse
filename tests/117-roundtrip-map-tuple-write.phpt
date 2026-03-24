--TEST--
Round-trip: Map and Tuple insert (write path)
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

$client->execute('DROP TABLE IF EXISTS _test_ext_map_tuple');
$client->execute("CREATE TABLE _test_ext_map_tuple (
    m Map(String, UInt32),
    t Tuple(String, UInt32, Float64),
    nested_arr Array(Tuple(String, UInt32))
) ENGINE = Memory");

$block = new Block();
$block->appendColumn('m', Column::create('Map(String, UInt32)', [
    ['alpha' => 1, 'beta' => 2],
    [],
    ['gamma' => 42],
]));
$block->appendColumn('t', Column::create('Tuple(String, UInt32, Float64)', [
    ['hello', 100, 1.5],
    ['world', 200, 2.5],
    ['test', 0, 0.0],
]));
$block->appendColumn('nested_arr', Column::create('Array(Tuple(String, UInt32))', [
    [['a', 1], ['b', 2]],
    [],
    [['c', 3]],
]));

$client->insert('_test_ext_map_tuple', $block);

$rows = $client->select('SELECT * FROM _test_ext_map_tuple');

echo "Row count: " . count($rows) . "\n";

// Row 0
echo "\n--- Row 0 ---\n";
var_dump($rows[0]['m']);
var_dump($rows[0]['t']);
var_dump($rows[0]['nested_arr']);

// Row 1 (empty map, empty nested array)
echo "\n--- Row 1 ---\n";
var_dump($rows[1]['m']);
var_dump($rows[1]['t']);
var_dump($rows[1]['nested_arr']);

// Row 2
echo "\n--- Row 2 ---\n";
var_dump($rows[2]['m']);
var_dump($rows[2]['nested_arr']);

echo "\nDone\n";
?>
--CLEAN--
<?php
require __DIR__ . '/clickhouse_test.inc';
$client = clickhouse_test_client();
try { $client->execute('DROP TABLE IF EXISTS _test_ext_map_tuple'); } catch (\Throwable $e) {}
?>
--EXPECT--
Row count: 3

--- Row 0 ---
array(2) {
  ["alpha"]=>
  int(1)
  ["beta"]=>
  int(2)
}
array(3) {
  [0]=>
  string(5) "hello"
  [1]=>
  int(100)
  [2]=>
  float(1.5)
}
array(2) {
  [0]=>
  array(2) {
    [0]=>
    string(1) "a"
    [1]=>
    int(1)
  }
  [1]=>
  array(2) {
    [0]=>
    string(1) "b"
    [1]=>
    int(2)
  }
}

--- Row 1 ---
array(0) {
}
array(3) {
  [0]=>
  string(5) "world"
  [1]=>
  int(200)
  [2]=>
  float(2.5)
}
array(0) {
}

--- Row 2 ---
array(1) {
  ["gamma"]=>
  int(42)
}
array(1) {
  [0]=>
  array(2) {
    [0]=>
    string(1) "c"
    [1]=>
    int(3)
  }
}

Done
