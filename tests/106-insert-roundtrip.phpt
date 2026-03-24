--TEST--
Insert and select round-trip with various types
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

$client->execute('DROP TABLE IF EXISTS _test_ext_roundtrip');
$client->execute('CREATE TABLE _test_ext_roundtrip (
    id UInt64,
    name String,
    score Float64,
    tags Array(String),
    nullable_val Nullable(Int32)
) ENGINE = Memory');

// Build a block
$block = new Block();
$block->appendColumn('id', Column::create('UInt64', [1, 2, 3]));
$block->appendColumn('name', Column::create('String', ['alice', 'bob', 'charlie']));
$block->appendColumn('score', Column::create('Float64', [9.5, 8.0, 7.25]));
$block->appendColumn('tags', Column::create('Array(String)', [['php', 'cpp'], ['go'], []]));
$block->appendColumn('nullable_val', Column::create('Nullable(Int32)', [100, null, -5]));

$client->insert('_test_ext_roundtrip', $block);

// Read back
$rows = $client->select('SELECT * FROM _test_ext_roundtrip ORDER BY id');

var_dump(count($rows));

// Row 1
var_dump($rows[0]['id']);
var_dump($rows[0]['name']);
var_dump($rows[0]['score']);
var_dump($rows[0]['tags']);
var_dump($rows[0]['nullable_val']);

// Row 2
var_dump($rows[1]['id']);
var_dump($rows[1]['nullable_val']);

// Row 3
var_dump($rows[2]['id']);
var_dump($rows[2]['tags']);
var_dump($rows[2]['nullable_val']);

echo "OK\n";
?>
--CLEAN--
<?php
require __DIR__ . '/clickhouse_test.inc';
$client = clickhouse_test_client();
try { $client->execute('DROP TABLE IF EXISTS _test_ext_roundtrip'); } catch (\Throwable $e) {}
?>
--EXPECT--
int(3)
int(1)
string(5) "alice"
float(9.5)
array(2) {
  [0]=>
  string(3) "php"
  [1]=>
  string(3) "cpp"
}
int(100)
int(2)
NULL
int(3)
array(0) {
}
int(-5)
OK
