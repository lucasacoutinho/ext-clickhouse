--TEST--
Round-trip expanded LowCardinality types and nested SimpleAggregateFunction columns
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
$client->execute('DROP TABLE IF EXISTS _test_ext_upstream_types');
$client->execute('CREATE TABLE _test_ext_upstream_types (
    id UInt8,
    number LowCardinality(Int32),
    ip LowCardinality(IPv4),
    optional LowCardinality(Nullable(Int32)),
    items SimpleAggregateFunction(groupArrayArray, Array(UInt64))
) ENGINE = Memory', null, ['allow_suspicious_low_cardinality_types' => '1']);

$block = new Block();
$block->appendColumn('id', Column::create('UInt8', [1, 2, 3]));
$block->appendColumn('number', Column::create('LowCardinality(Int32)', [-42, 7, -42]));
$block->appendColumn('ip', Column::create('LowCardinality(IPv4)', ['127.0.0.1', '192.168.1.1', '127.0.0.1']));
$block->appendColumn('optional', Column::create('LowCardinality(Nullable(Int32))', [null, 7, null]));
$block->appendColumn('items', Column::create('SimpleAggregateFunction(groupArrayArray, Array(UInt64))', [[1, 2], [], [3]]));
$client->insert('_test_ext_upstream_types', $block);

$expected = [
    ['id' => 1, 'number' => -42, 'ip' => '127.0.0.1', 'optional' => null, 'items' => [1, 2]],
    ['id' => 2, 'number' => 7, 'ip' => '192.168.1.1', 'optional' => 7, 'items' => []],
    ['id' => 3, 'number' => -42, 'ip' => '127.0.0.1', 'optional' => null, 'items' => [3]],
];
var_dump($client->select('SELECT * FROM _test_ext_upstream_types ORDER BY id') === $expected);
?>
--CLEAN--
<?php
require __DIR__ . '/clickhouse_test.inc';
clickhouse_test_client()->execute('DROP TABLE IF EXISTS _test_ext_upstream_types');
?>
--EXPECT--
bool(true)
