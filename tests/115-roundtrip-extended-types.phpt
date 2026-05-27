--TEST--
Round-trip: FixedString, Enum8, Enum16, Date32, DateTime64, Int128, UInt128
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

$client->execute('DROP TABLE IF EXISTS _test_ext_types2');
$client->execute("CREATE TABLE _test_ext_types2 (
    fs FixedString(8),
    e8 Enum8('a' = 1, 'b' = 2, 'c' = 3),
    e16 Enum16('red' = 100, 'green' = 200, 'blue' = 300),
    d32 Date32,
    dt64 DateTime64(3),
    i128 Int128,
    u128 UInt128
) ENGINE = Memory");

$block = new Block();
$block->appendColumn('fs',   Column::create('FixedString(8)', ['hello', 'world!!!' ]));
$block->appendColumn('e8',   Column::create("Enum8('a' = 1, 'b' = 2, 'c' = 3)", ['a', 'c']));
$block->appendColumn('e16',  Column::create("Enum16('red' = 100, 'green' = 200, 'blue' = 300)", ['green', 'blue']));
$block->appendColumn('d32',  Column::create('Date32', ['2024-06-15', '1925-01-01']));
$block->appendColumn('dt64', Column::create('DateTime64(3)', ['2024-06-15 12:30:45.123', '2000-01-01 00:00:00.000']));
$block->appendColumn('i128', Column::create('Int128', [42, '-170141183460469231731687303715884105728']));
$block->appendColumn('u128', Column::create('UInt128', [0, '340282366920938463463374607431768211455']));

$client->insert('_test_ext_types2', $block);

$rows = $client->select('SELECT * FROM _test_ext_types2 ORDER BY fs');

echo "Row count: " . count($rows) . "\n";

// Row 0 (fs='hello\0\0\0')
echo "\n--- Row 0 ---\n";
echo "fs starts with 'hello': " . (substr($rows[0]['fs'], 0, 5) === 'hello' ? 'yes' : 'no') . "\n";
echo "fs length: " . strlen($rows[0]['fs']) . "\n";
echo "e8: " . $rows[0]['e8'] . "\n";
echo "e16: " . $rows[0]['e16'] . "\n";
echo "d32: " . $rows[0]['d32'] . "\n";
echo "dt64: " . $rows[0]['dt64'] . "\n";
echo "i128: " . $rows[0]['i128'] . "\n";
echo "u128: " . $rows[0]['u128'] . "\n";

// Row 1 (fs='world!!!')
echo "\n--- Row 1 ---\n";
echo "fs: " . $rows[1]['fs'] . "\n";
echo "e8: " . $rows[1]['e8'] . "\n";
echo "e16: " . $rows[1]['e16'] . "\n";
echo "d32: " . $rows[1]['d32'] . "\n";
echo "dt64: " . $rows[1]['dt64'] . "\n";
echo "i128: " . $rows[1]['i128'] . "\n";
echo "u128: " . $rows[1]['u128'] . "\n";

echo "\nDone\n";
?>
--CLEAN--
<?php
require __DIR__ . '/clickhouse_test.inc';
$client = clickhouse_test_client();
try { $client->execute('DROP TABLE IF EXISTS _test_ext_types2'); } catch (\Throwable $e) {}
?>
--EXPECT--
Row count: 2

--- Row 0 ---
fs starts with 'hello': yes
fs length: 8
e8: a
e16: green
d32: 2024-06-15
dt64: 2024-06-15 12:30:45.123
i128: 42
u128: 0

--- Row 1 ---
fs: world!!!
e8: c
e16: blue
d32: 1925-01-01
dt64: 2000-01-01 00:00:00.000
i128: -170141183460469231731687303715884105728
u128: 340282366920938463463374607431768211455

Done
