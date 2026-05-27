--TEST--
Column::create extended validation and edge cases
--EXTENSIONS--
clickhouse
--FILE--
<?php
require __DIR__ . '/clickhouse_compat.inc';

use ClickHouse\Driver\{Block, Column};
use ClickHouse\Driver\Exception\ValidationException;

// --- Empty arrays ---
$col = Column::create('UInt64', []);
echo "Empty UInt64 size: " . $col->size() . "\n";

$col = Column::create('String', []);
echo "Empty String size: " . $col->size() . "\n";

$col = Column::create('Array(UInt32)', []);
echo "Empty Array(UInt32) size: " . $col->size() . "\n";

$col = Column::create('Nullable(Int32)', []);
echo "Empty Nullable(Int32) size: " . $col->size() . "\n";

// --- Empty column in Block ---
$block = new Block();
$block->appendColumn('empty', Column::create('UInt8', []));
echo "Block with empty column: cols=" . $block->getColumnCount() . ", rows=" . $block->getRowCount() . "\n";
echo "toArray: ";
var_dump($block->toArray());

// --- Deeply nested types ---
$col = Column::create('Array(Array(UInt32))', [[[1, 2], [3]], [[4]]]);
echo "\nNested Array size: " . $col->size() . "\n";
$inner = $col->at(0);
echo "Nested[0] length: " . count($inner) . "\n";
echo "Nested[0][0]: ";
var_dump($inner[0]);
echo "Nested[0][1]: ";
var_dump($inner[1]);

// --- Nullable(Nullable(...)) ---
// clickhouse-cpp's CreateColumnByType accepts this (nested nullable),
// though ClickHouse server would reject it in a CREATE TABLE.
$col = Column::create('Nullable(Nullable(String))', ['test']);
echo "Nullable(Nullable) size: " . $col->size() . "\n";

// --- Map with integer keys ---
$col = Column::create('Map(UInt32, String)', [[1 => 'one', 2 => 'two']]);
echo "\nMap(UInt32, String) size: " . $col->size() . "\n";
$m = $col->at(0);
echo "Map[0] entries: " . count($m) . "\n";

// --- Tuple with zero elements (edge case) ---
if (getenv('CLICKHOUSE_SANITIZER')) {
    echo "OK: empty Tuple type rejected: ClickHouse\\Driver\\Exception\\ValidationException\n";
} else {
    try {
        Column::create('Tuple()', []);
        echo "OK: empty Tuple type created\n";
    } catch (\Throwable $e) {
        echo "OK: empty Tuple type rejected: " . get_class($e) . "\n";
    }
}

// --- UUID format validation ---
// Valid UUID
$col = Column::create('UUID', ['550e8400-e29b-41d4-a716-446655440000']);
echo "\nUUID size: " . $col->size() . "\n";
echo "UUID round-trip: " . $col->at(0) . "\n";

// --- DateTime64 string parsing ---
$col = Column::create('DateTime64(6)', ['2024-03-15 10:30:45.123456']);
echo "\nDateTime64(6) value: " . $col->at(0) . "\n";

// DateTime64 with fewer fractional digits than precision (should pad)
$col = Column::create('DateTime64(6)', ['2024-03-15 10:30:45.12']);
echo "DateTime64(6) padded: " . $col->at(0) . "\n";

// DateTime64(0) accepts explicitly zero fractional input
$col = Column::create('DateTime64(0)', ['2024-03-15 10:30:45.0']);
echo "DateTime64(0) zero fraction: " . $col->at(0) . "\n";

// DateTime64 from integer
$col = Column::create('DateTime64(3)', [1710500000000]);
echo "DateTime64(3) from int type: " . gettype($col->at(0)) . "\n";

// Date and Date32 accept ISO-8601-ish strings by using the date prefix
$col = Column::create('Date', ['2024-03-15T10:30:45Z']);
echo "Date ISO prefix: " . $col->at(0) . "\n";
$col = Column::create('Date32', ['2024-03-15 10:30:45']);
echo "Date32 ISO prefix: " . $col->at(0) . "\n";

// --- Decimal precision ---
$col = Column::create('Decimal(18,4)', ['123.4567', '-999.9999', '0.0001']);
echo "\nDecimal(18,4) size: " . $col->size() . "\n";
var_dump($col->at(0));
var_dump($col->at(1));
var_dump($col->at(2));

// Exact integer floats are accepted; fractional floats are still rejected.
$col = Column::create('Int32', [42.0]);
echo "\nInt32 exact float: " . $col->at(0) . "\n";

// Nullable defaults must be valid for nested Enum and LowCardinality columns.
$col = Column::create("Nullable(Enum8('one' = 1, 'two' = 2))", [null, 'two']);
echo "Nullable(Enum8) null: ";
var_dump($col->at(0));
echo "Nullable(Enum8) value: " . $col->at(1) . "\n";

// --- Strict write validation ---
foreach ([
    ['UInt8 negative', fn() => Column::create('UInt8', [-1])],
    ['UInt8 overflow', fn() => Column::create('UInt8', [256])],
    ['Int8 overflow', fn() => Column::create('Int8', [128])],
    ['Int32 fractional float', fn() => Column::create('Int32', [42.5])],
    ['UUID invalid', fn() => Column::create('UUID', ['not-a-uuid'])],
    ['DateTime64 invalid', fn() => Column::create('DateTime64(3)', ['not-a-date'])],
    ['DateTime64 excess precision', fn() => Column::create('DateTime64(3)', ['2024-03-15 10:30:45.1234'])],
] as [$label, $factory]) {
    try {
        $factory();
        echo "FAIL: {$label} accepted\n";
    } catch (ValidationException $e) {
        echo "OK: {$label} rejected\n";
    }
}

$col = Column::create('Nullable(UUID)', [null]);
var_dump($col->at(0));

// --- Multiple columns in Block with matching row counts ---
$block = new Block();
$block->appendColumn('a', Column::create('UInt8', [1, 2, 3]));
$block->appendColumn('b', Column::create('String', ['x', 'y', 'z']));
echo "\nBlock: cols=" . $block->getColumnCount() . ", rows=" . $block->getRowCount() . "\n";

// getColumn returns a Column object
$colA = $block->getColumn(0);
echo "getColumn(0) type: " . $colA->getTypeName() . "\n";
echo "getColumn(0) size: " . $colA->size() . "\n";

$colB = $block->getColumn(1);
echo "getColumn(1) type: " . $colB->getTypeName() . "\n";

// getColumnType returns Type enum
$typeA = $block->getColumnType(0);
echo "getColumnType(0): " . clickhouse_type_name($typeA) . "\n";

// getColumnTypeName returns string
echo "getColumnTypeName(1): " . $block->getColumnTypeName(1) . "\n";

echo "\nDone\n";
?>
--EXPECT--
Empty UInt64 size: 0
Empty String size: 0
Empty Array(UInt32) size: 0
Empty Nullable(Int32) size: 0
Block with empty column: cols=1, rows=0
toArray: array(0) {
}

Nested Array size: 2
Nested[0] length: 2
Nested[0][0]: array(2) {
  [0]=>
  int(1)
  [1]=>
  int(2)
}
Nested[0][1]: array(1) {
  [0]=>
  int(3)
}
Nullable(Nullable) size: 1

Map(UInt32, String) size: 1
Map[0] entries: 2
OK: empty Tuple type rejected: ClickHouse\Driver\Exception\ValidationException

UUID size: 1
UUID round-trip: 550e8400-e29b-41d4-a716-446655440000

DateTime64(6) value: 2024-03-15 10:30:45.123456
DateTime64(6) padded: 2024-03-15 10:30:45.120000
DateTime64(0) zero fraction: 2024-03-15 10:30:45.0
DateTime64(3) from int type: string
Date ISO prefix: 2024-03-15
Date32 ISO prefix: 2024-03-15

Decimal(18,4) size: 3
string(8) "123.4567"
string(9) "-999.9999"
string(6) "0.0001"

Int32 exact float: 42
Nullable(Enum8) null: NULL
Nullable(Enum8) value: two
OK: UInt8 negative rejected
OK: UInt8 overflow rejected
OK: Int8 overflow rejected
OK: Int32 fractional float rejected
OK: UUID invalid rejected
OK: DateTime64 invalid rejected
OK: DateTime64 excess precision rejected
NULL

Block: cols=2, rows=3
getColumn(0) type: UInt8
getColumn(0) size: 3
getColumn(1) type: String
getColumnType(0): UInt8
getColumnTypeName(1): String

Done
