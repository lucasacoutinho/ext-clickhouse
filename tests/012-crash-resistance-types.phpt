--TEST--
Crash resistance: wrong types inside arrays and at C boundary
--EXTENSIONS--
clickhouse
--FILE--
<?php
use ClickHouse\Driver\{Block, Column};
use ClickHouse\Driver\Exception\ValidationException;

/*
 * These tests verify the extension does NOT segfault when receiving
 * unexpected types in positions where PHP's arginfo cannot enforce types
 * (e.g., array element types, callback arguments).
 *
 * Each test passes wrong-typed values into C code paths that would
 * crash if type checks are missing. A successful test means "no segfault".
 */

// --- Column::create with wrong element types ---

// String column with non-string values (should coerce via zval_get_string, not crash)
$col = Column::create('String', [42, true, 3.14, null]);
echo "OK: String with mixed types, size=" . $col->size() . "\n";

// UInt64 column with string values (should coerce via zval_get_long)
$col = Column::create('UInt64', ['123', '456']);
echo "OK: UInt64 with strings, size=" . $col->size() . "\n";

// Float64 column with string values
$col = Column::create('Float64', ['1.5', '2.5']);
echo "OK: Float64 with strings, size=" . $col->size() . "\n";

// Array column with non-array element
try {
    $col = Column::create('Array(UInt32)', ['not_an_array']);
    echo "FAIL: should have thrown for non-array element\n";
} catch (ValidationException $e) {
    echo "OK: Array rejects non-array element\n";
}

// Tuple with wrong element count
try {
    $col = Column::create('Tuple(UInt32, String)', [[1]]);
    echo "FAIL: should have thrown for wrong tuple size\n";
} catch (ValidationException $e) {
    echo "OK: Tuple rejects wrong element count\n";
}

// Nullable with various inner types
$col = Column::create('Nullable(String)', [null, 'hello', null]);
echo "OK: Nullable(String), size=" . $col->size() . "\n";

// Map with non-array element
try {
    $col = Column::create('Map(String, UInt32)', ['not_an_array']);
    echo "FAIL: should have thrown for non-array map element\n";
} catch (ValidationException $e) {
    echo "OK: Map rejects non-array element\n";
}

// --- Block edge cases ---

// Empty block operations
$block = new Block();
var_dump($block->getColumnCount());
var_dump($block->getRowCount());
var_dump($block->toArray());

// Block::getColumn with out-of-range index on empty block
try {
    $block->getColumn(0);
    echo "FAIL: should throw on empty block getColumn(0)\n";
} catch (ValidationException $e) {
    echo "OK: empty block getColumn(0) throws\n";
}

// Block::getColumnName with out-of-range
try {
    $block->getColumnName(0);
    echo "FAIL: should throw\n";
} catch (ValidationException $e) {
    echo "OK: empty block getColumnName(0) throws\n";
}

// Block::getColumnType with out-of-range
try {
    $block->getColumnType(0);
    echo "FAIL: should throw\n";
} catch (ValidationException $e) {
    echo "OK: empty block getColumnType(0) throws\n";
}

// Block::getColumnTypeName with out-of-range
try {
    $block->getColumnTypeName(0);
    echo "FAIL: should throw\n";
} catch (ValidationException $e) {
    echo "OK: empty block getColumnTypeName(0) throws\n";
}

// Negative indices
try {
    $block->getColumn(-1);
} catch (ValidationException $e) {
    echo "OK: negative getColumn index throws\n";
}

// --- Column::at edge cases ---

$col = Column::create('UInt8', []);
var_dump($col->size());
try {
    $col->at(0);
    echo "FAIL: at(0) on empty column should throw\n";
} catch (ValidationException $e) {
    echo "OK: at(0) on empty column throws\n";
}

// IPv6 with invalid string
try {
    $col = Column::create('IPv6', ['not_an_ip']);
    echo "FAIL: should have thrown for invalid IPv6\n";
} catch (ValidationException $e) {
    echo "OK: IPv6 rejects invalid address\n";
}

// IPv6 with non-string type
try {
    $col = Column::create('IPv6', [42]);
    echo "FAIL: should have thrown for non-string IPv6\n";
} catch (ValidationException $e) {
    echo "OK: IPv6 rejects non-string value\n";
}

// Point with non-array
try {
    $col = Column::create('Point', ['not_an_array']);
    echo "FAIL: should have thrown for non-array Point\n";
} catch (ValidationException $e) {
    echo "OK: Point rejects non-array\n";
}

// Point with missing indices
try {
    $col = Column::create('Point', [['x' => 1.0]]);
    echo "FAIL: should have thrown for Point without index 0/1\n";
} catch (ValidationException $e) {
    echo "OK: Point rejects array without indices 0/1\n";
}

echo "Done\n";
?>
--EXPECT--
OK: String with mixed types, size=4
OK: UInt64 with strings, size=2
OK: Float64 with strings, size=2
OK: Array rejects non-array element
OK: Tuple rejects wrong element count
OK: Nullable(String), size=3
OK: Map rejects non-array element
int(0)
int(0)
array(0) {
}
OK: empty block getColumn(0) throws
OK: empty block getColumnName(0) throws
OK: empty block getColumnType(0) throws
OK: empty block getColumnTypeName(0) throws
OK: negative getColumn index throws
int(0)
OK: at(0) on empty column throws
OK: IPv6 rejects invalid address
OK: IPv6 rejects non-string value
OK: Point rejects non-array
OK: Point rejects array without indices 0/1
Done
