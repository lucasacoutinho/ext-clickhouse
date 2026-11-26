--TEST--
Array materialization preserves offsets, nested values and scalar types
--EXTENSIONS--
clickhouse
--FILE--
<?php
use ClickHouse\Driver\Column;

$cases = [
    'Array(Int8)' => [[-2, 3], [], [4, -5]],
    'Array(Int16)' => [[-200, 300], [], [400, -500]],
    'Array(Int32)' => [[-200, 300], [], [400, -500]],
    'Array(Int64)' => [[-200, 300], [], [400, -500]],
    'Array(UInt8)' => [[1, 2], [], [3, 4]],
    'Array(UInt16)' => [[1, 2], [], [3, 4]],
    'Array(UInt32)' => [[1, 2], [], [3, 4]],
    'Array(UInt64)' => [[1, '18446744073709551615'], [], [3, 4]],
    'Array(Float32)' => [[1.5, -2.25], [], [3.5]],
    'Array(Float64)' => [[1.5, -2.25], [], [3.5]],
    'Array(String)' => [["a\0b", ''], [], ['next', "\0"]],
    'Array(FixedString(3))' => [["a\0b", 'xyz'], [], ['def']],
    'Array(Nullable(Int32))' => [[null, 1], [], [2, null]],
    'Array(Array(UInt32))' => [[[1, 2], [], [3]], [], [[4], [5, 6]]],
    'Array(Tuple(String, UInt32))' => [[['a', 1]], [], [['b', 2], ['c', 3]]],
    'Map(String, Array(UInt32))' => [['a' => [1, 2]], [], ['b' => [], 'c' => [3]]],
];

foreach ($cases as $type => $values) {
    $column = Column::create($type, $values);
    $actual = [$column->at(2), $column->at(0), $column->at(1)];
    $expected = [$values[2], $values[0], $values[1]];
    echo $type, ': ', $actual === $expected ? 'OK' : 'FAIL', "\n";
}
?>
--EXPECT--
Array(Int8): OK
Array(Int16): OK
Array(Int32): OK
Array(Int64): OK
Array(UInt8): OK
Array(UInt16): OK
Array(UInt32): OK
Array(UInt64): OK
Array(Float32): OK
Array(Float64): OK
Array(String): OK
Array(FixedString(3)): OK
Array(Nullable(Int32)): OK
Array(Array(UInt32)): OK
Array(Tuple(String, UInt32)): OK
Map(String, Array(UInt32)): OK
