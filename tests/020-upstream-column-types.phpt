--TEST--
Upstream LowCardinality dictionary types and nested SimpleAggregateFunction columns
--EXTENSIONS--
clickhouse
--FILE--
<?php
use ClickHouse\Driver\Column;

$cases = [
    'LowCardinality(Int32)' => [-42, 0, -42, 7],
    'LowCardinality(UInt64)' => [0, '18446744073709551615', 0],
    'LowCardinality(Float64)' => [1.5, -2.25, 1.5],
    'LowCardinality(Date)' => ['1970-01-01', '2024-06-15', '1970-01-01'],
    'LowCardinality(Date32)' => ['1900-01-01', '2299-12-31', '1900-01-01'],
    'LowCardinality(UUID)' => ['550e8400-e29b-41d4-a716-446655440000', '00000000-0000-0000-0000-000000000000'],
    'LowCardinality(IPv4)' => ['127.0.0.1', '192.168.1.1', '127.0.0.1'],
    'LowCardinality(IPv6)' => ['::1', '2001:db8::1', '::1'],
    'LowCardinality(Nullable(Int32))' => [null, -42, null, 0, -42],
    'SimpleAggregateFunction(groupArrayArray, Array(UInt64))' => [[1, 2], [], [3]],
];

foreach ($cases as $type => $values) {
    $column = Column::create($type, $values);
    echo $type . ': ' . ($column->toArray() === $values ? 'OK' : 'FAIL') . "\n";
}
?>
--EXPECT--
LowCardinality(Int32): OK
LowCardinality(UInt64): OK
LowCardinality(Float64): OK
LowCardinality(Date): OK
LowCardinality(Date32): OK
LowCardinality(UUID): OK
LowCardinality(IPv4): OK
LowCardinality(IPv6): OK
LowCardinality(Nullable(Int32)): OK
SimpleAggregateFunction(groupArrayArray, Array(UInt64)): OK
