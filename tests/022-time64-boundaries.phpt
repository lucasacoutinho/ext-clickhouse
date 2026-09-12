--TEST--
Time64 preserves signed 64-bit values outside the PHP integer range
--EXTENSIONS--
clickhouse
--FILE--
<?php
use ClickHouse\Driver\Column;

$values = [
    '-9223372036854775808',
    '-2147483649',
    (string) PHP_INT_MIN,
    '-1',
    '0',
    '1',
    (string) PHP_INT_MAX,
    '2147483648',
    '9223372036854775807',
];

$column = Column::create('Time64(9)', $values);
$actual = $column->toArray();
$expectedTypes = PHP_INT_SIZE >= 8
    ? array_fill(0, count($values), 'integer')
    : ['string', 'string', 'integer', 'integer', 'integer', 'integer', 'integer', 'string', 'string'];
echo "string values: ";
var_dump(array_map(static fn($value): string => (string) $value, $actual) === $values);

echo "value types: ";
var_dump(array_map('gettype', $actual) === $expectedTypes);

$indexedValues = [];
foreach (array_keys($values) as $index) {
    $indexedValues[] = (string) $column->at($index);
}
echo "at() values: ";
var_dump($indexedValues === $values);
?>
--EXPECT--
string values: bool(true)
value types: bool(true)
at() values: bool(true)
