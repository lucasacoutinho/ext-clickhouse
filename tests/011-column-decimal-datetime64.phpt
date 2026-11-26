--TEST--
Column::create() with Decimal and DateTime64
--EXTENSIONS--
clickhouse
--FILE--
<?php
use ClickHouse\Driver\Column;

// Store Decimal as a string to preserve precision.
$col = Column::create('Decimal(18,4)', ['123.4567', '0.0001', '-99999.9999']);
var_dump($col->size());
var_dump($col->getTypeName());

$cases = [
    'Decimal(9,4)' => ['0.0000', '-0.0001', '12345.6789'],
    'Decimal(18,0)' => ['-999999999999999999', '0', '999999999999999999'],
    'Decimal(38,38)' => ['-0.'.str_repeat('9', 38), '0.'.str_repeat('0', 37).'1'],
    'Decimal(38,3)' => [str_repeat('9', 35).'.999', '-'.str_repeat('9', 35).'.999'],
];
foreach ($cases as $type => $values) {
    var_dump(Column::create($type, $values)->toArray() === $values);
}

// DateTime uses a Unix timestamp.
$col = Column::create('DateTime', [0, 1704067200]);
var_dump($col->at(0));
var_dump($col->at(1));

// Date uses a 'Y-m-d' string.
$col = Column::create('Date', ['2024-01-01', '1970-01-01']);
var_dump($col->size());

echo "OK\n";
?>
--EXPECTF--
int(3)
string(%d) "Decimal(18,4)"
bool(true)
bool(true)
bool(true)
bool(true)
int(0)
int(1704067200)
int(2)
OK
