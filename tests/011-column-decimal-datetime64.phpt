--TEST--
Column::create() with Decimal and DateTime64
--EXTENSIONS--
clickhouse
--FILE--
<?php
use ClickHouse\Driver\Column;

// Decimal — stored as string to preserve precision
$col = Column::create('Decimal(18,4)', ['123.4567', '0.0001', '-99999.9999']);
var_dump($col->size());
var_dump($col->getTypeName());

// DateTime — unix timestamp
$col = Column::create('DateTime', [0, 1704067200]);
var_dump($col->at(0));
var_dump($col->at(1));

// Date — string 'Y-m-d'
$col = Column::create('Date', ['2024-01-01', '1970-01-01']);
var_dump($col->size());

echo "OK\n";
?>
--EXPECTF--
int(3)
string(%d) "Decimal(18,4)"
int(0)
int(1704067200)
int(2)
OK
