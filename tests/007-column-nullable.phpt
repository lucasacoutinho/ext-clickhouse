--TEST--
Column::create() with Nullable types
--EXTENSIONS--
clickhouse
--FILE--
<?php
use ClickHouse\Driver\Column;

$col = Column::create('Nullable(String)', ['hello', null, 'world']);
var_dump($col->size());
var_dump($col->at(0));
var_dump($col->at(1));
var_dump($col->at(2));
var_dump($col->getTypeName());

$col = Column::create('Nullable(Int32)', [42, null, -1]);
var_dump($col->at(0));
var_dump($col->at(1));
var_dump($col->at(2));

echo "OK\n";
?>
--EXPECT--
int(3)
string(5) "hello"
NULL
string(5) "world"
string(16) "Nullable(String)"
int(42)
NULL
int(-1)
OK
