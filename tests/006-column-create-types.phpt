--TEST--
Column::create() with various types
--EXTENSIONS--
clickhouse
--FILE--
<?php
use ClickHouse\Driver\Column;
use ClickHouse\Driver\Type;

// Integer types
$col = Column::create('UInt8', [1, 2, 255]);
var_dump($col->size());
var_dump($col->at(0));
var_dump($col->at(2));
var_dump($col->getTypeName());
var_dump($col->getType() === Type::UInt8);

// Int64
$col = Column::create('Int64', [-100, 0, PHP_INT_MAX]);
var_dump($col->at(0));
var_dump($col->at(2));

// Float64
$col = Column::create('Float64', [3.14, -2.718, 0.0]);
var_dump($col->at(0));
var_dump($col->at(2));

// String
$col = Column::create('String', ['hello', 'world', '']);
var_dump($col->size());
var_dump($col->at(0));
var_dump($col->at(2));

// toArray
$col = Column::create('UInt32', [100, 200]);
var_dump($col->toArray());
?>
--EXPECTF--
int(3)
int(1)
int(255)
string(5) "UInt8"
bool(true)
int(-100)
int(%d)
float(3.14)
float(0)
int(3)
string(5) "hello"
string(0) ""
array(2) {
  [0]=>
  int(100)
  [1]=>
  int(200)
}
