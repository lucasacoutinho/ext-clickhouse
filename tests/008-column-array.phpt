--TEST--
Column::create() with Array types
--EXTENSIONS--
clickhouse
--FILE--
<?php
use ClickHouse\Driver\Column;

$col = Column::create('Array(UInt32)', [[1, 2, 3], [4, 5], []]);
var_dump($col->size());

// First array
$arr = $col->at(0);
var_dump($arr);

// Empty array
$arr = $col->at(2);
var_dump($arr);

// Array of strings
$col = Column::create('Array(String)', [['a', 'b'], ['c']]);
var_dump($col->at(0));
var_dump($col->at(1));

echo "OK\n";
?>
--EXPECT--
int(3)
array(3) {
  [0]=>
  int(1)
  [1]=>
  int(2)
  [2]=>
  int(3)
}
array(0) {
}
array(2) {
  [0]=>
  string(1) "a"
  [1]=>
  string(1) "b"
}
array(1) {
  [0]=>
  string(1) "c"
}
OK
