--TEST--
Column types: FixedString, Enum8, Enum16
--EXTENSIONS--
clickhouse
--FILE--
<?php
use ClickHouse\Driver\Column;
use ClickHouse\Driver\Type;

// --- FixedString ---
$col = Column::create('FixedString(5)', ['hello', 'world', 'ab']);
echo "FixedString size: " . $col->size() . "\n";
echo "FixedString type: " . $col->getTypeName() . "\n";
var_dump($col->getType() === Type::FixedString);

// FixedString values - shorter strings get null-padded
$v0 = $col->at(0);
echo "val[0] length: " . strlen($v0) . "\n";
echo "val[0]: " . $v0 . "\n";

$v2 = $col->at(2);
echo "val[2] raw length: " . strlen($v2) . "\n";
// 'ab' padded to 5 bytes with nulls
echo "val[2] starts with 'ab': " . (str_starts_with($v2, 'ab') ? 'yes' : 'no') . "\n";

// toArray
$arr = $col->toArray();
echo "toArray count: " . count($arr) . "\n";

// --- Enum8 ---
$col = Column::create("Enum8('apple' = 1, 'banana' = 2, 'cherry' = 3)", ['apple', 'banana', 'cherry', 'apple']);
echo "\nEnum8 size: " . $col->size() . "\n";
echo "Enum8 type enum: " . ($col->getType() === Type::Enum8 ? 'Enum8' : 'other') . "\n";
var_dump($col->at(0));
var_dump($col->at(1));
var_dump($col->at(2));
var_dump($col->at(3));

// --- Enum16 ---
$col = Column::create("Enum16('red' = 100, 'green' = 200, 'blue' = 300)", ['green', 'blue', 'red']);
echo "\nEnum16 size: " . $col->size() . "\n";
echo "Enum16 type enum: " . ($col->getType() === Type::Enum16 ? 'Enum16' : 'other') . "\n";
var_dump($col->at(0));
var_dump($col->at(1));
var_dump($col->at(2));

echo "\nDone\n";
?>
--EXPECT--
FixedString size: 3
FixedString type: FixedString(5)
bool(true)
val[0] length: 5
val[0]: hello
val[2] raw length: 5
val[2] starts with 'ab': yes
toArray count: 3

Enum8 size: 4
Enum8 type enum: Enum8
string(5) "apple"
string(6) "banana"
string(6) "cherry"
string(5) "apple"

Enum16 size: 3
Enum16 type enum: Enum16
string(5) "green"
string(4) "blue"
string(3) "red"

Done
