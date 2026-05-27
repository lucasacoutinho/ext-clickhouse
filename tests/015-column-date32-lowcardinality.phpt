--TEST--
Column types: Date32, LowCardinality
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php
if (getenv('CLICKHOUSE_SANITIZER')) {
    die('skip LowCardinality constructor hits known clickhouse-cpp sanitizer issue');
}
?>
--FILE--
<?php
use ClickHouse\Driver\Column;
use ClickHouse\Driver\Type;

// --- Date32 ---
// Date32 has extended range compared to Date (supports dates before 1970 and after 2149)
$col = Column::create('Date32', ['2024-06-15', '1900-01-01', '2299-12-31']);
echo "Date32 size: " . $col->size() . "\n";
echo "Date32 type: " . ($col->getType() === Type::Date32 ? 'Date32' : 'other') . "\n";
var_dump($col->at(0));
var_dump($col->at(1));
var_dump($col->at(2));

// Date32 from numeric timestamp
$col = Column::create('Date32', [0]);
echo "Date32 epoch: " . $col->at(0) . "\n";

// --- LowCardinality(String) ---
// LowCardinality is a dictionary-encoded optimization; should be transparent to PHP
$col = Column::create('LowCardinality(String)', ['hello', 'world', 'hello', 'hello', 'world']);
echo "\nLC(String) size: " . $col->size() . "\n";
echo "LC type: " . ($col->getType() === Type::LowCardinality ? 'LowCardinality' : 'other') . "\n";
echo "LC typeName: " . $col->getTypeName() . "\n";
var_dump($col->at(0));
var_dump($col->at(1));
var_dump($col->at(2));
var_dump($col->at(3));
var_dump($col->at(4));

// --- LowCardinality(FixedString) ---
$col = Column::create('LowCardinality(FixedString(3))', ['abc', 'def', 'abc']);
echo "\nLC(FixedString) size: " . $col->size() . "\n";
var_dump($col->at(0));
var_dump($col->at(2));

// --- LowCardinality(Nullable(String)) ---
$col = Column::create('LowCardinality(Nullable(String))', ['hello', null, 'hello', null]);
echo "\nLC(Nullable(String)) size: " . $col->size() . "\n";
var_dump($col->at(0));
var_dump($col->at(1));
var_dump($col->at(2));
var_dump($col->at(3));

echo "\nDone\n";
?>
--EXPECT--
Date32 size: 3
Date32 type: Date32
string(10) "2024-06-15"
string(10) "1900-01-01"
string(10) "2299-12-31"
Date32 epoch: 1970-01-01

LC(String) size: 5
LC type: LowCardinality
LC typeName: LowCardinality(String)
string(5) "hello"
string(5) "world"
string(5) "hello"
string(5) "hello"
string(5) "world"

LC(FixedString) size: 3
string(3) "abc"
string(3) "abc"

LC(Nullable(String)) size: 4
string(5) "hello"
NULL
string(5) "hello"
NULL

Done
