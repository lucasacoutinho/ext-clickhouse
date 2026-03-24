--TEST--
Column types: Int128, UInt128
--EXTENSIONS--
clickhouse
--FILE--
<?php
use ClickHouse\Driver\Column;
use ClickHouse\Driver\Type;

// --- Int128 ---
// Int128 accepts both int and string representations
$col = Column::create('Int128', [0, 42, -1, '170141183460469231731687303715884105727', '-170141183460469231731687303715884105728']);
echo "Int128 size: " . $col->size() . "\n";
echo "Int128 type: " . ($col->getType() === Type::Int128 ? 'Int128' : 'other') . "\n";

// Values come back as strings (too large for PHP int)
var_dump($col->at(0)); // "0"
var_dump($col->at(1)); // "42"
var_dump($col->at(2)); // "-1"
// Max Int128
echo "val[3]: " . $col->at(3) . "\n";
// Min Int128
echo "val[4]: " . $col->at(4) . "\n";

// --- UInt128 ---
$col = Column::create('UInt128', [0, 255, '340282366920938463463374607431768211455']);
echo "\nUInt128 size: " . $col->size() . "\n";
echo "UInt128 type: " . ($col->getType() === Type::UInt128 ? 'UInt128' : 'other') . "\n";

var_dump($col->at(0)); // "0"
var_dump($col->at(1)); // "255"
// Max UInt128
echo "val[2]: " . $col->at(2) . "\n";

// Small Int128 values that fit in a PHP long can be passed as int
$col = Column::create('Int128', [100]);
echo "\nSmall Int128: " . $col->at(0) . "\n";

echo "Done\n";
?>
--EXPECT--
Int128 size: 5
Int128 type: Int128
string(1) "0"
string(2) "42"
string(2) "-1"
val[3]: 170141183460469231731687303715884105727
val[4]: -170141183460469231731687303715884105728

UInt128 size: 3
UInt128 type: UInt128
string(1) "0"
string(3) "255"
val[2]: 340282366920938463463374607431768211455

Small Int128: 100
Done
