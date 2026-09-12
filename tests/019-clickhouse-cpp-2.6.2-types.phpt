--TEST--
clickhouse-cpp 2.6.2 version and Bool, JSON, Time, Time64 columns
--EXTENSIONS--
clickhouse
--FILE--
<?php
require __DIR__ . '/clickhouse_compat.inc';

use ClickHouse\Driver\Column;
use ClickHouse\Driver\Exception\ValidationException;

ob_start();
phpinfo(INFO_MODULES);
$moduleInfo = ob_get_clean();
echo strpos($moduleInfo, '2.6.2') !== false ? "cpp 2.6.2\n" : "wrong cpp version\n";

$bool = Column::create('Bool', [true, false, 1, 0]);
echo $bool->getTypeName() . ' ' . clickhouse_type_name($bool->getType()) . "\n";
var_dump($bool->toArray());

$json = Column::create('JSON', ['{"answer":42}', '[true,false]']);
echo $json->getTypeName() . ' ' . clickhouse_type_name($json->getType()) . "\n";
var_dump($json->toArray());

$time = Column::create('Time', [-3600, 0, 3661]);
echo $time->getTypeName() . ' ' . clickhouse_type_name($time->getType()) . "\n";
var_dump($time->toArray());

$time64 = Column::create('Time64(3)', [-1, 0, 1234567]);
echo $time64->getTypeName() . ' ' . clickhouse_type_name($time64->getType()) . "\n";
var_dump($time64->toArray());

$wideTime64 = Column::create('Time64(3)', ['3000000000', '-3000000000']);
$time64Values = $wideTime64->toArray();
echo "Time64 wide values preserved: ";
var_dump(
    (string) $time64Values[0] === '3000000000'
    && (string) $time64Values[1] === '-3000000000'
    && gettype($time64Values[0]) === (PHP_INT_SIZE >= 8 ? 'integer' : 'string')
    && gettype($time64Values[1]) === (PHP_INT_SIZE >= 8 ? 'integer' : 'string')
);

$lowCardinalityTime64 = Column::create(
    'LowCardinality(Time64(3))',
    ['3000000000', '-3000000000', '3000000000']
);
$lowCardinalityValues = $lowCardinalityTime64->toArray();
echo "LowCardinality Time64 wide values preserved: ";
var_dump(
    array_map('strval', $lowCardinalityValues) === ['3000000000', '-3000000000', '3000000000']
    && gettype($lowCardinalityValues[0]) === (PHP_INT_SIZE >= 8 ? 'integer' : 'string')
    && gettype($lowCardinalityValues[1]) === (PHP_INT_SIZE >= 8 ? 'integer' : 'string')
);

foreach (
    [
        ['Bool', [2]],
        ['JSON', ['']],
        ['Time', [2147483648]],
    ] as [$type, $values]
) {
    try {
        Column::create($type, $values);
        echo "not rejected\n";
    } catch (ValidationException $e) {
        echo $type . " rejected\n";
    }
}
?>
--EXPECT--
cpp 2.6.2
Bool Bool
array(4) {
  [0]=>
  bool(true)
  [1]=>
  bool(false)
  [2]=>
  bool(true)
  [3]=>
  bool(false)
}
JSON JSON
array(2) {
  [0]=>
  string(13) "{"answer":42}"
  [1]=>
  string(12) "[true,false]"
}
Time Time
array(3) {
  [0]=>
  int(-3600)
  [1]=>
  int(0)
  [2]=>
  int(3661)
}
Time64(3) Time64
array(3) {
  [0]=>
  int(-1)
  [1]=>
  int(0)
  [2]=>
  int(1234567)
}
Time64 wide values preserved: bool(true)
LowCardinality Time64 wide values preserved: bool(true)
Bool rejected
JSON rejected
Time rejected
