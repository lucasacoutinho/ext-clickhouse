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
Bool rejected
JSON rejected
Time rejected
