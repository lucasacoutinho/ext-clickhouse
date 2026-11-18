--TEST--
Extreme DateTime64 values that gmtime_r cannot represent are rejected
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php
if (PHP_INT_SIZE < 8) {
    die('skip requires 64-bit PHP integers');
}
?>
--FILE--
<?php
use ClickHouse\Driver\Column;
use ClickHouse\Driver\Exception\ClickHouseException;

$normal = Column::create('DateTime64(0)', [0]);
echo 'epoch=' . $normal->at(0) . "\n";

$precise = Column::create('DateTime64(3)', [1710500000000]);
echo 'precise=' . $precise->at(0) . "\n";

/* These still fit in struct tm, so the formatted civil time is kept. */
$maxMillis = Column::create('DateTime64(3)', [PHP_INT_MAX]);
echo 'max millis prefix=' . substr($maxMillis->at(0), 0, 10) . "\n";

$rejected = 0;
$cases = [
    ['DateTime64(0)', PHP_INT_MAX],
    ['DateTime64(0)', PHP_INT_MIN],
    ['Nullable(DateTime64(0))', PHP_INT_MAX],
    ['Array(DateTime64(0))', [PHP_INT_MAX]],
];

foreach ($cases as $case) {
    $type = $case[0];
    $value = $case[1];
    try {
        $column = Column::create($type, [$value]);
        $column->at(0);
        echo "FAIL: $type accepted\n";
    } catch (ClickHouseException $e) {
        if ($e->getMessage() === 'DateTime value is outside the representable range') {
            $rejected++;
        } else {
            echo "FAIL: $type: " . $e->getMessage() . "\n";
        }
    }
}

$mixed = Column::create('DateTime64(0)', [0, PHP_INT_MAX]);
try {
    $mixed->toArray();
    echo "FAIL: mixed toArray accepted\n";
} catch (ClickHouseException $e) {
    echo 'mixed=' . $e->getMessage() . "\n";
}
echo 'still readable=' . $mixed->at(0) . "\n";
echo "rejected=$rejected\n";
?>
--EXPECT--
epoch=1970-01-01 00:00:00.0
precise=2024-03-15 10:53:20.000
max millis prefix=292278994-
mixed=DateTime value is outside the representable range
still readable=1970-01-01 00:00:00.0
rejected=4
