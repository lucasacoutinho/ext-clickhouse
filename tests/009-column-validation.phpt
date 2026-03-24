--TEST--
Column::create() error handling
--EXTENSIONS--
clickhouse
--FILE--
<?php
use ClickHouse\Driver\Column;
use ClickHouse\Driver\Exception\ValidationException;

// Unknown type
try {
    Column::create('NotAType', [1]);
    echo "FAIL: should have thrown\n";
} catch (ValidationException $e) {
    echo "OK: " . $e->getMessage() . "\n";
}

// Out of range index
$col = Column::create('UInt8', [1, 2]);
try {
    $col->at(5);
    echo "FAIL: should have thrown\n";
} catch (ValidationException $e) {
    echo "OK: index out of range\n";
}

try {
    $col->at(-1);
    echo "FAIL: should have thrown\n";
} catch (ValidationException $e) {
    echo "OK: negative index\n";
}

echo "Done\n";
?>
--EXPECT--
OK: Unknown ClickHouse type: NotAType
OK: index out of range
OK: negative index
Done
