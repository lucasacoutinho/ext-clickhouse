--TEST--
FixedString width of zero or above the server limit is rejected
--EXTENSIONS--
clickhouse
--FILE--
<?php
use ClickHouse\Driver\Column;
use ClickHouse\Driver\Exception\ValidationException;

$valid = Column::create('FixedString(1)', ['a']);
echo 'FixedString(1) size=' . $valid->size() . "\n";

$cases = [
    'FixedString(0)',
    'LowCardinality(FixedString(0))',
    'FixedString(16777216)',
];

foreach ($cases as $type) {
    try {
        $column = Column::create($type, []);
        echo $type . ' accepted size=' . $column->size() . "\n";
    } catch (ValidationException $e) {
        $message = $e->getMessage();
        $bounded = strpos($message, 'FixedString size must be between 1 and 16777215') !== false;
        echo $type . ($bounded ? " rejected\n" : " unexpected: $message\n");
    }
}
?>
--EXPECT--
FixedString(1) size=1
FixedString(0) rejected
LowCardinality(FixedString(0)) rejected
FixedString(16777216) rejected
