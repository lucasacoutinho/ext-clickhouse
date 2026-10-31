--TEST--
Column::create() rejects malformed and excessively nested type names
--EXTENSIONS--
clickhouse
--FILE--
<?php
use ClickHouse\Driver\Column;
use ClickHouse\Driver\Exception\ValidationException;

$nested = static fn (int $depth): string =>
    str_repeat('Array(', $depth) . 'UInt8' . str_repeat(')', $depth);

$typeNames = [
    'trailing type' => 'UInt8,UInt8',
    'unclosed' => 'Array(UInt8',
    'unopened' => 'UInt8)',
    'extra close' => 'Array(UInt8))',
    'depth 600' => $nested(600),
];

foreach ($typeNames as $label => $typeName) {
    try {
        Column::create($typeName, []);
        echo "$label: accepted\n";
    } catch (ValidationException $e) {
        echo "$label: rejected\n";
    }
}

echo 'depth 100: ', Column::create($nested(100), [])->getTypeName() === $nested(100) ? 'accepted' : 'mismatch', "\n";
?>
--EXPECT--
trailing type: rejected
unclosed: rejected
unopened: rejected
extra close: rejected
depth 600: rejected
depth 100: accepted
