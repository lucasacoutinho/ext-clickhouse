--TEST--
Native value conversion rejects narrowing and honors DateTime64 timezones
--EXTENSIONS--
clickhouse
--FILE--
<?php
use ClickHouse\Driver\{ClientOptions, Column, CompressionMethod};
use ClickHouse\Driver\Exception\ValidationException;

$validDate = Column::create('Date', ['1970-01-01', '2149-06-06']);
var_dump($validDate->toArray());

$validDateTime = Column::create('DateTime', [0, 4294967295]);
var_dump($validDateTime->toArray());

$validDate32 = Column::create('Date32', ['1900-01-01', '2299-12-31', -1]);
var_dump($validDate32->toArray());

$validIPv4 = Column::create('IPv4', [0, 4294967295]);
var_dump($validIPv4->toArray());

$validDecimal = Column::create('Decimal(9,0)', ['999999999', '-999999999']);
var_dump($validDecimal->toArray());

$zoned = Column::create(
    "DateTime64(3, 'America/Sao_Paulo')",
    ['1970-01-01 00:00:00.000']
);
echo "Sao Paulo midnight in UTC: " . $zoned->at(0) . "\n";

$invalidValues = [
    ['invalid Date', static fn() => Column::create('Date', ['2024-02-30'])],
    ['Date below range', static fn() => Column::create('Date', ['1969-12-31'])],
    ['Date above range', static fn() => Column::create('Date', ['2200-01-01'])],
    ['Date trailing garbage', static fn() => Column::create('Date', ['2024-02-29 junk'])],
    ['Date invalid ISO suffix', static fn() => Column::create('Date', ['2024-02-29Tgarbage'])],
    ['Date timestamp below range', static fn() => Column::create('Date', [-1])],
    ['Date32 below range', static fn() => Column::create('Date32', ['1899-12-31'])],
    ['Date32 above range', static fn() => Column::create('Date32', ['2300-01-01'])],
    ['Date32 timestamp below range', static fn() => Column::create('Date32', [PHP_INT_MIN])],
    ['Date32 timestamp above range', static fn() => Column::create('Date32', [PHP_INT_MAX])],
    ['DateTime below range', static fn() => Column::create('DateTime', [-1])],
    ['DateTime above range', static fn() => Column::create('DateTime', [4294967296])],
    ['DateTime64 fractional float', static fn() => Column::create('DateTime64(3)', [1.5])],
    ['DateTime64 array', static fn() => Column::create('DateTime64(3)', [[]])],
    ['DateTime64 null', static fn() => Column::create('DateTime64(3)', [null])],
    ['IPv4 below range', static fn() => Column::create('IPv4', [-1])],
    ['IPv4 above range', static fn() => Column::create('IPv4', [4294967296])],
    ['IPv4 fractional float', static fn() => Column::create('IPv4', [1.5])],
    ['IPv4 array', static fn() => Column::create('IPv4', [[]])],
    ['Decimal positive overflow', static fn() => Column::create('Decimal(9,0)', ['3000000000'])],
    ['Decimal negative overflow', static fn() => Column::create('Decimal(9,0)', ['-3000000000'])],
];

foreach ($invalidValues as [$label, $factory]) {
    try {
        $factory();
        echo "FAIL: {$label} accepted\n";
    } catch (ValidationException $e) {
        echo "OK: {$label} rejected\n";
    }
}

$invalidOptions = [
    ['port below range', static fn() => new ClientOptions('localhost', -1)],
    ['port above range', static fn() => new ClientOptions('localhost', 65536)],
    [
        'negative retries',
        static fn() => new ClientOptions(
            'localhost', 9000, 'default', 'default', '', CompressionMethod::None, false, -1
        ),
    ],
    [
        'endpoint port overflow',
        static fn() => new ClientOptions(
            'localhost', 9000, 'default', 'default', '', CompressionMethod::None,
            false, 1, 5, false, true, 5000, 0, 0, null,
            [['host' => 'localhost', 'port' => 65536]]
        ),
    ],
    [
        'zero compression chunk',
        static fn() => new ClientOptions(
            'localhost', 9000, 'default', 'default', '', CompressionMethod::None,
            false, 1, 5, false, true, 5000, 0, 0, null, null, 60, 5, 3, 0
        ),
    ],
];

foreach ($invalidOptions as [$label, $factory]) {
    try {
        $factory();
        echo "FAIL: {$label} accepted\n";
    } catch (ValidationException $e) {
        echo "OK: {$label} rejected\n";
    }
}
?>
--EXPECT--
array(2) {
  [0]=>
  string(10) "1970-01-01"
  [1]=>
  string(10) "2149-06-06"
}
array(2) {
  [0]=>
  int(0)
  [1]=>
  int(4294967295)
}
array(3) {
  [0]=>
  string(10) "1900-01-01"
  [1]=>
  string(10) "2299-12-31"
  [2]=>
  string(10) "1969-12-31"
}
array(2) {
  [0]=>
  string(7) "0.0.0.0"
  [1]=>
  string(15) "255.255.255.255"
}
array(2) {
  [0]=>
  string(9) "999999999"
  [1]=>
  string(10) "-999999999"
}
Sao Paulo midnight in UTC: 1970-01-01 03:00:00.000
OK: invalid Date rejected
OK: Date below range rejected
OK: Date above range rejected
OK: Date trailing garbage rejected
OK: Date invalid ISO suffix rejected
OK: Date timestamp below range rejected
OK: Date32 below range rejected
OK: Date32 above range rejected
OK: Date32 timestamp below range rejected
OK: Date32 timestamp above range rejected
OK: DateTime below range rejected
OK: DateTime above range rejected
OK: DateTime64 fractional float rejected
OK: DateTime64 array rejected
OK: DateTime64 null rejected
OK: IPv4 below range rejected
OK: IPv4 above range rejected
OK: IPv4 fractional float rejected
OK: IPv4 array rejected
OK: Decimal positive overflow rejected
OK: Decimal negative overflow rejected
OK: port below range rejected
OK: port above range rejected
OK: negative retries rejected
OK: endpoint port overflow rejected
OK: zero compression chunk rejected
