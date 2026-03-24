--TEST--
Column::create() with UUID, IPv4, IPv6
--EXTENSIONS--
clickhouse
--FILE--
<?php
use ClickHouse\Driver\Column;

// UUID
$col = Column::create('UUID', [
    '550e8400-e29b-41d4-a716-446655440000',
    '00000000-0000-0000-0000-000000000000',
]);
var_dump($col->size());
echo $col->at(0) . "\n";
echo $col->at(1) . "\n";

// IPv4
$col = Column::create('IPv4', ['192.168.1.1', '10.0.0.1', '127.0.0.1']);
var_dump($col->size());
echo $col->at(0) . "\n";
echo $col->at(2) . "\n";

// IPv6
$col = Column::create('IPv6', ['::1', '2001:db8::1']);
var_dump($col->size());
echo $col->at(0) . "\n";

echo "OK\n";
?>
--EXPECTF--
int(2)
550e8400-e29b-41d4-a716-446655440000
00000000-0000-0000-0000-000000000000
int(3)
192.168.1.1
127.0.0.1
int(2)
::1
OK
