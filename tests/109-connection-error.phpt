--TEST--
Connection to invalid host throws exception
--EXTENSIONS--
clickhouse
--FILE--
<?php
use ClickHouse\Driver\{Client, ClientOptions, CompressionMethod};
use ClickHouse\Driver\Exception\ClickHouseException;

$opts = new ClientOptions(
    '192.0.2.1',  // RFC 5737 TEST-NET, guaranteed unreachable
    19000,
    'default',
    'default',
    '',
    CompressionMethod::None,
    false,
    1,
    5,
    false,
    true,
    500
);

try {
    $client = new Client($opts);
    $client->ping();
    echo "FAIL: should have thrown\n";
} catch (ClickHouseException $e) {
    echo "Caught expected exception\n";
    var_dump(strlen($e->getMessage()) > 0);
}
?>
--EXPECT--
Caught expected exception
bool(true)
