--TEST--
ClientOptions constructor with defaults
--EXTENSIONS--
clickhouse
--FILE--
<?php
use ClickHouse\Driver\ClientOptions;
use ClickHouse\Driver\CompressionMethod;

// Default constructor
$opts = new ClientOptions();
var_dump($opts instanceof ClientOptions);

// Named arguments
$opts2 = new ClientOptions(
    host: '127.0.0.1',
    port: 9001,
    database: 'test_db',
    user: 'admin',
    password: 'secret',
    compression: CompressionMethod::LZ4,
    pingBeforeQuery: true,
    connectTimeoutMs: 10000,
);
var_dump($opts2 instanceof ClientOptions);

echo "OK\n";
?>
--EXPECT--
bool(true)
bool(true)
OK
