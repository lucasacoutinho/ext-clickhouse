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

// Positional arguments keep the test compatible with PHP 7.4.
$opts2 = new ClientOptions(
    '127.0.0.1',
    9001,
    'test_db',
    'admin',
    'secret',
    CompressionMethod::LZ4,
    true,
    1,
    5,
    false,
    true,
    10000
);
var_dump($opts2 instanceof ClientOptions);

try {
    $opts3 = new ClientOptions(
        'example.com',
        9440,
        'default',
        'default',
        '',
        CompressionMethod::LZ4,
        false,
        1,
        5,
        false,
        true,
        5000,
        0,
        0,
        [
            'ca_file' => '/tmp/ca.pem',
            'ca_files' => ['/tmp/ca2.pem'],
            'client_cert' => '/tmp/client.crt',
            'client_key' => '/tmp/client.key',
        ]
    );
    var_dump($opts3 instanceof ClientOptions);
} catch (\ClickHouse\Driver\Exception\ClickHouseException $e) {
    if (strpos($e->getMessage(), 'no SSL support') === false) {
        throw $e;
    }
    var_dump(true);
}

echo "OK\n";
?>
--EXPECT--
bool(true)
bool(true)
bool(true)
OK
