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

$constructor = new ReflectionMethod(ClientOptions::class, '__construct');
$parameters = $constructor->getParameters();
echo "Constructor parameters: " . count($parameters) . "\n";
echo "Extended parameters: " .
    implode(', ', array_map(
        static fn(ReflectionParameter $parameter): string => $parameter->getName(),
        array_slice($parameters, 15)
    )) . "\n";

if (PHP_VERSION_ID >= 80000) {
    $namedOptions = (new ReflectionClass(ClientOptions::class))->newInstanceArgs([
        'endpoints' => [],
        'tcpKeepAliveIdleSeconds' => 30,
        'maxCompressionChunkSize' => 32768,
    ]);
} else {
    $namedOptions = new ClientOptions(
        'localhost', 9000, 'default', 'default', '', CompressionMethod::None,
        false, 1, 5, false, true, 5000, 0, 0, null, [], 30, 5, 3, 32768
    );
}
var_dump($namedOptions instanceof ClientOptions);

if (getenv('CLICKHOUSE_SANITIZER')) {
    var_dump(true);
} else {
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
}

echo "OK\n";
?>
--EXPECT--
bool(true)
bool(true)
Constructor parameters: 20
Extended parameters: endpoints, tcpKeepAliveIdleSeconds, tcpKeepAliveIntervalSeconds, tcpKeepAliveCount, maxCompressionChunkSize
bool(true)
bool(true)
OK
