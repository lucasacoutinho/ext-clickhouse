--TEST--
ClickHouse: LZ4 and ZSTD compression
--SKIPIF--
<?php
if (!extension_loaded('clickhouse')) die('skip clickhouse extension not loaded');
if (!getenv('CLICKHOUSE_HOST')) die('skip CLICKHOUSE_HOST not set');
?>
--FILE--
<?php
$host = getenv('CLICKHOUSE_HOST') ?: 'clickhouse';
$port = (int)(getenv('CLICKHOUSE_PORT') ?: 9000);

$client = new ClickHouse\Client($host, $port);

// Test compression constants exist
echo "Constants:\n";
var_dump(ClickHouse\Client::COMPRESS_NONE);
var_dump(ClickHouse\Client::COMPRESS_LZ4);
var_dump(ClickHouse\Client::COMPRESS_ZSTD);

// Test default compression (none)
echo "Default compression: ";
var_dump($client->getCompression());

// Test query without compression
$result = $client->query("SELECT 1 as num, 'hello' as str");
echo "No compression: ";
var_dump($result[0]['num'] === 1 && $result[0]['str'] === 'hello');

// Test LZ4 compression
$client->setCompression(ClickHouse\Client::COMPRESS_LZ4);
echo "LZ4 enabled: ";
var_dump($client->getCompression());

$result = $client->query("SELECT 2 as num, 'lz4 test' as str");
echo "LZ4 query: ";
var_dump($result[0]['num'] === 2 && $result[0]['str'] === 'lz4 test');

// Test ZSTD compression
$client->setCompression(ClickHouse\Client::COMPRESS_ZSTD);
echo "ZSTD enabled: ";
var_dump($client->getCompression());

$result = $client->query("SELECT 3 as num, 'zstd test' as str");
echo "ZSTD query: ";
var_dump($result[0]['num'] === 3 && $result[0]['str'] === 'zstd test');

// Test larger query with ZSTD
$result = $client->query("SELECT number, toString(number) as str FROM numbers(100)");
echo "ZSTD large query: ";
var_dump(count($result) === 100 && $result[99]['number'] === 99);

// Switch back to LZ4 for another large query
$client->setCompression(ClickHouse\Client::COMPRESS_LZ4);
$result = $client->query("SELECT number FROM numbers(100)");
echo "LZ4 large query: ";
var_dump(count($result) === 100 && $result[50]['number'] === 50);

// Disable compression
$client->setCompression(ClickHouse\Client::COMPRESS_NONE);
echo "Compression disabled: ";
var_dump($client->getCompression());

$result = $client->query("SELECT 4 as num");
echo "No compression query: ";
var_dump($result[0]['num'] === 4);

echo "OK\n";
?>
--EXPECT--
Constants:
int(0)
int(1)
int(2)
Default compression: int(0)
No compression: bool(true)
LZ4 enabled: int(1)
LZ4 query: bool(true)
ZSTD enabled: int(2)
ZSTD query: bool(true)
ZSTD large query: bool(true)
LZ4 large query: bool(true)
Compression disabled: int(0)
No compression query: bool(true)
OK
