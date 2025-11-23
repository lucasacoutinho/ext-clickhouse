--TEST--
ClickHouse: Query timeout support
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

// Test initial timeout
echo "Initial timeout: ";
var_dump($client->getTimeout() === 0);

// Test setTimeout
$client->setTimeout(5000);
echo "After setTimeout(5000): ";
var_dump($client->getTimeout() === 5000);

// Test query works with timeout set
$result = $client->query("SELECT 1 as num");
echo "Query with timeout: ";
var_dump($result[0]['num'] === 1);

// Test reset timeout
$client->setTimeout(0);
echo "After setTimeout(0): ";
var_dump($client->getTimeout() === 0);

// Test negative timeout throws exception
echo "Negative timeout throws: ";
try {
    $client->setTimeout(-100);
    echo "bool(false)\n";
} catch (ClickHouse\Exception $e) {
    echo "bool(true)\n";
}

echo "OK\n";
?>
--EXPECT--
Initial timeout: bool(true)
After setTimeout(5000): bool(true)
Query with timeout: bool(true)
After setTimeout(0): bool(true)
Negative timeout throws: bool(true)
OK
