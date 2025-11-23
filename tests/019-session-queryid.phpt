--TEST--
ClickHouse: Session and Query ID support
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

// Test initial state
echo "Initial session: ";
var_dump($client->getSession());
echo "Initial queryId: ";
var_dump($client->getQueryId());

// Test setSession/getSession
$client->setSession("test-session-123");
echo "After setSession: ";
var_dump($client->getSession());

// Test setQueryId/getQueryId
$client->setQueryId("my-query-prefix");
echo "After setQueryId: ";
var_dump($client->getQueryId());

// Test clearing with null
$client->setSession(null);
echo "After clearing session: ";
var_dump($client->getSession());

$client->setQueryId(null);
echo "After clearing queryId: ";
var_dump($client->getQueryId());

// Test session with temp table
$sessionId = "php-test-session-" . uniqid();
$client->setSession($sessionId);

// Create temp table
$client->execute("CREATE TEMPORARY TABLE IF NOT EXISTS test_temp (id UInt32, val String)");
$client->execute("INSERT INTO test_temp VALUES (1, 'foo'), (2, 'bar')");

// Query temp table (should work since same session)
$result = $client->query("SELECT * FROM test_temp ORDER BY id");
echo "Temp table row count: ";
var_dump(count($result) === 2);
echo "First row id: ";
var_dump($result[0]['id'] === 1);
echo "Second row val: ";
var_dump($result[1]['val'] === 'bar');

echo "OK\n";
?>
--EXPECT--
Initial session: NULL
Initial queryId: NULL
After setSession: string(16) "test-session-123"
After setQueryId: string(15) "my-query-prefix"
After clearing session: NULL
After clearing queryId: NULL
Temp table row count: bool(true)
First row id: bool(true)
Second row val: bool(true)
OK
