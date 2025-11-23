--TEST--
ClickHouse: INSERT and execute methods
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

// Test execute() for DDL
$client->execute("DROP TABLE IF EXISTS test_insert_phpt");
$client->execute("CREATE TABLE test_insert_phpt (id UInt32, name String, score Float64) ENGINE = Memory");
echo "Table created\n";

// Test insert() with multiple rows
$client->insert("test_insert_phpt", ["id", "name", "score"], [
    [1, "Alice", 95.5],
    [2, "Bob", 87.3],
    [3, "Charlie", 92.1]
]);
echo "Data inserted\n";

// Verify data
$result = $client->query("SELECT * FROM test_insert_phpt ORDER BY id");
var_dump(count($result) === 3);
var_dump($result[0]['id'] === 1);
var_dump($result[0]['name'] === 'Alice');
var_dump($result[1]['id'] === 2);
var_dump($result[2]['name'] === 'Charlie');

// Clean up
$client->execute("DROP TABLE test_insert_phpt");
echo "OK\n";
?>
--EXPECT--
Table created
Data inserted
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
OK
