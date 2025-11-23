--TEST--
ClickHouse: External tables support
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

// Basic external table query
$result = $client->queryWithTable(
    "SELECT * FROM test_data ORDER BY id",
    "test_data",
    ["id" => "String", "name" => "String"],
    [
        ["1", "Alice"],
        ["2", "Bob"],
        ["3", "Charlie"]
    ]
);

echo "Row count: ";
var_dump(count($result) === 3);

echo "First row id: ";
var_dump($result[0]['id'] === '1');

echo "First row name: ";
var_dump($result[0]['name'] === 'Alice');

// Create test table for JOIN
$client->execute("CREATE TABLE IF NOT EXISTS ext_test_join (id String, value UInt32) ENGINE = Memory");
$client->execute("TRUNCATE TABLE ext_test_join");
$client->execute("INSERT INTO ext_test_join VALUES ('1', 100), ('2', 200)");

// JOIN with external table
$result2 = $client->queryWithTable(
    "SELECT t.id, t.value, e.name FROM ext_test_join t JOIN ext_data e ON t.id = e.id ORDER BY t.id",
    "ext_data",
    ["id" => "String", "name" => "String"],
    [
        ["1", "Alice"],
        ["2", "Bob"],
        ["3", "Charlie"]
    ]
);

echo "Join row count: ";
var_dump(count($result2) === 2);

echo "Join first row name: ";
var_dump($result2[0]['name'] === 'Alice');

echo "Join first row value: ";
var_dump($result2[0]['value'] === 100);

// Clean up
$client->execute("DROP TABLE IF EXISTS ext_test_join");

echo "OK\n";
?>
--EXPECT--
Row count: bool(true)
First row id: bool(true)
First row name: bool(true)
Join row count: bool(true)
Join first row name: bool(true)
Join first row value: bool(true)
OK
