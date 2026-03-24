--TEST--
Client::select() with simple expressions
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php
require __DIR__ . '/clickhouse_test.inc';
clickhouse_test_skip();
?>
--FILE--
<?php
require __DIR__ . '/clickhouse_test.inc';

$client = clickhouse_test_client();

// Simple scalar
$rows = $client->select('SELECT 1 AS n');
var_dump(count($rows));
var_dump($rows[0]['n']);

// Multiple columns
$rows = $client->select("SELECT 42 AS id, 'hello' AS name, 3.14 AS pi");
var_dump($rows[0]['id']);
var_dump($rows[0]['name']);
var_dump($rows[0]['pi']);

// Multiple rows
$rows = $client->select('SELECT number FROM system.numbers LIMIT 3');
var_dump(count($rows));
var_dump($rows[0]['number']);
var_dump($rows[2]['number']);

echo "OK\n";
?>
--EXPECTF--
int(1)
int(1)
int(42)
string(5) "hello"
float(3.14)
int(3)
int(0)
int(2)
OK
