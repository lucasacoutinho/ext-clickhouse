--TEST--
Query parameters via {name:Type} syntax
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

// Parameterized SELECT with {name:Type} syntax
$rows = $client->select(
    "SELECT {val:UInt64} AS n, {name:String} AS s",
    params: ['val' => '42', 'name' => 'hello']
);
var_dump($rows[0]['n']);
var_dump($rows[0]['s']);

// Parameterized with NULL
$rows = $client->select(
    "SELECT {maybe:Nullable(String)} AS v",
    params: ['maybe' => null]
);
var_dump($rows[0]['v']);

echo "OK\n";
?>
--EXPECT--
int(42)
string(5) "hello"
NULL
OK
