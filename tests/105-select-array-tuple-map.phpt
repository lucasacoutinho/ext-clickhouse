--TEST--
Client::select() with Array, Tuple, and Map types
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

// Array
$rows = $client->select("SELECT [1, 2, 3] AS arr");
var_dump($rows[0]['arr']);

// Nested Array
$rows = $client->select("SELECT [[1, 2], [3]] AS nested");
var_dump($rows[0]['nested']);

// Tuple
$rows = $client->select("SELECT tuple(1, 'hello', 3.14) AS t");
$t = $rows[0]['t'];
var_dump(is_array($t));
var_dump(count($t));
var_dump($t[0]);
var_dump($t[1]);

// Map
$rows = $client->select("SELECT map('key1', 'val1', 'key2', 'val2') AS m");
$m = $rows[0]['m'];
var_dump(is_array($m));
var_dump($m['key1']);
var_dump($m['key2']);

echo "OK\n";
?>
--EXPECT--
array(3) {
  [0]=>
  int(1)
  [1]=>
  int(2)
  [2]=>
  int(3)
}
array(2) {
  [0]=>
  array(2) {
    [0]=>
    int(1)
    [1]=>
    int(2)
  }
  [1]=>
  array(1) {
    [0]=>
    int(3)
  }
}
bool(true)
int(3)
int(1)
string(5) "hello"
bool(true)
string(4) "val1"
string(4) "val2"
OK
