--TEST--
Client::select() keeps row keys for duplicate, numeric, and empty results across blocks
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php
require __DIR__ . "/clickhouse_test.inc";
clickhouse_test_skip();
?>
--FILE--
<?php
require __DIR__ . "/clickhouse_test.inc";

$client = clickhouse_test_client();

var_dump($client->select("SELECT 1 AS a, 2 AS b, 3 AS `7`")[0] === ["a" => 1, "b" => 2, 7 => 3]);
var_dump(array_keys($client->select("SELECT 1 AS `7`")[0]) === [7]);

$duplicate = $client->select("SELECT x, x FROM (SELECT 1 AS x)");
var_dump($duplicate[0] === ["x" => 1]);

var_dump($client->select("SELECT 1 WHERE 0"));

$rows = $client->select("SELECT number AS n, toString(number) AS s FROM numbers(200000) SETTINGS max_block_size = 1000");
var_dump(count($rows), $rows[0]["n"], $rows[199999]);

$overflow = $client->select("SELECT toUInt64(18446744073709551615) AS big, toFixedString('ab', 2) AS fixed, true AS flag, toFloat32(0.5) AS f")[0];
var_dump($overflow);
?>
--EXPECT--
bool(true)
bool(true)
bool(true)
array(0) {
}
int(200000)
int(0)
array(2) {
  ["n"]=>
  int(199999)
  ["s"]=>
  string(6) "199999"
}
array(4) {
  ["big"]=>
  string(20) "18446744073709551615"
  ["fixed"]=>
  string(2) "ab"
  ["flag"]=>
  bool(true)
  ["f"]=>
  float(0.5)
}
