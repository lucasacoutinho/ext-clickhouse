--TEST--
ClickHouse: Array and Tuple types
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

// Test Array
$result = $client->query("SELECT [1, 2, 3] as arr");
var_dump(is_array($result[0]['arr']));
var_dump($result[0]['arr']);

// Test Tuple
$result = $client->query("SELECT (1, 'a', 3.14) as tup");
var_dump(is_array($result[0]['tup']));
var_dump($result[0]['tup']);

echo "OK\n";
?>
--EXPECT--
bool(true)
array(3) {
  [0]=>
  int(1)
  [1]=>
  int(2)
  [2]=>
  int(3)
}
bool(true)
array(3) {
  [0]=>
  int(1)
  [1]=>
  string(1) "a"
  [2]=>
  float(3.14)
}
OK
