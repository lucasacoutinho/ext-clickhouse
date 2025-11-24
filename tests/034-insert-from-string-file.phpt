--TEST--
ClickHouse: insertFromString and insertFromFile methods
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

// Test insertFromString with CSV
$client->execute("DROP TABLE IF EXISTS test_insert_string");
$client->execute("CREATE TABLE test_insert_string (id UInt32, name String, value Float32) ENGINE = Memory");

$csvData = "1,Alice,3.14\n2,Bob,2.71\n3,Charlie,1.41";
$client->insertFromString("test_insert_string", $csvData, "CSV");

$result = $client->query("SELECT * FROM test_insert_string ORDER BY id");
var_dump(count($result) === 3);
var_dump($result[0]['id'] === 1);
var_dump($result[0]['name'] === 'Alice');
var_dump(abs($result[0]['value'] - 3.14) < 0.01);

// Test insertFromString with TSV
$client->execute("DROP TABLE IF EXISTS test_insert_tsv");
$client->execute("CREATE TABLE test_insert_tsv (a UInt8, b String) ENGINE = Memory");

$tsvData = "10\tFirst\n20\tSecond\n30\tThird";
$client->insertFromString("test_insert_tsv", $tsvData, "TSV");

$result = $client->query("SELECT * FROM test_insert_tsv ORDER BY a");
var_dump(count($result) === 3);
var_dump($result[1]['a'] === 20);
var_dump($result[1]['b'] === 'Second');

// Test insertFromFile
$client->execute("DROP TABLE IF EXISTS test_insert_file");
$client->execute("CREATE TABLE test_insert_file (x Int32, y String) ENGINE = Memory");

$tmpFile = sys_get_temp_dir() . '/clickhouse_test_' . uniqid() . '.csv';
file_put_contents($tmpFile, "100,Hello\n200,World\n300,Test");

$client->insertFromFile("test_insert_file", $tmpFile, "CSV");
unlink($tmpFile);

$result = $client->query("SELECT * FROM test_insert_file ORDER BY x");
var_dump(count($result) === 3);
var_dump($result[0]['x'] === 100);
var_dump($result[0]['y'] === 'Hello');
var_dump($result[2]['x'] === 300);
var_dump($result[2]['y'] === 'Test');

// Test with JSONEachRow format
$client->execute("DROP TABLE IF EXISTS test_insert_json");
$client->execute("CREATE TABLE test_insert_json (id UInt32, msg String) ENGINE = Memory");

$jsonData = '{"id":1,"msg":"first"}' . "\n" . '{"id":2,"msg":"second"}';
$client->insertFromString("test_insert_json", $jsonData, "JSONEachRow");

$result = $client->query("SELECT * FROM test_insert_json ORDER BY id");
var_dump(count($result) === 2);
var_dump($result[0]['id'] === 1);
var_dump($result[0]['msg'] === 'first');

echo "OK\n";
?>
--EXPECT--
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
OK
