--TEST--
Client::getServerInfo() returns ServerInfo object
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
$info = $client->getServerInfo();

var_dump($info instanceof ClickHouse\Driver\ServerInfo);
var_dump(is_string($info->name));
var_dump(is_string($info->timezone));
var_dump(is_int($info->versionMajor));
var_dump($info->versionMajor > 0);
var_dump(is_int($info->revision));

echo "OK\n";
?>
--EXPECT--
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
OK
