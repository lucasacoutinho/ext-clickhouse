--TEST--
Client::ping() connects to ClickHouse
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
$client->ping();
echo "PING OK\n";
?>
--EXPECT--
PING OK
