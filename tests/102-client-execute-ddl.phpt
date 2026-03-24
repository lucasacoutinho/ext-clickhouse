--TEST--
Client::execute() runs DDL statements
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

$client->execute('DROP TABLE IF EXISTS _test_ext_ddl');
$client->execute('CREATE TABLE _test_ext_ddl (id UInt64, name String) ENGINE = Memory');
$client->execute('DROP TABLE _test_ext_ddl');

echo "DDL OK\n";
?>
--CLEAN--
<?php
require __DIR__ . '/clickhouse_test.inc';
$client = clickhouse_test_client();
try { $client->execute('DROP TABLE IF EXISTS _test_ext_ddl'); } catch (\Throwable $e) {}
?>
--EXPECT--
DDL OK
