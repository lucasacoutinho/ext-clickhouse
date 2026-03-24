--TEST--
Server-side SQL errors produce ServerException with ClickHouse error code
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
use ClickHouse\Driver\Exception\ServerException;

$client = clickhouse_test_client();

try {
    $client->select('SELECT * FROM _nonexistent_table_abc123');
    echo "FAIL: should have thrown\n";
} catch (ServerException $e) {
    echo "Caught ServerException\n";
    var_dump($e->getClickHouseCode() > 0);
    var_dump(strlen($e->getMessage()) > 0);
} catch (\Throwable $e) {
    echo "Wrong exception type: " . get_class($e) . "\n";
}
?>
--EXPECT--
Caught ServerException
bool(true)
bool(true)
