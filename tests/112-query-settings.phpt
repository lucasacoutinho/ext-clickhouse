--TEST--
Per-query settings
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

// Verify settings parameter is accepted without error
$rows = $client->select(
    'SELECT number FROM system.numbers LIMIT 10',
    null,
    ['max_block_size' => '5']
);

var_dump(count($rows) === 10);

echo "OK\n";
?>
--EXPECT--
bool(true)
OK
