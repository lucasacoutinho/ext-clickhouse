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

// Use max_result_rows setting to limit output
$rows = $client->select(
    'SELECT number FROM system.numbers LIMIT 100',
    settings: ['max_result_rows' => '5', 'result_overflow_mode' => 'break']
);

// Should get at most 5 rows due to the setting
var_dump(count($rows) <= 5);

echo "OK\n";
?>
--EXPECT--
bool(true)
OK
