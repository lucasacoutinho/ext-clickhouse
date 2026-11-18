--TEST--
Client refuses re-entrant use while a query is in progress
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

use ClickHouse\Driver\Exception\ValidationException;

$client = clickhouse_test_client();
$options = clickhouse_test_options();
$query = 'SELECT number FROM system.numbers LIMIT 10';
$expected = 'Cannot re-enter Client while a query is in progress';

$attempt = static function (callable $body) use ($client, $query, $expected) {
    $rows = 0;
    $message = '';
    $client->selectByBlock($query, static function ($block) use ($body, &$rows, &$message, $expected) {
        $rows += $block->getRowCount();
        try {
            $body();
            $message = 'allowed';
        } catch (ValidationException $e) {
            $message = $e->getMessage() === $expected ? 'rejected' : $e->getMessage();
        }
        return true;
    });
    echo "rows=$rows message=$message\n";
};

$attempt(static function () use ($client, $options) {
    $client->__construct($options);
});
$attempt(static function () use ($client) {
    $client->resetConnection();
});
$attempt(static function () use ($client) {
    $client->resetConnectionEndpoint();
});
$attempt(static function () use ($client) {
    $client->select('SELECT 1');
});
$attempt(static function () use ($client) {
    $client->ping();
});

try {
    $client->selectByBlock($query, static function () use ($client, $options) {
        $client->__construct($options);
        return true;
    });
    echo "FAIL: uncaught construct was allowed\n";
} catch (ValidationException $e) {
    echo 'uncaught=' . $e->getMessage() . "\n";
}

var_dump($client->select('SELECT 42 AS x')[0]['x']);

$client->__construct($options);
var_dump($client->select('SELECT 7 AS x')[0]['x']);
?>
--EXPECT--
rows=10 message=rejected
rows=10 message=rejected
rows=10 message=rejected
rows=10 message=rejected
rows=10 message=rejected
uncaught=Cannot re-enter Client while a query is in progress
int(42)
int(7)
