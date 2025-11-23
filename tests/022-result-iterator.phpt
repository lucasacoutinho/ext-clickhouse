--TEST--
ClickHouse: ResultIterator for streaming results
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

// Test queryIterator returns ResultIterator
$iter = $client->queryIterator("SELECT number FROM system.numbers LIMIT 5");
echo "Returns ResultIterator: ";
var_dump($iter instanceof ClickHouse\ResultIterator);

// Test Countable
echo "Implements Countable: ";
var_dump(count($iter) === 5);

// Test Iterator with foreach
echo "Foreach iteration:\n";
$rows = [];
foreach ($iter as $key => $row) {
    $rows[] = ['key' => $key, 'number' => $row['number']];
}
var_dump(count($rows) === 5);
var_dump($rows[0]['key'] === 0);
var_dump($rows[0]['number'] === 0);
var_dump($rows[4]['key'] === 4);
var_dump($rows[4]['number'] === 4);

// Test rewind
echo "Rewind works: ";
$iter->rewind();
var_dump($iter->valid() === true);
var_dump($iter->key() === 0);

// Test manual iteration
echo "Manual iteration: ";
$iter->rewind();
$first = $iter->current();
$iter->next();
$second = $iter->current();
var_dump($first['number'] === 0 && $second['number'] === 1);

// Test empty result
$emptyIter = $client->queryIterator("SELECT number FROM system.numbers LIMIT 0");
echo "Empty iterator count: ";
var_dump(count($emptyIter) === 0);
echo "Empty iterator valid: ";
var_dump($emptyIter->valid() === false);

echo "OK\n";
?>
--EXPECT--
Returns ResultIterator: bool(true)
Implements Countable: bool(true)
Foreach iteration:
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
Rewind works: bool(true)
bool(true)
Manual iteration: bool(true)
Empty iterator count: bool(true)
Empty iterator valid: bool(true)
OK
