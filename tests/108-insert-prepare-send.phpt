--TEST--
Client::prepareInsert() / sendBlock() / endInsert() streaming insert
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
use ClickHouse\Driver\{Block, Column};

$client = clickhouse_test_client();

$client->execute('DROP TABLE IF EXISTS _test_ext_streaming');
$client->execute('CREATE TABLE _test_ext_streaming (id UInt64, val String) ENGINE = Memory');

// prepareInsert returns a Block with the table's column structure
$block = $client->prepareInsert('INSERT INTO _test_ext_streaming (id, val) VALUES');

// Send multiple blocks
for ($batch = 0; $batch < 3; $batch++) {
    $b = new Block();
    $ids = [];
    $vals = [];
    for ($i = 0; $i < 10; $i++) {
        $ids[] = $batch * 10 + $i;
        $vals[] = "row_" . ($batch * 10 + $i);
    }
    $b->appendColumn('id', Column::create('UInt64', $ids));
    $b->appendColumn('val', Column::create('String', $vals));
    $client->sendBlock($b);
}
$client->endInsert();

$rows = $client->select('SELECT count() AS cnt FROM _test_ext_streaming');
var_dump($rows[0]['cnt']);

echo "OK\n";
?>
--CLEAN--
<?php
require __DIR__ . '/clickhouse_test.inc';
$client = clickhouse_test_client();
try { $client->execute('DROP TABLE IF EXISTS _test_ext_streaming'); } catch (\Throwable $e) {}
?>
--EXPECT--
int(30)
OK
