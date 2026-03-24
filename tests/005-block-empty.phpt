--TEST--
Block creation and empty state
--EXTENSIONS--
clickhouse
--FILE--
<?php
use ClickHouse\Driver\Block;
use ClickHouse\Driver\Column;

$block = new Block();
var_dump($block->getColumnCount());
var_dump($block->getRowCount());
var_dump($block->toArray());

// Append a column
$col = Column::create('UInt64', [10, 20, 30]);
$block->appendColumn('id', $col);

var_dump($block->getColumnCount());
var_dump($block->getRowCount());
var_dump($block->getColumnName(0));

// toArray transposes columns into rows
$rows = $block->toArray();
var_dump(count($rows));
var_dump($rows[0]['id']);
var_dump($rows[2]['id']);
?>
--EXPECT--
int(0)
int(0)
array(0) {
}
int(1)
int(3)
string(2) "id"
int(3)
int(10)
int(30)
