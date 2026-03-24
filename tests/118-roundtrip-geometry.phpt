--TEST--
Round-trip: Point, Ring, Polygon, MultiPolygon via ClickHouse server
--EXTENSIONS--
clickhouse
--SKIPIF--
<?php
require __DIR__ . '/clickhouse_test.inc';
clickhouse_test_skip();
// Geo types require allow_experimental_geo_types (ClickHouse >= 21.8)
$client = clickhouse_test_client();
try {
    $client->execute("SET allow_experimental_geo_types = 1");
    $client->execute('DROP TABLE IF EXISTS _test_ext_geo_check');
    $client->execute('CREATE TABLE _test_ext_geo_check (p Point) ENGINE = Memory');
    $client->execute('DROP TABLE IF EXISTS _test_ext_geo_check');
} catch (\Throwable $e) {
    die('skip Geo types not supported: ' . $e->getMessage());
}
?>
--FILE--
<?php
require __DIR__ . '/clickhouse_test.inc';
use ClickHouse\Driver\{Block, Column};

$client = clickhouse_test_client();

// Enable experimental geo types for the session
$client->execute("SET allow_experimental_geo_types = 1");

$client->execute('DROP TABLE IF EXISTS _test_ext_geo');
$client->execute("CREATE TABLE _test_ext_geo (
    pt Point,
    rn Ring,
    pg Polygon,
    mp MultiPolygon
) ENGINE = Memory");

// Build data
$point1 = [1.5, 2.5];
$point2 = [3.0, 4.0];

$ring1 = [[0.0, 0.0], [10.0, 0.0], [10.0, 10.0], [0.0, 0.0]];
$ring2 = [[1.0, 1.0], [2.0, 1.0], [2.0, 2.0], [1.0, 1.0]];

$polygon1 = [$ring1];
$polygon2 = [$ring1, $ring2]; // outer + hole

$multipolygon1 = [$polygon1];
$multipolygon2 = [$polygon1, [[[5.0, 5.0], [6.0, 5.0], [6.0, 6.0], [5.0, 5.0]]]];

$block = new Block();
$block->appendColumn('pt', Column::create('Point', [$point1, $point2]));
$block->appendColumn('rn', Column::create('Ring', [$ring1, $ring2]));
$block->appendColumn('pg', Column::create('Polygon', [$polygon1, $polygon2]));
$block->appendColumn('mp', Column::create('MultiPolygon', [$multipolygon1, $multipolygon2]));

$client->insert('_test_ext_geo', $block);

$rows = $client->select('SELECT * FROM _test_ext_geo');

echo "Row count: " . count($rows) . "\n";

// Row 0
echo "\n--- Row 0 ---\n";
echo "pt: [" . $rows[0]['pt'][0] . ", " . $rows[0]['pt'][1] . "]\n";
echo "rn points: " . count($rows[0]['rn']) . "\n";
echo "pg rings: " . count($rows[0]['pg']) . "\n";
echo "mp polygons: " . count($rows[0]['mp']) . "\n";

// Row 1
echo "\n--- Row 1 ---\n";
echo "pt: [" . $rows[1]['pt'][0] . ", " . $rows[1]['pt'][1] . "]\n";
echo "rn points: " . count($rows[1]['rn']) . "\n";
echo "pg rings (with hole): " . count($rows[1]['pg']) . "\n";
echo "mp polygons: " . count($rows[1]['mp']) . "\n";

echo "\nDone\n";
?>
--CLEAN--
<?php
require __DIR__ . '/clickhouse_test.inc';
$client = clickhouse_test_client();
try { $client->execute('DROP TABLE IF EXISTS _test_ext_geo'); } catch (\Throwable $e) {}
?>
--EXPECT--
Row count: 2

--- Row 0 ---
pt: [1.5, 2.5]
rn points: 4
pg rings: 1
mp polygons: 1

--- Row 1 ---
pt: [3, 4]
rn points: 4
pg rings (with hole): 2
mp polygons: 2

Done
