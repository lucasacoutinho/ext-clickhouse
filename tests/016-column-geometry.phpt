--TEST--
Column types: Point, Ring, Polygon, MultiPolygon
--EXTENSIONS--
clickhouse
--FILE--
<?php
use ClickHouse\Driver\Column;
use ClickHouse\Driver\Type;

// --- Point ---
// Point is stored as [x, y] (two doubles)
$col = Column::create('Point', [[1.0, 2.0], [3.5, -4.5], [0.0, 0.0]]);
echo "Point size: " . $col->size() . "\n";
echo "Point type: " . ($col->getType() === Type::Point ? 'Point' : 'other') . "\n";
$p0 = $col->at(0);
echo "Point[0]: [" . $p0[0] . ", " . $p0[1] . "]\n";
$p1 = $col->at(1);
echo "Point[1]: [" . $p1[0] . ", " . $p1[1] . "]\n";
$p2 = $col->at(2);
echo "Point[2]: [" . $p2[0] . ", " . $p2[1] . "]\n";

// --- Ring ---
// Ring = Array(Point), a closed polygon boundary
$ring1 = [[0.0, 0.0], [10.0, 0.0], [10.0, 10.0], [0.0, 10.0], [0.0, 0.0]];
$ring2 = [[1.0, 1.0], [2.0, 1.0], [2.0, 2.0], [1.0, 1.0]];
$col = Column::create('Ring', [$ring1, $ring2]);
echo "\nRing size: " . $col->size() . "\n";
echo "Ring type: " . ($col->getType() === Type::Ring ? 'Ring' : 'other') . "\n";

$r0 = $col->at(0);
echo "Ring[0] points: " . count($r0) . "\n";
echo "Ring[0][0]: [" . $r0[0][0] . ", " . $r0[0][1] . "]\n";
echo "Ring[0][4]: [" . $r0[4][0] . ", " . $r0[4][1] . "]\n";

// --- Polygon ---
// Polygon = Array(Ring), outer ring + optional holes
$outer = [[0.0, 0.0], [20.0, 0.0], [20.0, 20.0], [0.0, 20.0], [0.0, 0.0]];
$hole = [[5.0, 5.0], [10.0, 5.0], [10.0, 10.0], [5.0, 5.0]];
$polygon = [$outer, $hole];
$col = Column::create('Polygon', [$polygon]);
echo "\nPolygon size: " . $col->size() . "\n";
echo "Polygon type: " . ($col->getType() === Type::Polygon ? 'Polygon' : 'other') . "\n";

$poly0 = $col->at(0);
echo "Polygon[0] rings: " . count($poly0) . "\n";
echo "Polygon[0] outer ring points: " . count($poly0[0]) . "\n";
echo "Polygon[0] hole points: " . count($poly0[1]) . "\n";

// --- MultiPolygon ---
// MultiPolygon = Array(Polygon)
$poly1 = [[[0.0, 0.0], [1.0, 0.0], [1.0, 1.0], [0.0, 0.0]]];
$poly2 = [[[5.0, 5.0], [6.0, 5.0], [6.0, 6.0], [5.0, 5.0]]];
$col = Column::create('MultiPolygon', [[$poly1, $poly2]]);
echo "\nMultiPolygon size: " . $col->size() . "\n";
echo "MultiPolygon type: " . ($col->getType() === Type::MultiPolygon ? 'MultiPolygon' : 'other') . "\n";

$mp0 = $col->at(0);
echo "MultiPolygon[0] polygons: " . count($mp0) . "\n";

echo "\nDone\n";
?>
--EXPECT--
Point size: 3
Point type: Point
Point[0]: [1, 2]
Point[1]: [3.5, -4.5]
Point[2]: [0, 0]

Ring size: 2
Ring type: Ring
Ring[0] points: 5
Ring[0][0]: [0, 0]
Ring[0][4]: [0, 0]

Polygon size: 1
Polygon type: Polygon
Polygon[0] rings: 2
Polygon[0] outer ring points: 5
Polygon[0] hole points: 4

MultiPolygon size: 1
MultiPolygon type: MultiPolygon
MultiPolygon[0] polygons: 2

Done
