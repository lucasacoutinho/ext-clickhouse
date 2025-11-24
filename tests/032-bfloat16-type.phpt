--TEST--
ClickHouse: BFloat16 type support
--SKIPIF--
<?php
if (!extension_loaded('clickhouse')) die('skip clickhouse extension not loaded');
if (!getenv('CLICKHOUSE_HOST')) die('skip CLICKHOUSE_HOST not set');

// BFloat16 was added in ClickHouse 23.12
$host = getenv('CLICKHOUSE_HOST') ?: 'clickhouse';
$port = (int)(getenv('CLICKHOUSE_PORT') ?: 9000);
try {
    $client = new ClickHouse\Client($host, $port);
    $result = $client->query("SELECT version()");
    $version = $result[0]['version()'];
    if (version_compare($version, '23.12', '<')) {
        die('skip BFloat16 requires ClickHouse 23.12+');
    }
} catch (Exception $e) {
    die('skip Could not connect to ClickHouse');
}
?>
--FILE--
<?php
$host = getenv('CLICKHOUSE_HOST') ?: 'clickhouse';
$port = (int)(getenv('CLICKHOUSE_PORT') ?: 9000);

$client = new ClickHouse\Client($host, $port);

// Test basic BFloat16 values
$result = $client->query("SELECT toBFloat16(3.14159) as pi, toBFloat16(0) as zero, toBFloat16(-123.456) as neg");
var_dump(is_float($result[0]['pi']));
var_dump(abs($result[0]['pi'] - 3.140625) < 0.0001);  // BFloat16 precision
var_dump($result[0]['zero'] === 0.0);
var_dump($result[0]['neg'] < 0);

// Test BFloat16 in Array
$result = $client->query("SELECT [toBFloat16(1.0), toBFloat16(2.0), toBFloat16(3.0)] as arr");
var_dump(count($result[0]['arr']) === 3);
var_dump($result[0]['arr'][0] === 1.0);
var_dump($result[0]['arr'][1] === 2.0);
var_dump($result[0]['arr'][2] === 3.0);

// Test BFloat16 in Nullable
$result = $client->query("SELECT toBFloat16OrNull('3.5') as val, toBFloat16OrNull(NULL) as nil");
var_dump($result[0]['val'] === 3.5);
var_dump($result[0]['nil'] === null);

echo "OK\n";
?>
--EXPECT--
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
OK
