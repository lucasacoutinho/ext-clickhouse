--TEST--
ClickHouse: UUID type
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

// Test UUID
$result = $client->query("SELECT toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0') as uuid");
var_dump(is_string($result[0]['uuid']));
var_dump(strlen($result[0]['uuid']) == 36); // UUID format: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx

echo "OK\n";
?>
--EXPECT--
bool(true)
bool(true)
OK
