--TEST--
ClickHouse: Core extension should not handle :parameter PDO syntax
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

// Core extension should NOT support :parameter PDO-style syntax
// The query should be passed as-is without :param substitution
// This will likely cause a ClickHouse syntax error, which is expected

try {
    // Attempt to use :param syntax (should not be substituted by core extension)
    $stmt = $client->prepare("SELECT :num as value");
    $stmt->bind("num", 42, "Int32");
    $result = $stmt->execute();

    // If we get here, it means :param was somehow substituted (wrong behavior)
    echo "ERROR: :param syntax was substituted by core extension\n";

} catch (Exception $e) {
    // Expected: ClickHouse should reject the :param syntax
    // This proves the core extension is NOT handling PDO-style parameters
    if (strpos($e->getMessage(), 'Syntax error') !== false ||
        strpos($e->getMessage(), 'Unknown identifier') !== false ||
        strpos($e->getMessage(), ':num') !== false) {
        echo "OK: Core extension correctly does not handle :param syntax\n";
    } else {
        echo "Unexpected error: " . $e->getMessage() . "\n";
    }
}
?>
--EXPECT--
OK: Core extension correctly does not handle :param syntax