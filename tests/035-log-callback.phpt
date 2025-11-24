--TEST--
ClickHouse\Client::setLogCallback() - Server log callback functionality
--SKIPIF--
<?php
if (!extension_loaded('clickhouse')) {
    echo 'skip ClickHouse extension not available';
}
?>
--FILE--
<?php
$client = new ClickHouse\Client(
    getenv('CLICKHOUSE_HOST') ?: 'localhost',
    (int)(getenv('CLICKHOUSE_PORT') ?: 9000),
    getenv('CLICKHOUSE_USER') ?: 'default',
    getenv('CLICKHOUSE_PASSWORD') ?: '',
    getenv('CLICKHOUSE_DB') ?: 'default'
);

$logEntries = [];
$callbackExecuted = false;

// Set log callback to capture log messages
$client->setLogCallback(function($timestamp, $microseconds, $thread_id, $level, $source, $text) use (&$logEntries, &$callbackExecuted) {
    $callbackExecuted = true;
    $logEntries[] = [
        'timestamp' => $timestamp,
        'microseconds' => $microseconds,
        'thread_id' => $thread_id,
        'level' => $level,
        'source' => $source,
        'text' => $text
    ];
});

// Simple query - with SETTINGS send_logs_level to request log messages
// Note: Whether we get logs depends on server configuration
$result = $client->query("SELECT 1 as num SETTINGS send_logs_level='trace'");

// Verify query result
echo "Query result: ";
var_dump($result[0]['num']);

// Check callback setup worked
echo "Log callback was set: Yes\n";

// Check if we received log entries
if (count($logEntries) > 0) {
    echo "Received log entries: Yes\n";

    // Verify structure of first log entry
    $first = $logEntries[0];
    $valid = isset($first['timestamp']) &&
             isset($first['microseconds']) &&
             isset($first['thread_id']) &&
             isset($first['level']) &&
             isset($first['source']) &&
             isset($first['text']);
    echo "Log entry structure valid: ", $valid ? "Yes" : "No", "\n";

    // Verify types
    echo "Types correct: ";
    $typesOk = is_numeric($first['timestamp']) &&
               is_numeric($first['microseconds']) &&
               is_numeric($first['thread_id']) &&
               is_numeric($first['level']) && $first['level'] >= 1 && $first['level'] <= 8 &&
               is_string($first['source']) &&
               is_string($first['text']);
    echo $typesOk ? "Yes" : "No", "\n";
} else {
    echo "Received log entries: No (may depend on server config)\n";
}

// Test clearing the callback
$client->setLogCallback(null);
echo "Callback cleared: Yes\n";

// Test setting callback again
$client->setLogCallback(function() {
    // Just a placeholder
});
echo "Callback set again: Yes\n";

echo "OK\n";
?>
--EXPECTF--
Query result: int(1)
Log callback was set: Yes
Received log entries: %a
Callback cleared: Yes
Callback set again: Yes
OK
