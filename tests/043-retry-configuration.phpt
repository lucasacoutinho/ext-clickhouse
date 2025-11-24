--TEST--
ClickHouse: Retry configuration and exponential backoff
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

// Test 1: Default configuration
echo "Test 1: Default retry configuration\n";
echo "Auto-reconnect: " . ($client->getAutoReconnect() ? 'yes' : 'no') . "\n";
echo "Max retry attempts: " . $client->getMaxRetryAttempts() . "\n";
$delay = $client->getRetryDelay();
echo "Base delay: " . $delay['base_delay'] . "\n";
echo "Max delay: " . $delay['max_delay'] . "\n";
echo "Jitter: " . ($client->getRetryJitter() ? 'yes' : 'no') . "\n";
echo "Total retries: " . $client->getTotalRetryAttempts() . "\n";

// Test 2: Set custom configuration
echo "\nTest 2: Custom configuration\n";
$client->setAutoReconnect(true);
$client->setMaxRetryAttempts(5);
$client->setRetryDelay(0.05, 2.0);
$client->setRetryJitter(false);

echo "Auto-reconnect: " . ($client->getAutoReconnect() ? 'yes' : 'no') . "\n";
echo "Max retry attempts: " . $client->getMaxRetryAttempts() . "\n";
$delay = $client->getRetryDelay();
echo "Base delay: " . $delay['base_delay'] . "\n";
echo "Max delay: " . $delay['max_delay'] . "\n";
echo "Jitter: " . ($client->getRetryJitter() ? 'yes' : 'no') . "\n";

// Test 3: Validation - negative retry attempts
echo "\nTest 3: Validation\n";
try {
    $client->setMaxRetryAttempts(-1);
    echo "ERROR: Should reject negative attempts\n";
} catch (Exception $e) {
    echo "Negative attempts: rejected\n";
}

// Test 4: Validation - negative delay
try {
    $client->setRetryDelay(-0.1, 1.0);
    echo "ERROR: Should reject negative delay\n";
} catch (Exception $e) {
    echo "Negative delay: rejected\n";
}

// Test 5: Validation - base > max
try {
    $client->setRetryDelay(2.0, 1.0);
    echo "ERROR: Should reject base > max\n";
} catch (Exception $e) {
    echo "Base > max: rejected\n";
}

// Test 6: Retry metrics
echo "\nTest 4: Retry metrics\n";
$client->resetRetryMetrics();
echo "After reset: " . $client->getTotalRetryAttempts() . "\n";

// Test 7: Execute query (should not trigger retries on success)
$result = $client->query("SELECT 1 as test");
echo "Query result: " . $result[0]['test'] . "\n";
echo "Retries after query: " . $client->getTotalRetryAttempts() . "\n";

// Test 8: Unlimited retries (0)
echo "\nTest 5: Unlimited retries\n";
$client->setMaxRetryAttempts(0);
echo "Max attempts (0 = unlimited): " . $client->getMaxRetryAttempts() . "\n";

echo "\nOK\n";
?>
--EXPECT--
Test 1: Default retry configuration
Auto-reconnect: no
Max retry attempts: 3
Base delay: 0.1
Max delay: 5
Jitter: yes
Total retries: 0

Test 2: Custom configuration
Auto-reconnect: yes
Max retry attempts: 5
Base delay: 0.05
Max delay: 2
Jitter: no

Test 3: Validation
Negative attempts: rejected
Negative delay: rejected
Base > max: rejected

Test 4: Retry metrics
After reset: 0
Query result: 1
Retries after query: 0

Test 5: Unlimited retries
Max attempts (0 = unlimited): 0

OK
