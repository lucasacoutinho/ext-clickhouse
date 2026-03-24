<?php
/**
 * Test helper for ext-clickhouse integration tests.
 *
 * Environment variables:
 *   CLICKHOUSE_HOST  (default: localhost)
 *   CLICKHOUSE_PORT  (default: 9000)
 *   CLICKHOUSE_USER  (default: default)
 *   CLICKHOUSE_PASS  (default: empty)
 *   CLICKHOUSE_DB    (default: default)
 */

function clickhouse_test_options(): ClickHouse\Driver\ClientOptions {
    return new ClickHouse\Driver\ClientOptions(
        host: getenv('CLICKHOUSE_HOST') ?: 'localhost',
        port: (int)(getenv('CLICKHOUSE_PORT') ?: 9000),
        database: getenv('CLICKHOUSE_DB') ?: 'default',
        user: getenv('CLICKHOUSE_USER') ?: 'default',
        password: getenv('CLICKHOUSE_PASS') ?: '',
    );
}

function clickhouse_test_client(): ClickHouse\Driver\Client {
    return new ClickHouse\Driver\Client(clickhouse_test_options());
}

function clickhouse_test_skip(): void {
    try {
        $client = clickhouse_test_client();
        $client->ping();
    } catch (\Throwable $e) {
        die('skip ClickHouse not available: ' . $e->getMessage());
    }
}
