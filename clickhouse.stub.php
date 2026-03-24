<?php

/**
 * @generate-class-entries
 * @undocumentable
 */

namespace ClickHouse\Driver;

enum CompressionMethod: int {
    case None = -1;
    case LZ4 = 1;
    case ZSTD = 2;
}

enum Type: int {
    case Void = 0;
    case Int8 = 1;
    case Int16 = 2;
    case Int32 = 3;
    case Int64 = 4;
    case UInt8 = 5;
    case UInt16 = 6;
    case UInt32 = 7;
    case UInt64 = 8;
    case Float32 = 9;
    case Float64 = 10;
    case String = 11;
    case FixedString = 12;
    case DateTime = 13;
    case Date = 14;
    case Array = 15;
    case Nullable = 16;
    case Tuple = 17;
    case Enum8 = 18;
    case Enum16 = 19;
    case UUID = 20;
    case IPv4 = 21;
    case IPv6 = 22;
    case Int128 = 23;
    case UInt128 = 24;
    case Decimal = 25;
    case Decimal32 = 26;
    case Decimal64 = 27;
    case Decimal128 = 28;
    case LowCardinality = 29;
    case DateTime64 = 30;
    case Date32 = 31;
    case Map = 32;
    case Point = 33;
    case Ring = 34;
    case Polygon = 35;
    case MultiPolygon = 36;
}

final class ClientOptions {
    public function __construct(
        string $host = 'localhost',
        int $port = 9000,
        string $database = 'default',
        string $user = 'default',
        string $password = '',
        CompressionMethod $compression = CompressionMethod::None,
        bool $pingBeforeQuery = false,
        int $sendRetries = 1,
        int $retryTimeoutSeconds = 5,
        bool $tcpKeepAlive = false,
        bool $tcpNoDelay = true,
        int $connectTimeoutMs = 5000,
        int $recvTimeoutMs = 0,
        int $sendTimeoutMs = 0,
        ?array $ssl = null,
        /** @var array<array{host: string, port?: int}> Additional endpoints for failover */
        ?array $endpoints = null,
        int $tcpKeepAliveIdleSeconds = 60,
        int $tcpKeepAliveIntervalSeconds = 5,
        int $tcpKeepAliveCount = 3,
        int $maxCompressionChunkSize = 65535,
    ) {}
}

final class Client {
    public function __construct(ClientOptions $options) {}

    public function execute(string $query, ?array $params = null, ?array $settings = null, ?string $queryId = null): void {}

    public function select(string $query, ?array $params = null, ?array $settings = null, ?string $queryId = null): array {}

    /**
     * @param callable(Block): bool|void $callback     Called per data block. Return false to cancel.
     * @param callable(array): void|null $onProgress   Called with ['rows'=>int,'bytes'=>int,'total_rows'=>int,'written_rows'=>int,'written_bytes'=>int]
     * @param callable(array): void|null $onProfile    Called with ['rows'=>int,'blocks'=>int,'bytes'=>int,'rows_before_limit'=>int,'applied_limit'=>bool]
     */
    public function selectByBlock(
        string $query,
        callable $callback,
        ?array $params = null,
        ?array $settings = null,
        ?string $queryId = null,
        ?callable $onProgress = null,
        ?callable $onProfile = null,
    ): void {}

    public function insert(string $tableName, Block $block, ?string $queryId = null): void {}

    /**
     * Execute SELECT with external temporary tables.
     * @param array<array{name: string, data: Block}> $externalTables
     */
    public function selectWithExternalData(string $query, array $externalTables, ?array $params = null, ?array $settings = null, ?string $queryId = null): array {}

    public function ping(): void {}

    public function resetConnection(): void {}

    /** Try to connect to a different endpoint (round-robin failover) */
    public function resetConnectionEndpoint(): void {}

    /** Get the currently connected endpoint as ['host' => string, 'port' => int], or null */
    public function getCurrentEndpoint(): ?array {}

    public function getServerInfo(): ServerInfo {}
}

final class Block {
    public function __construct() {}

    public function appendColumn(string $name, Column $column): void {}

    public function getColumnCount(): int {}

    public function getRowCount(): int {}

    public function getColumn(int $index): Column {}

    public function getColumnName(int $index): string {}

    public function getColumnType(int $index): Type {}

    public function getColumnTypeName(int $index): string {}

    public function toArray(): array {}
}

final class Column {
    public static function create(string $typeName, array $values): Column {}

    public function getTypeName(): string {}

    public function getType(): Type {}

    public function size(): int {}

    public function at(int $index): mixed {}

    public function toArray(): array {}
}

readonly class ServerInfo {
    public string $name;
    public string $timezone;
    public string $displayName;
    public int $versionMajor;
    public int $versionMinor;
    public int $versionPatch;
    public int $revision;
}

namespace ClickHouse\Driver\Exception;

class ClickHouseException extends \RuntimeException {}

class ConnectionException extends ClickHouseException {}

class ServerException extends ClickHouseException {
    public function getClickHouseCode(): int {}
}

class ProtocolException extends ClickHouseException {}

class ValidationException extends \InvalidArgumentException {}
