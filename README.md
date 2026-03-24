# ext-clickhouse

A PHP extension providing native TCP access to ClickHouse using the [clickhouse-cpp](https://github.com/ClickHouse/clickhouse-cpp) C++ client library. Communicates over the ClickHouse native protocol with LZ4/ZSTD compression support.

## Requirements

- PHP 8.1+
- C++17 compiler (GCC 8+ or Clang 7+)
## Build

```bash
git clone --recursive https://github.com/lightprofco/ext-clickhouse.git
cd ext-clickhouse

phpize
./configure --enable-clickhouse
make
make install
```

If you cloned without `--recursive`, fetch the submodule:

```bash
git submodule update --init --recursive
```

Add to your `php.ini`:

```ini
extension=clickhouse
```

## Usage

```php
use ClickHouse\Driver\Client;
use ClickHouse\Driver\ClientOptions;
use ClickHouse\Driver\Block;
use ClickHouse\Driver\Column;
use ClickHouse\Driver\CompressionMethod;

$client = new Client(new ClientOptions(
    host: '127.0.0.1',
    port: 9000,
    compression: CompressionMethod::LZ4,
));

$client->ping();

// DDL
$client->execute('CREATE TABLE IF NOT EXISTS test (id UInt64, name String) ENGINE = Memory');

// Insert
$block = new Block();
$block->appendColumn('id', Column::create('UInt64', [1, 2, 3]));
$block->appendColumn('name', Column::create('String', ['Alice', 'Bob', 'Charlie']));
$client->insert('test', $block);

// Select
$rows = $client->select('SELECT * FROM test');

// Block-by-block streaming
$client->selectByBlock('SELECT * FROM test', function (Block $block): void {
    foreach ($block->toArray() as $row) {
        // process row
    }
});
```

## Docker

Pre-built images are available on GitHub Container Registry:

```bash
docker pull ghcr.io/lightprofco/ext-clickhouse:php8.4-latest
```

Or build locally:

```bash
docker build --build-arg PHP_VERSION=8.4 -t ext-clickhouse .
```

## Running tests

```bash
# Requires a running ClickHouse instance
CLICKHOUSE_HOST=127.0.0.1 make test
```

## License

[MIT](LICENSE)
