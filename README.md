# ext-clickhouse

A PHP extension providing native TCP access to ClickHouse using the [clickhouse-cpp](https://github.com/ClickHouse/clickhouse-cpp) C++ client library. Communicates over the ClickHouse native protocol with LZ4/ZSTD compression support.

## Requirements

- PHP 7.4, 8.0, 8.1, 8.2, 8.3, 8.4, or 8.5
- C++17 compiler (GCC 8+ or Clang 7+)

## PHP version support

CI builds and tests the extension on PHP 7.4, 8.0, 8.1, 8.2, 8.3, 8.4, and 8.5.

On PHP 8.1+, `ClickHouse\Driver\CompressionMethod` and `ClickHouse\Driver\Type` are native backed enums. On PHP 7.4 and 8.0, the same names are final classes with integer constants, so code can still use constants such as `CompressionMethod::LZ4`. Methods returning `Type` return integer constants on PHP 7.4 and 8.0, and enum cases on PHP 8.1+.

## clickhouse-cpp versioning

`clickhouse-cpp` is pinned as a git submodule and built into `clickhouse.so`; users do not install `clickhouse-cpp` separately. The extension has its own release version, and each release documents the embedded `clickhouse-cpp` version. `phpinfo()` also reports the embedded C++ client version.

The current submodule pin is `clickhouse-cpp` `v2.6.0`.

To upgrade the C++ client, bump the submodule to an upstream `clickhouse-cpp` tag, run the full PHP matrix, then publish a new extension release. Release source archives must include the initialized submodule so PIE can build without requiring git submodule operations.

For release builds, treat the submodule SHA as part of the extension source. Do not link against a system-installed `clickhouse-cpp` by default; that would make extension behavior depend on distro packaging and local C++ ABI choices.

Release versioning is independent from `clickhouse-cpp`: dependency-only bugfix or security bumps can be patch releases, observable protocol/type/TLS support changes should be minor releases, and PHP API/ABI breaks require a major release.
## Build

```bash
git clone --recursive https://github.com/lucasacoutinho/ext-clickhouse.git
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

## Install with PIE

```bash
pie install lucasacoutinho/ext-clickhouse
```

## Usage

```php
use ClickHouse\Driver\Client;
use ClickHouse\Driver\ClientOptions;
use ClickHouse\Driver\Block;
use ClickHouse\Driver\Column;
use ClickHouse\Driver\CompressionMethod;

$client = new Client(new ClientOptions(
    '127.0.0.1',
    9000,
    'default',
    'default',
    '',
    CompressionMethod::LZ4
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

### TLS

Pass an SSL option array as the 15th `ClientOptions` constructor argument. Defaults are secure when SSL is enabled: system CA locations and SNI are enabled unless explicitly overridden.

```php
$client = new Client(new ClientOptions(
    'host.example.com',
    9440,
    'default',
    'default',
    'secret',
    CompressionMethod::LZ4,
    false,
    1,
    5,
    false,
    true,
    5000,
    0,
    0,
    [
        'ca_file' => '/path/to/ca.pem',
        'client_cert' => '/path/to/client.crt',
        'client_key' => '/path/to/client.key',
    ]
));
```

## Docker

Pre-built images are available on GitHub Container Registry:

```bash
docker pull ghcr.io/lucasacoutinho/ext-clickhouse:php8.5-latest
```

Or build locally:

```bash
docker build --build-arg PHP_VERSION=8.5 -t ext-clickhouse .
```

## Running tests

```bash
# Requires a running ClickHouse instance
CLICKHOUSE_HOST=127.0.0.1 make test
```

## License

[MIT](LICENSE)
