<div align="center">
  <h1>ext-clickhouse</h1>
  <p>
    A native ClickHouse client for PHP. It speaks the native TCP protocol and
    ships the C++ client inside <code>clickhouse.so</code>.
  </p>
  <p>
    <a href="https://github.com/lucasacoutinho/ext-clickhouse/actions/workflows/ci.yml"><img alt="Build status" src="https://img.shields.io/github/actions/workflow/status/lucasacoutinho/ext-clickhouse/ci.yml?branch=main&style=for-the-badge&labelColor=000000"></a>
    <a href="https://packagist.org/packages/lucasacoutinho/ext-clickhouse"><img alt="Packagist version" src="https://img.shields.io/packagist/v/lucasacoutinho/ext-clickhouse?style=for-the-badge&labelColor=000000"></a>
    <a href="#requirements"><img alt="PHP 7.4 through 8.5" src="https://img.shields.io/badge/PHP-7.4%20to%208.5-777BB4?style=for-the-badge&logo=php&logoColor=white&labelColor=000000"></a>
    <a href="https://github.com/lucasacoutinho/ext-clickhouse/blob/main/LICENSE"><img alt="MIT license" src="https://img.shields.io/github/license/lucasacoutinho/ext-clickhouse?style=for-the-badge&labelColor=000000"></a>
  </p>
</div>

## Getting started

Install the extension with [PIE](https://github.com/php/pie):

```bash
pie install lucasacoutinho/ext-clickhouse
```

Enable it in `php.ini`:

```ini
extension=clickhouse
```

Create a client and check the connection:

```php
use ClickHouse\Driver\Client;
use ClickHouse\Driver\ClientOptions;
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
```

The driver connects to ClickHouse over its native TCP port, usually `9000`.
It supports LZ4 and ZSTD compression, TLS, typed columns, inserts, and
block-by-block result streaming without an HTTP or cURL transport.

## Requirements

| Component | Supported version |
| --- | --- |
| PHP | 7.4 through 8.5 |
| Compiler for source builds | GCC 8+, Clang 7+, or another C++17 compiler |
| ClickHouse C++ client | Bundled with the extension |

CI builds and tests every supported PHP version.

On PHP 8.1+, `ClickHouse\Driver\CompressionMethod` and
`ClickHouse\Driver\Type` are native backed enums. PHP 7.4 and 8.0 expose the
same names as final classes with integer constants. Code can use constants
such as `CompressionMethod::LZ4` on every supported PHP version. Methods that
return `Type` return integer constants on PHP 7.4 and 8.0, and enum cases on
PHP 8.1+.

## Working with data

```php
use ClickHouse\Driver\Block;
use ClickHouse\Driver\Column;

$client->execute(
    'CREATE TABLE IF NOT EXISTS test '
    . '(id UInt64, name String) ENGINE = Memory'
);

$block = new Block();
$block->appendColumn('id', Column::create('UInt64', [1, 2, 3]));
$block->appendColumn(
    'name',
    Column::create('String', ['Alice', 'Bob', 'Charlie'])
);

$client->insert('test', $block);

$rows = $client->select('SELECT * FROM test ORDER BY id');

$client->selectByBlock('SELECT * FROM test', function (Block $block): void {
    foreach ($block->toArray() as $row) {
        // Process one result block at a time.
    }
});
```

The public PHP API is declared in
[`clickhouse.stub.php`](clickhouse.stub.php).

## TLS

Pass an SSL option array as the 15th `ClientOptions` constructor argument.
When SSL is enabled, the client uses system CA locations and SNI unless the
options override them.

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

## Build from source

Clone the initialized submodule, then use the standard PHP extension build
flow:

```bash
git clone --recursive https://github.com/lucasacoutinho/ext-clickhouse.git
cd ext-clickhouse

phpize
./configure --enable-clickhouse
make
make install
```

If the repository was cloned without `--recursive`, initialize the dependency
before building:

```bash
git submodule update --init --recursive
```

## Docker

Versioned and rolling images are published for each supported PHP release:

```bash
docker pull ghcr.io/lucasacoutinho/ext-clickhouse:php8.5-v1.4.0
docker pull ghcr.io/lucasacoutinho/ext-clickhouse:php8.5-latest
```

Build the image locally with a different PHP version when needed:

```bash
docker build --build-arg PHP_VERSION=8.5 -t ext-clickhouse .
```

## Bundled clickhouse-cpp

The repository pins `clickhouse-cpp` as a git submodule and compiles it into
`clickhouse.so`. Users do not install or link a separate system copy.
`phpinfo()` reports the embedded client version.

The v1.4 release line reports `clickhouse-cpp` v2.6.2 and pins upstream commit
`a63f50d97b7ae034ef0ac5a620ecfd785289f9b7`. This post-tag snapshot adds
LowCardinality dictionary types, fixes nested SimpleAggregateFunction columns,
and reports malformed protocol packets as errors.

The build explicitly preserves native Bool values, Abseil-backed wide integers,
and the optional C++ endpoint API. Rebuild `ext-clickhouse-pdo` against this
revision and use matching 1.4.x native extensions; the upstream C++ object
layouts changed.

The submodule SHA is part of the extension source and release contract. New
pins should prefer upstream tags. A post-tag pin must document the fixes it
needs and pass the full PHP matrix. Source archives include the initialized
submodule so PIE can build without running git commands.

Extension releases use their own version numbers. A dependency-only bug fix or
security update can be a patch release. Observable protocol, type, or TLS
changes require a minor release. PHP API or ABI breaks require a major release.

## Testing

Run the PHPT suite against a live ClickHouse server:

```bash
CLICKHOUSE_HOST=127.0.0.1 make test
```

The GitHub Actions matrix also runs integration tests, sanitizers, coverage,
formatting, and clang-tidy on every supported PHP version.

## Contributing

Bug reports and focused pull requests are welcome. Open an
[issue](https://github.com/lucasacoutinho/ext-clickhouse/issues) with the PHP
version, ClickHouse version, and a minimal reproduction for driver problems.

## License

[MIT](LICENSE)
