# ClickHouse Native Extension

A PHP extension for ClickHouse using the native TCP protocol.

## Installation

```bash
phpize
./configure
make
sudo make install
```

Enable the extension in `php.ini`:
```ini
extension=clickhouse.so
```

## Requirements

- PHP 8.1+
- ClickHouse server
- LZ4 library (optional, for compression)
- ZSTD library (optional, for compression)
- OpenSSL (optional, for TLS)

## Usage

```php
$client = new ClickHouse\Client('localhost', 9000);

// Simple query
$result = $client->query('SELECT version()');
print_r($result);

// Insert data
$client->insert('users', ['id', 'name'], [
    [1, 'Alice'],
    [2, 'Bob']
]);

// Parameters
$client->execute('INSERT INTO users VALUES ({id}, {name})', [
    'id' => 1,
    'name' => 'Alice'
]);
```

## Features

- Native TCP protocol
- All ClickHouse data types
- Compression (LZ4, ZSTD)
- SSL/TLS support
- Streaming results
- Async queries
- Prepared statements
- Progress callbacks
- Transaction support (experimental)

## Testing

```bash
make test
```

## License

MIT License
