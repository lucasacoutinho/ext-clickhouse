--TEST--
Extension is loaded and classes exist
--EXTENSIONS--
clickhouse
--FILE--
<?php
var_dump(extension_loaded('clickhouse'));

// Core classes
var_dump(class_exists('ClickHouse\Driver\Client'));
var_dump(class_exists('ClickHouse\Driver\ClientOptions'));
var_dump(class_exists('ClickHouse\Driver\Block'));
var_dump(class_exists('ClickHouse\Driver\Column'));
var_dump(class_exists('ClickHouse\Driver\ServerInfo'));

// Enums
var_dump(enum_exists('ClickHouse\Driver\CompressionMethod'));
var_dump(enum_exists('ClickHouse\Driver\Type'));

// Exceptions
var_dump(class_exists('ClickHouse\Driver\Exception\ClickHouseException'));
var_dump(class_exists('ClickHouse\Driver\Exception\ConnectionException'));
var_dump(class_exists('ClickHouse\Driver\Exception\ServerException'));
var_dump(class_exists('ClickHouse\Driver\Exception\ProtocolException'));
var_dump(class_exists('ClickHouse\Driver\Exception\ValidationException'));
?>
--EXPECT--
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
bool(true)
