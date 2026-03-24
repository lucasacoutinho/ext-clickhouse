--TEST--
CompressionMethod and Type enum values
--EXTENSIONS--
clickhouse
--FILE--
<?php
use ClickHouse\Driver\CompressionMethod;
use ClickHouse\Driver\Type;

// CompressionMethod
var_dump(CompressionMethod::None->value);
var_dump(CompressionMethod::LZ4->value);
var_dump(CompressionMethod::ZSTD->value);

// Type enum — spot check key values
var_dump(Type::Int8->value);
var_dump(Type::UInt64->value);
var_dump(Type::String->value);
var_dump(Type::Nullable->value);
var_dump(Type::Array->value);
var_dump(Type::Map->value);
var_dump(Type::DateTime64->value);

// Backed enum from value
var_dump(Type::from(11) === Type::String);
var_dump(CompressionMethod::from(1) === CompressionMethod::LZ4);
?>
--EXPECT--
int(-1)
int(1)
int(2)
int(1)
int(8)
int(11)
int(16)
int(15)
int(32)
int(30)
bool(true)
bool(true)
