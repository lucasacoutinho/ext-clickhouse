--TEST--
Exception class hierarchy
--EXTENSIONS--
clickhouse
--FILE--
<?php
use ClickHouse\Driver\Exception\ClickHouseException;
use ClickHouse\Driver\Exception\ConnectionException;
use ClickHouse\Driver\Exception\ServerException;
use ClickHouse\Driver\Exception\ProtocolException;
use ClickHouse\Driver\Exception\ValidationException;

// ClickHouseException extends Exception
$e = new ClickHouseException('test');
var_dump($e instanceof \Exception);
var_dump($e instanceof \Throwable);

// ConnectionException extends ClickHouseException
$e = new ConnectionException('conn');
var_dump($e instanceof ClickHouseException);
var_dump($e instanceof \Exception);

// ServerException extends ClickHouseException, has getClickHouseCode()
$e = new ServerException('srv');
var_dump($e instanceof ClickHouseException);
var_dump(method_exists($e, 'getClickHouseCode'));

// ProtocolException extends ClickHouseException
$e = new ProtocolException('proto');
var_dump($e instanceof ClickHouseException);

// ValidationException extends InvalidArgumentException
$e = new ValidationException('val');
var_dump($e instanceof \InvalidArgumentException);
var_dump(!($e instanceof ClickHouseException));
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
