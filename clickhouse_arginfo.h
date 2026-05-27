/* This is a generated-like file. Regenerate with:
 *   php php-src/build/gen_stub.php ext-clickhouse/clickhouse.stub.php
 */

ZEND_BEGIN_ARG_INFO_EX(arginfo_class_ClickHouse_Driver_ClientOptions___construct, 0, 0, 0)
    ZEND_ARG_TYPE_INFO_WITH_DEFAULT_VALUE(0, host, IS_STRING, 0, "\"localhost\"")
    ZEND_ARG_TYPE_INFO_WITH_DEFAULT_VALUE(0, port, IS_LONG, 0, "9000")
    ZEND_ARG_TYPE_INFO_WITH_DEFAULT_VALUE(0, database, IS_STRING, 0, "\"default\"")
    ZEND_ARG_TYPE_INFO_WITH_DEFAULT_VALUE(0, user, IS_STRING, 0, "\"default\"")
    ZEND_ARG_TYPE_INFO_WITH_DEFAULT_VALUE(0, password, IS_STRING, 0, "\"\"")
#if PHP_VERSION_ID >= 80100
    ZEND_ARG_OBJ_INFO_WITH_DEFAULT_VALUE(0, compression, ClickHouse\\Driver\\CompressionMethod, 0, "ClickHouse\\Driver\\CompressionMethod::None")
#else
    ZEND_ARG_TYPE_INFO_WITH_DEFAULT_VALUE(0, compression, IS_LONG, 0, "ClickHouse\\Driver\\CompressionMethod::None")
#endif
    ZEND_ARG_TYPE_INFO_WITH_DEFAULT_VALUE(0, pingBeforeQuery, _IS_BOOL, 0, "false")
    ZEND_ARG_TYPE_INFO_WITH_DEFAULT_VALUE(0, sendRetries, IS_LONG, 0, "1")
    ZEND_ARG_TYPE_INFO_WITH_DEFAULT_VALUE(0, retryTimeoutSeconds, IS_LONG, 0, "5")
    ZEND_ARG_TYPE_INFO_WITH_DEFAULT_VALUE(0, tcpKeepAlive, _IS_BOOL, 0, "false")
    ZEND_ARG_TYPE_INFO_WITH_DEFAULT_VALUE(0, tcpNoDelay, _IS_BOOL, 0, "true")
    ZEND_ARG_TYPE_INFO_WITH_DEFAULT_VALUE(0, connectTimeoutMs, IS_LONG, 0, "5000")
    ZEND_ARG_TYPE_INFO_WITH_DEFAULT_VALUE(0, recvTimeoutMs, IS_LONG, 0, "0")
    ZEND_ARG_TYPE_INFO_WITH_DEFAULT_VALUE(0, sendTimeoutMs, IS_LONG, 0, "0")
    ZEND_ARG_TYPE_INFO_WITH_DEFAULT_VALUE(0, ssl, IS_ARRAY, 1, "null")
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(arginfo_class_ClickHouse_Driver_Client___construct, 0, 0, 1)
    ZEND_ARG_OBJ_INFO(0, options, ClickHouse\\Driver\\ClientOptions, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_class_ClickHouse_Driver_Client_execute, 0, 1, IS_VOID, 0)
    ZEND_ARG_TYPE_INFO(0, query, IS_STRING, 0)
    ZEND_ARG_TYPE_INFO_WITH_DEFAULT_VALUE(0, params, IS_ARRAY, 1, "null")
    ZEND_ARG_TYPE_INFO_WITH_DEFAULT_VALUE(0, settings, IS_ARRAY, 1, "null")
    ZEND_ARG_TYPE_INFO_WITH_DEFAULT_VALUE(0, queryId, IS_STRING, 1, "null")
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_class_ClickHouse_Driver_Client_select, 0, 1, IS_ARRAY, 0)
    ZEND_ARG_TYPE_INFO(0, query, IS_STRING, 0)
    ZEND_ARG_TYPE_INFO_WITH_DEFAULT_VALUE(0, params, IS_ARRAY, 1, "null")
    ZEND_ARG_TYPE_INFO_WITH_DEFAULT_VALUE(0, settings, IS_ARRAY, 1, "null")
    ZEND_ARG_TYPE_INFO_WITH_DEFAULT_VALUE(0, queryId, IS_STRING, 1, "null")
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_class_ClickHouse_Driver_Client_selectByBlock, 0, 2, IS_VOID, 0)
    ZEND_ARG_TYPE_INFO(0, query, IS_STRING, 0)
    ZEND_ARG_TYPE_INFO(0, callback, IS_CALLABLE, 0)
    ZEND_ARG_TYPE_INFO_WITH_DEFAULT_VALUE(0, params, IS_ARRAY, 1, "null")
    ZEND_ARG_TYPE_INFO_WITH_DEFAULT_VALUE(0, settings, IS_ARRAY, 1, "null")
    ZEND_ARG_TYPE_INFO_WITH_DEFAULT_VALUE(0, queryId, IS_STRING, 1, "null")
    ZEND_ARG_TYPE_INFO_WITH_DEFAULT_VALUE(0, onProgress, IS_CALLABLE, 1, "null")
    ZEND_ARG_TYPE_INFO_WITH_DEFAULT_VALUE(0, onProfile, IS_CALLABLE, 1, "null")
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_class_ClickHouse_Driver_Client_insert, 0, 2, IS_VOID, 0)
    ZEND_ARG_TYPE_INFO(0, tableName, IS_STRING, 0)
    ZEND_ARG_OBJ_INFO(0, block, ClickHouse\\Driver\\Block, 0)
    ZEND_ARG_TYPE_INFO_WITH_DEFAULT_VALUE(0, queryId, IS_STRING, 1, "null")
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_class_ClickHouse_Driver_Client_selectWithExternalData, 0, 2, IS_ARRAY, 0)
    ZEND_ARG_TYPE_INFO(0, query, IS_STRING, 0)
    ZEND_ARG_TYPE_INFO(0, externalTables, IS_ARRAY, 0)
    ZEND_ARG_TYPE_INFO_WITH_DEFAULT_VALUE(0, params, IS_ARRAY, 1, "null")
    ZEND_ARG_TYPE_INFO_WITH_DEFAULT_VALUE(0, settings, IS_ARRAY, 1, "null")
    ZEND_ARG_TYPE_INFO_WITH_DEFAULT_VALUE(0, queryId, IS_STRING, 1, "null")
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_class_ClickHouse_Driver_Client_ping, 0, 0, IS_VOID, 0)
ZEND_END_ARG_INFO()

#define arginfo_class_ClickHouse_Driver_Client_resetConnection arginfo_class_ClickHouse_Driver_Client_ping
#define arginfo_class_ClickHouse_Driver_Client_resetConnectionEndpoint arginfo_class_ClickHouse_Driver_Client_ping

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_class_ClickHouse_Driver_Client_getCurrentEndpoint, 0, 0, IS_ARRAY, 1)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_OBJ_INFO_EX(arginfo_class_ClickHouse_Driver_Client_getServerInfo, 0, 0, ClickHouse\\Driver\\ServerInfo, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_INFO_EX(arginfo_class_ClickHouse_Driver_Block___construct, 0, 0, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_class_ClickHouse_Driver_Block_appendColumn, 0, 2, IS_VOID, 0)
    ZEND_ARG_TYPE_INFO(0, name, IS_STRING, 0)
    ZEND_ARG_OBJ_INFO(0, column, ClickHouse\\Driver\\Column, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_class_ClickHouse_Driver_Block_getColumnCount, 0, 0, IS_LONG, 0)
ZEND_END_ARG_INFO()

#define arginfo_class_ClickHouse_Driver_Block_getRowCount arginfo_class_ClickHouse_Driver_Block_getColumnCount

ZEND_BEGIN_ARG_WITH_RETURN_OBJ_INFO_EX(arginfo_class_ClickHouse_Driver_Block_getColumn, 0, 1, ClickHouse\\Driver\\Column, 0)
    ZEND_ARG_TYPE_INFO(0, index, IS_LONG, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_class_ClickHouse_Driver_Block_getColumnName, 0, 1, IS_STRING, 0)
    ZEND_ARG_TYPE_INFO(0, index, IS_LONG, 0)
ZEND_END_ARG_INFO()

#if PHP_VERSION_ID >= 80100
ZEND_BEGIN_ARG_WITH_RETURN_OBJ_INFO_EX(arginfo_class_ClickHouse_Driver_Block_getColumnType, 0, 1, ClickHouse\\Driver\\Type, 0)
#else
ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_class_ClickHouse_Driver_Block_getColumnType, 0, 1, IS_LONG, 0)
#endif
    ZEND_ARG_TYPE_INFO(0, index, IS_LONG, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_class_ClickHouse_Driver_Block_getColumnTypeName, 0, 1, IS_STRING, 0)
    ZEND_ARG_TYPE_INFO(0, index, IS_LONG, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_class_ClickHouse_Driver_Block_toArray, 0, 0, IS_ARRAY, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_OBJ_INFO_EX(arginfo_class_ClickHouse_Driver_Column_create, 0, 2, ClickHouse\\Driver\\Column, 0)
    ZEND_ARG_TYPE_INFO(0, typeName, IS_STRING, 0)
    ZEND_ARG_TYPE_INFO(0, values, IS_ARRAY, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_class_ClickHouse_Driver_Column_getTypeName, 0, 0, IS_STRING, 0)
ZEND_END_ARG_INFO()

#if PHP_VERSION_ID >= 80100
ZEND_BEGIN_ARG_WITH_RETURN_OBJ_INFO_EX(arginfo_class_ClickHouse_Driver_Column_getType, 0, 0, ClickHouse\\Driver\\Type, 0)
#else
ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_class_ClickHouse_Driver_Column_getType, 0, 0, IS_LONG, 0)
#endif
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_class_ClickHouse_Driver_Column_size, 0, 0, IS_LONG, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_class_ClickHouse_Driver_Column_at, 0, 1, IS_MIXED, 0)
    ZEND_ARG_TYPE_INFO(0, index, IS_LONG, 0)
ZEND_END_ARG_INFO()

#define arginfo_class_ClickHouse_Driver_Column_toArray arginfo_class_ClickHouse_Driver_Block_toArray

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_class_ClickHouse_Driver_Exception_ServerException_getClickHouseCode, 0, 0, IS_LONG, 0)
ZEND_END_ARG_INFO()
