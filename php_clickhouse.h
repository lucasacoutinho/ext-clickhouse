#ifndef PHP_CLICKHOUSE_H
#define PHP_CLICKHOUSE_H

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

extern "C" {
#include "php.h"
#include "ext/standard/info.h"
#include "zend_exceptions.h"
#include "zend_enum.h"
}

#define PHP_CLICKHOUSE_VERSION "0.1.0"
#define PHP_CLICKHOUSE_EXTNAME "clickhouse"

extern "C" { extern zend_module_entry clickhouse_module_entry; }
#define phpext_clickhouse_ptr &clickhouse_module_entry

/* Class entries — declared in their respective .cpp files, externed here */
extern zend_class_entry *clickhouse_ce_ClientOptions;
extern zend_class_entry *clickhouse_ce_Client;
extern zend_class_entry *clickhouse_ce_Block;
extern zend_class_entry *clickhouse_ce_Column;
extern zend_class_entry *clickhouse_ce_ServerInfo;
extern zend_class_entry *clickhouse_ce_CompressionMethod;
extern zend_class_entry *clickhouse_ce_Type;

/* Exception class entries */
extern zend_class_entry *clickhouse_ce_ClickHouseException;
extern zend_class_entry *clickhouse_ce_ConnectionException;
extern zend_class_entry *clickhouse_ce_ServerException;
extern zend_class_entry *clickhouse_ce_ProtocolException;
extern zend_class_entry *clickhouse_ce_ValidationException;

/* Error code class */
extern zend_class_entry *clickhouse_ce_ErrorCode;

/* Registration functions called from MINIT */
void php_clickhouse_register_exceptions(int module_number);
void php_clickhouse_register_enums(int module_number);
void php_clickhouse_register_client_options(int module_number);
void php_clickhouse_register_client(int module_number);
void php_clickhouse_register_block(int module_number);
void php_clickhouse_register_column(int module_number);
void php_clickhouse_register_server_info(int module_number);
void php_clickhouse_register_error_codes(int module_number);

#endif /* PHP_CLICKHOUSE_H */
