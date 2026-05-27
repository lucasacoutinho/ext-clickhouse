#ifndef PHP_CLICKHOUSE_H
#define PHP_CLICKHOUSE_H

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

extern "C" {
#include "php.h"
#include "ext/standard/info.h"
#include "zend_exceptions.h"
#if PHP_VERSION_ID >= 80100
#include "zend_enum.h"
#endif
}

#define PHP_CLICKHOUSE_VERSION "1.0.1"
#define PHP_CLICKHOUSE_EXTNAME "clickhouse"

#ifndef ZEND_ARG_TYPE_INFO_WITH_DEFAULT_VALUE
#define ZEND_ARG_TYPE_INFO_WITH_DEFAULT_VALUE(pass_by_ref, name, type_hint, allow_null,            \
                                              default_value)                                       \
    ZEND_ARG_TYPE_INFO(pass_by_ref, name, type_hint, allow_null)
#endif

#ifndef ZEND_ARG_OBJ_INFO_WITH_DEFAULT_VALUE
#define ZEND_ARG_OBJ_INFO_WITH_DEFAULT_VALUE(pass_by_ref, name, class_name, allow_null,            \
                                             default_value)                                        \
    ZEND_ARG_OBJ_INFO(pass_by_ref, name, class_name, allow_null)
#endif

#ifndef Z_PARAM_STR_OR_NULL
#define Z_PARAM_STR_OR_NULL(dest) Z_PARAM_STR_EX(dest, 1, 0)
#endif

#ifndef Z_PARAM_FUNC_OR_NULL
#define Z_PARAM_FUNC_OR_NULL(dest_fci, dest_fcc) Z_PARAM_FUNC_EX(dest_fci, dest_fcc, 1, 0)
#endif

#ifndef IS_MIXED
#define IS_MIXED IS_UNDEF
#endif

#ifdef ZEND_ACC_READONLY
#define PHP_CLICKHOUSE_ACC_READONLY ZEND_ACC_READONLY
#else
#define PHP_CLICKHOUSE_ACC_READONLY 0
#endif

#if PHP_VERSION_ID < 80000
#define PHP_CLICKHOUSE_PROPERTY_OBJECT(zv) (zv)
#else
#define PHP_CLICKHOUSE_PROPERTY_OBJECT(zv) Z_OBJ_P(zv)
#endif

extern "C" {
extern zend_module_entry clickhouse_module_entry;
}
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
