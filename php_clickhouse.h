/*
  +----------------------------------------------------------------------+
  | ClickHouse Native Driver for PHP                                    |
  +----------------------------------------------------------------------+
  | Copyright (c) 2024                                                  |
  +----------------------------------------------------------------------+
  | This source file is subject to the MIT license that is bundled      |
  | with this package in the file LICENSE.                              |
  +----------------------------------------------------------------------+
*/

#ifndef PHP_CLICKHOUSE_H
#define PHP_CLICKHOUSE_H

extern zend_module_entry clickhouse_module_entry;
#define phpext_clickhouse_ptr &clickhouse_module_entry

#define PHP_CLICKHOUSE_VERSION "0.1.0"
#define PHP_CLICKHOUSE_EXTNAME "clickhouse"

#ifdef PHP_WIN32
#   define PHP_CLICKHOUSE_API __declspec(dllexport)
#elif defined(__GNUC__) && __GNUC__ >= 4
#   define PHP_CLICKHOUSE_API __attribute__ ((visibility("default")))
#else
#   define PHP_CLICKHOUSE_API
#endif

#ifdef ZTS
#include "TSRM.h"
#endif

/* Module lifecycle */
PHP_MINIT_FUNCTION(clickhouse);
PHP_MSHUTDOWN_FUNCTION(clickhouse);
PHP_RINIT_FUNCTION(clickhouse);
PHP_RSHUTDOWN_FUNCTION(clickhouse);
PHP_MINFO_FUNCTION(clickhouse);

/* Include internal headers */
#include "src/buffer.h"
#include "src/protocol.h"
#include "src/connection.h"
#include "src/column.h"

/* Persistent connection list entry */
typedef struct {
    zend_ptr_stack free_connections;
} clickhouse_plist_entry;

/* Module globals */
ZEND_BEGIN_MODULE_GLOBALS(clickhouse)
    zend_bool allow_persistent;
    zend_long max_persistent;
    zend_long max_links;
    zend_long num_persistent;
    zend_long num_links;
ZEND_END_MODULE_GLOBALS(clickhouse)

#ifdef ZTS
#define CLICKHOUSE_G(v) ZEND_TSRMG(clickhouse_globals_id, zend_clickhouse_globals *, v)
#else
#define CLICKHOUSE_G(v) (clickhouse_globals.v)
#endif

ZEND_EXTERN_MODULE_GLOBALS(clickhouse)

#endif /* PHP_CLICKHOUSE_H */
