#ifndef PHP_CLICKHOUSE_ERROR_CODES_H
#define PHP_CLICKHOUSE_ERROR_CODES_H

#include "php_clickhouse.h"

extern zend_class_entry *clickhouse_ce_ErrorCode;

void php_clickhouse_register_error_codes(int module_number);

#endif
