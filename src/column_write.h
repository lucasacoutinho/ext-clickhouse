#ifndef PHP_CLICKHOUSE_COLUMN_WRITE_H
#define PHP_CLICKHOUSE_COLUMN_WRITE_H

#include "php_clickhouse.h"
#include "clickhouse/columns/column.h"

/**
 * Append a PHP zval value to a typed ClickHouse column.
 * This is the INSERT write path: PHP → ClickHouse.
 * Throws PHP ValidationException on type mismatch.
 */
void php_clickhouse_zval_to_column(
    clickhouse::ColumnRef &col,
    zval *value);

#endif
