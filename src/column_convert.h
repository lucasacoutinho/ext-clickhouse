#ifndef PHP_CLICKHOUSE_COLUMN_CONVERT_H
#define PHP_CLICKHOUSE_COLUMN_CONVERT_H

#include "php_clickhouse.h"
#include "clickhouse/columns/column.h"

/**
 * Convert a value at row `index` from a ClickHouse column to a PHP zval.
 * This is the SELECT read path: ClickHouse → PHP.
 */
void php_clickhouse_column_to_zval(
    const clickhouse::ColumnRef &col,
    size_t index,
    zval *return_value);

#endif
