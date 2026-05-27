#ifndef PHP_CLICKHOUSE_COLUMN_H
#define PHP_CLICKHOUSE_COLUMN_H

#include "php_clickhouse.h"
#include "clickhouse/columns/column.h"

struct php_clickhouse_column
{
    clickhouse::ColumnRef column; /* shared_ptr — shared with Block */
    zend_object std;
};

static inline php_clickhouse_column *php_clickhouse_column_from_obj(zend_object *obj)
{
    return reinterpret_cast<php_clickhouse_column *>(reinterpret_cast<char *>(obj) -
                                                     XtOffsetOf(php_clickhouse_column, std));
}

static inline php_clickhouse_column *php_clickhouse_column_from_zval(zval *zv)
{
    return php_clickhouse_column_from_obj(Z_OBJ_P(zv));
}

#define Z_CLICKHOUSE_COLUMN_P(zv) php_clickhouse_column_from_obj(Z_OBJ_P(zv))

void php_clickhouse_register_column(int module_number);

/* Create a PHP Column object wrapping an existing ColumnRef */
void php_clickhouse_create_column_from_ref(zval *return_value, clickhouse::ColumnRef col_ref);

#endif
