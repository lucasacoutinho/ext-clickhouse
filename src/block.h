#ifndef PHP_CLICKHOUSE_BLOCK_H
#define PHP_CLICKHOUSE_BLOCK_H

#include "php_clickhouse.h"
#include "clickhouse/block.h"

#include <memory>

struct php_clickhouse_block
{
    std::unique_ptr<clickhouse::Block> block;
    zend_object std;
};

static inline php_clickhouse_block *php_clickhouse_block_from_obj(zend_object *obj)
{
    return reinterpret_cast<php_clickhouse_block *>(reinterpret_cast<char *>(obj) -
                                                    XtOffsetOf(php_clickhouse_block, std));
}

static inline php_clickhouse_block *php_clickhouse_block_from_zval(zval *zv)
{
    return php_clickhouse_block_from_obj(Z_OBJ_P(zv));
}

#define Z_CLICKHOUSE_BLOCK_P(zv) php_clickhouse_block_from_obj(Z_OBJ_P(zv))

void php_clickhouse_register_block(int module_number);

/* Create a PHP Block object from a C++ Block (copies column refs) */
void php_clickhouse_create_block_from_cpp(zval *return_value, const clickhouse::Block &cpp_block);

#endif
