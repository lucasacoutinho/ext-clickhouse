#ifndef PHP_CLICKHOUSE_CLIENT_H
#define PHP_CLICKHOUSE_CLIENT_H

#include "php_clickhouse.h"
#include "clickhouse/client.h"

#include <memory>

struct php_clickhouse_client
{
    std::unique_ptr<clickhouse::Client> client;
    zend_object std;
};

static inline php_clickhouse_client *php_clickhouse_client_from_obj(zend_object *obj)
{
    return reinterpret_cast<php_clickhouse_client *>(reinterpret_cast<char *>(obj) -
                                                     XtOffsetOf(php_clickhouse_client, std));
}

#define Z_CLICKHOUSE_CLIENT_P(zv) php_clickhouse_client_from_obj(Z_OBJ_P(zv))

void php_clickhouse_register_client(int module_number);

#endif
