#ifndef PHP_CLICKHOUSE_CLIENT_OPTIONS_H
#define PHP_CLICKHOUSE_CLIENT_OPTIONS_H

#include "php_clickhouse.h"
#include "clickhouse/client.h"

#include <memory>

struct php_clickhouse_client_options
{
    std::unique_ptr<clickhouse::ClientOptions> options;
    zend_object std;
};

static inline php_clickhouse_client_options *
php_clickhouse_client_options_from_obj(zend_object *obj)
{
    return reinterpret_cast<php_clickhouse_client_options *>(
        reinterpret_cast<char *>(obj) - XtOffsetOf(php_clickhouse_client_options, std));
}

#define Z_CLICKHOUSE_OPTIONS_P(zv) php_clickhouse_client_options_from_obj(Z_OBJ_P(zv))

void php_clickhouse_register_client_options(int module_number);
void php_clickhouse_register_enums(int module_number);
void php_clickhouse_register_server_info(int module_number);

#endif
