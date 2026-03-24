#include "php_clickhouse.h"
#include "clickhouse/client.h"

#include <cstdio>

static PHP_MINIT_FUNCTION(clickhouse)
{
    php_clickhouse_register_exceptions(module_number);
    php_clickhouse_register_enums(module_number);
    php_clickhouse_register_server_info(module_number);
    php_clickhouse_register_client_options(module_number);
    php_clickhouse_register_client(module_number);
    php_clickhouse_register_block(module_number);
    php_clickhouse_register_column(module_number);
    php_clickhouse_register_error_codes(module_number);

    return SUCCESS;
}

static PHP_MSHUTDOWN_FUNCTION(clickhouse)
{
    return SUCCESS;
}

static PHP_MINFO_FUNCTION(clickhouse)
{
    auto ver = clickhouse::Client::GetVersion();
    char cpp_version[64];
    snprintf(cpp_version, sizeof(cpp_version), "%d.%d.%d",
        ver.major, ver.minor, ver.patch);

    php_info_print_table_start();
    php_info_print_table_header(2, "ClickHouse Native Driver", "enabled");
    php_info_print_table_row(2, "Extension Version", PHP_CLICKHOUSE_VERSION);
    php_info_print_table_row(2, "clickhouse-cpp Version", cpp_version);
    php_info_print_table_row(2, "Protocol", "Native TCP (port 9000)");
    php_info_print_table_row(2, "Compression", "LZ4, ZSTD");
    php_info_print_table_end();
}

extern "C" {

static const zend_function_entry clickhouse_functions[] = {
    PHP_FE_END
};

zend_module_entry clickhouse_module_entry = {
    STANDARD_MODULE_HEADER,
    PHP_CLICKHOUSE_EXTNAME,
    clickhouse_functions,
    PHP_MINIT(clickhouse),
    PHP_MSHUTDOWN(clickhouse),
    NULL, /* RINIT */
    NULL, /* RSHUTDOWN */
    PHP_MINFO(clickhouse),
    PHP_CLICKHOUSE_VERSION,
    STANDARD_MODULE_PROPERTIES
};

#ifdef COMPILE_DL_CLICKHOUSE
ZEND_GET_MODULE(clickhouse)
#endif

} /* extern "C" */
