#include "src/client_options.h"
#include "src/common.h"
#include "clickhouse_arginfo.h"

#include <chrono>

zend_class_entry *clickhouse_ce_ClientOptions = nullptr;
zend_class_entry *clickhouse_ce_CompressionMethod = nullptr;
zend_class_entry *clickhouse_ce_Type = nullptr;
zend_class_entry *clickhouse_ce_ServerInfo = nullptr;

static zend_object_handlers clickhouse_client_options_handlers;

static zend_object *php_clickhouse_client_options_create(zend_class_entry *ce)
{
    auto *intern = static_cast<php_clickhouse_client_options *>(
        zend_object_alloc(sizeof(php_clickhouse_client_options), ce));

    new (&intern->options) std::unique_ptr<clickhouse::ClientOptions>();

    zend_object_std_init(&intern->std, ce);
    object_properties_init(&intern->std, ce);

    return &intern->std;
}

static void php_clickhouse_client_options_free(zend_object *object)
{
    auto *intern = php_clickhouse_client_options_from_obj(object);
    intern->options.~unique_ptr();
    zend_object_std_dtor(object);
}

ZEND_METHOD(ClickHouse_Driver_ClientOptions, __construct)
{
    zend_string *host = nullptr;
    zend_long port = 9000;
    zend_string *database = nullptr;
    zend_string *user = nullptr;
    zend_string *password = nullptr;
    zval *compression = nullptr;
    bool ping_before_query = false;
    zend_long send_retries = 1;
    zend_long retry_timeout_seconds = 5;
    bool tcp_keepalive = false;
    bool tcp_nodelay = true;
    zend_long connect_timeout_ms = 5000;
    zend_long recv_timeout_ms = 0;
    zend_long send_timeout_ms = 0;
    zval *ssl = nullptr;
    zval *endpoints = nullptr;
    zend_long tcp_keepalive_idle = 60;
    zend_long tcp_keepalive_interval = 5;
    zend_long tcp_keepalive_count = 3;
    zend_long max_compression_chunk_size = 65535;

    ZEND_PARSE_PARAMETERS_START(0, 20)
        Z_PARAM_OPTIONAL
        Z_PARAM_STR(host)
        Z_PARAM_LONG(port)
        Z_PARAM_STR(database)
        Z_PARAM_STR(user)
        Z_PARAM_STR(password)
        Z_PARAM_OBJECT_OF_CLASS(compression, clickhouse_ce_CompressionMethod)
        Z_PARAM_BOOL(ping_before_query)
        Z_PARAM_LONG(send_retries)
        Z_PARAM_LONG(retry_timeout_seconds)
        Z_PARAM_BOOL(tcp_keepalive)
        Z_PARAM_BOOL(tcp_nodelay)
        Z_PARAM_LONG(connect_timeout_ms)
        Z_PARAM_LONG(recv_timeout_ms)
        Z_PARAM_LONG(send_timeout_ms)
        Z_PARAM_ARRAY_EX(ssl, 1, 0) /* nullable */
        Z_PARAM_ARRAY_EX(endpoints, 1, 0) /* nullable */
        Z_PARAM_LONG(tcp_keepalive_idle)
        Z_PARAM_LONG(tcp_keepalive_interval)
        Z_PARAM_LONG(tcp_keepalive_count)
        Z_PARAM_LONG(max_compression_chunk_size)
    ZEND_PARSE_PARAMETERS_END();

    auto *intern = Z_CLICKHOUSE_OPTIONS_P(ZEND_THIS);

    CLICKHOUSE_TRY

    auto opts = std::make_unique<clickhouse::ClientOptions>();

    opts->SetHost(host ? std::string(ZSTR_VAL(host), ZSTR_LEN(host)) : "localhost");
    opts->SetPort(static_cast<uint16_t>(port));
    opts->SetDefaultDatabase(database ? std::string(ZSTR_VAL(database), ZSTR_LEN(database)) : "default");
    opts->SetUser(user ? std::string(ZSTR_VAL(user), ZSTR_LEN(user)) : "default");
    opts->SetPassword(password ? std::string(ZSTR_VAL(password), ZSTR_LEN(password)) : "");
    opts->SetPingBeforeQuery(ping_before_query);
    opts->SetSendRetries(static_cast<unsigned int>(send_retries));
    opts->SetRetryTimeout(std::chrono::seconds(retry_timeout_seconds));
    opts->TcpKeepAlive(tcp_keepalive);
    opts->TcpNoDelay(tcp_nodelay);
    opts->SetConnectionConnectTimeout(std::chrono::milliseconds(connect_timeout_ms));
    opts->SetConnectionRecvTimeout(std::chrono::milliseconds(recv_timeout_ms));
    opts->SetConnectionSendTimeout(std::chrono::milliseconds(send_timeout_ms));
    opts->SetTcpKeepAliveIdle(std::chrono::seconds(tcp_keepalive_idle));
    opts->SetTcpKeepAliveInterval(std::chrono::seconds(tcp_keepalive_interval));
    opts->SetTcpKeepAliveCount(static_cast<unsigned int>(tcp_keepalive_count));
    opts->SetMaxCompressionChunkSize(static_cast<unsigned int>(max_compression_chunk_size));

    /* Endpoints array: [['host' => 'h1', 'port' => 9000], ...] */
    if (endpoints && Z_TYPE_P(endpoints) == IS_ARRAY) {
        std::vector<clickhouse::Endpoint> eps;
        HashTable *ht = Z_ARRVAL_P(endpoints);
        zval *ep_entry;
        ZEND_HASH_FOREACH_VAL(ht, ep_entry) {
            if (Z_TYPE_P(ep_entry) != IS_ARRAY) continue;
            HashTable *ep_ht = Z_ARRVAL_P(ep_entry);
            clickhouse::Endpoint ep;
            zval *h = zend_hash_str_find(ep_ht, "host", sizeof("host") - 1);
            if (h && Z_TYPE_P(h) == IS_STRING) {
                ep.host = std::string(Z_STRVAL_P(h), Z_STRLEN_P(h));
            }
            zval *p = zend_hash_str_find(ep_ht, "port", sizeof("port") - 1);
            if (p) {
                ep.port = static_cast<uint16_t>(zval_get_long(p));
            }
            eps.push_back(std::move(ep));
        } ZEND_HASH_FOREACH_END();
        opts->SetEndpoints(std::move(eps));
    }

    /* Compression enum */
    if (compression) {
        zval *val = zend_enum_fetch_case_value(Z_OBJ_P(compression));
        if (val && Z_TYPE_P(val) == IS_LONG) {
            opts->SetCompressionMethod(static_cast<clickhouse::CompressionMethod>(Z_LVAL_P(val)));
        }
    }

    /* SSL options (array or null) */
    if (ssl && Z_TYPE_P(ssl) == IS_ARRAY) {
        clickhouse::ClientOptions::SSLOptions ssl_opts;

        HashTable *ht = Z_ARRVAL_P(ssl);
        zval *tmp;

        if ((tmp = zend_hash_str_find(ht, "skip_verification", sizeof("skip_verification") - 1)) != nullptr) {
            ssl_opts.SetSkipVerification(zend_is_true(tmp));
        }
        if ((tmp = zend_hash_str_find(ht, "use_default_ca", sizeof("use_default_ca") - 1)) != nullptr) {
            ssl_opts.SetUseDefaultCALocations(zend_is_true(tmp));
        }
        if ((tmp = zend_hash_str_find(ht, "ca_directory", sizeof("ca_directory") - 1)) != nullptr && Z_TYPE_P(tmp) == IS_STRING) {
            ssl_opts.SetPathToCADirectory(std::string(Z_STRVAL_P(tmp), Z_STRLEN_P(tmp)));
        }
        if ((tmp = zend_hash_str_find(ht, "use_sni", sizeof("use_sni") - 1)) != nullptr) {
            ssl_opts.SetUseSNI(zend_is_true(tmp));
        }

        opts->SetSSLOptions(std::move(ssl_opts));
    }

    intern->options = std::move(opts);

    CLICKHOUSE_CATCH
}

static const zend_function_entry class_ClickHouse_Driver_ClientOptions_methods[] = {
    ZEND_ME(ClickHouse_Driver_ClientOptions, __construct, arginfo_class_ClickHouse_Driver_ClientOptions___construct, ZEND_ACC_PUBLIC)
    ZEND_FE_END
};

void php_clickhouse_register_client_options(int module_number)
{
    zend_class_entry ce;

    INIT_NS_CLASS_ENTRY(ce, "ClickHouse\\Driver", "ClientOptions",
        class_ClickHouse_Driver_ClientOptions_methods);
    clickhouse_ce_ClientOptions = zend_register_internal_class(&ce);
    clickhouse_ce_ClientOptions->ce_flags |= ZEND_ACC_FINAL;
    clickhouse_ce_ClientOptions->create_object = php_clickhouse_client_options_create;
#if PHP_VERSION_ID >= 80200
    clickhouse_ce_ClientOptions->default_object_handlers = &clickhouse_client_options_handlers;
#endif

    memcpy(&clickhouse_client_options_handlers, zend_get_std_object_handlers(), sizeof(zend_object_handlers));
    clickhouse_client_options_handlers.offset = XtOffsetOf(php_clickhouse_client_options, std);
    clickhouse_client_options_handlers.free_obj = php_clickhouse_client_options_free;
    clickhouse_client_options_handlers.clone_obj = nullptr;
}

void php_clickhouse_register_enums(int module_number)
{
    /* CompressionMethod enum */
    clickhouse_ce_CompressionMethod = zend_register_internal_enum(
        "ClickHouse\\Driver\\CompressionMethod", IS_LONG, NULL);

    zval val;
    ZVAL_LONG(&val, -1); zend_enum_add_case_cstr(clickhouse_ce_CompressionMethod, "None", &val);
    ZVAL_LONG(&val,  1); zend_enum_add_case_cstr(clickhouse_ce_CompressionMethod, "LZ4", &val);
    ZVAL_LONG(&val,  2); zend_enum_add_case_cstr(clickhouse_ce_CompressionMethod, "ZSTD", &val);

    /* Type enum — maps 1:1 to clickhouse::Type::Code */
    clickhouse_ce_Type = zend_register_internal_enum(
        "ClickHouse\\Driver\\Type", IS_LONG, NULL);

    ZVAL_LONG(&val,  0); zend_enum_add_case_cstr(clickhouse_ce_Type, "Void", &val);
    ZVAL_LONG(&val,  1); zend_enum_add_case_cstr(clickhouse_ce_Type, "Int8", &val);
    ZVAL_LONG(&val,  2); zend_enum_add_case_cstr(clickhouse_ce_Type, "Int16", &val);
    ZVAL_LONG(&val,  3); zend_enum_add_case_cstr(clickhouse_ce_Type, "Int32", &val);
    ZVAL_LONG(&val,  4); zend_enum_add_case_cstr(clickhouse_ce_Type, "Int64", &val);
    ZVAL_LONG(&val,  5); zend_enum_add_case_cstr(clickhouse_ce_Type, "UInt8", &val);
    ZVAL_LONG(&val,  6); zend_enum_add_case_cstr(clickhouse_ce_Type, "UInt16", &val);
    ZVAL_LONG(&val,  7); zend_enum_add_case_cstr(clickhouse_ce_Type, "UInt32", &val);
    ZVAL_LONG(&val,  8); zend_enum_add_case_cstr(clickhouse_ce_Type, "UInt64", &val);
    ZVAL_LONG(&val,  9); zend_enum_add_case_cstr(clickhouse_ce_Type, "Float32", &val);
    ZVAL_LONG(&val, 10); zend_enum_add_case_cstr(clickhouse_ce_Type, "Float64", &val);
    ZVAL_LONG(&val, 11); zend_enum_add_case_cstr(clickhouse_ce_Type, "String", &val);
    ZVAL_LONG(&val, 12); zend_enum_add_case_cstr(clickhouse_ce_Type, "FixedString", &val);
    ZVAL_LONG(&val, 13); zend_enum_add_case_cstr(clickhouse_ce_Type, "DateTime", &val);
    ZVAL_LONG(&val, 14); zend_enum_add_case_cstr(clickhouse_ce_Type, "Date", &val);
    ZVAL_LONG(&val, 15); zend_enum_add_case_cstr(clickhouse_ce_Type, "Array", &val);
    ZVAL_LONG(&val, 16); zend_enum_add_case_cstr(clickhouse_ce_Type, "Nullable", &val);
    ZVAL_LONG(&val, 17); zend_enum_add_case_cstr(clickhouse_ce_Type, "Tuple", &val);
    ZVAL_LONG(&val, 18); zend_enum_add_case_cstr(clickhouse_ce_Type, "Enum8", &val);
    ZVAL_LONG(&val, 19); zend_enum_add_case_cstr(clickhouse_ce_Type, "Enum16", &val);
    ZVAL_LONG(&val, 20); zend_enum_add_case_cstr(clickhouse_ce_Type, "UUID", &val);
    ZVAL_LONG(&val, 21); zend_enum_add_case_cstr(clickhouse_ce_Type, "IPv4", &val);
    ZVAL_LONG(&val, 22); zend_enum_add_case_cstr(clickhouse_ce_Type, "IPv6", &val);
    ZVAL_LONG(&val, 23); zend_enum_add_case_cstr(clickhouse_ce_Type, "Int128", &val);
    ZVAL_LONG(&val, 24); zend_enum_add_case_cstr(clickhouse_ce_Type, "UInt128", &val);
    ZVAL_LONG(&val, 25); zend_enum_add_case_cstr(clickhouse_ce_Type, "Decimal", &val);
    ZVAL_LONG(&val, 26); zend_enum_add_case_cstr(clickhouse_ce_Type, "Decimal32", &val);
    ZVAL_LONG(&val, 27); zend_enum_add_case_cstr(clickhouse_ce_Type, "Decimal64", &val);
    ZVAL_LONG(&val, 28); zend_enum_add_case_cstr(clickhouse_ce_Type, "Decimal128", &val);
    ZVAL_LONG(&val, 29); zend_enum_add_case_cstr(clickhouse_ce_Type, "LowCardinality", &val);
    ZVAL_LONG(&val, 30); zend_enum_add_case_cstr(clickhouse_ce_Type, "DateTime64", &val);
    ZVAL_LONG(&val, 31); zend_enum_add_case_cstr(clickhouse_ce_Type, "Date32", &val);
    ZVAL_LONG(&val, 32); zend_enum_add_case_cstr(clickhouse_ce_Type, "Map", &val);
    ZVAL_LONG(&val, 33); zend_enum_add_case_cstr(clickhouse_ce_Type, "Point", &val);
    ZVAL_LONG(&val, 34); zend_enum_add_case_cstr(clickhouse_ce_Type, "Ring", &val);
    ZVAL_LONG(&val, 35); zend_enum_add_case_cstr(clickhouse_ce_Type, "Polygon", &val);
    ZVAL_LONG(&val, 36); zend_enum_add_case_cstr(clickhouse_ce_Type, "MultiPolygon", &val);
}

void php_clickhouse_register_server_info(int module_number)
{
    zend_class_entry ce;

    INIT_NS_CLASS_ENTRY(ce, "ClickHouse\\Driver", "ServerInfo", NULL);
    clickhouse_ce_ServerInfo = zend_register_internal_class(&ce);
#if PHP_VERSION_ID >= 80200
    clickhouse_ce_ServerInfo->ce_flags |= ZEND_ACC_READONLY_CLASS;
#endif

    zend_declare_property_string(clickhouse_ce_ServerInfo, "name", sizeof("name") - 1, "", ZEND_ACC_PUBLIC | ZEND_ACC_READONLY);
    zend_declare_property_string(clickhouse_ce_ServerInfo, "timezone", sizeof("timezone") - 1, "", ZEND_ACC_PUBLIC | ZEND_ACC_READONLY);
    zend_declare_property_string(clickhouse_ce_ServerInfo, "displayName", sizeof("displayName") - 1, "", ZEND_ACC_PUBLIC | ZEND_ACC_READONLY);
    zend_declare_property_long(clickhouse_ce_ServerInfo, "versionMajor", sizeof("versionMajor") - 1, 0, ZEND_ACC_PUBLIC | ZEND_ACC_READONLY);
    zend_declare_property_long(clickhouse_ce_ServerInfo, "versionMinor", sizeof("versionMinor") - 1, 0, ZEND_ACC_PUBLIC | ZEND_ACC_READONLY);
    zend_declare_property_long(clickhouse_ce_ServerInfo, "versionPatch", sizeof("versionPatch") - 1, 0, ZEND_ACC_PUBLIC | ZEND_ACC_READONLY);
    zend_declare_property_long(clickhouse_ce_ServerInfo, "revision", sizeof("revision") - 1, 0, ZEND_ACC_PUBLIC | ZEND_ACC_READONLY);
}
