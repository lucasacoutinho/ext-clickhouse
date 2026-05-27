#include "src/client_options.h"
#include "src/common.h"
#include "clickhouse_arginfo.h"

#include <chrono>
#include <cstring>
#include <string>
#include <vector>

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
    intern->std.handlers = &clickhouse_client_options_handlers;

    return &intern->std;
}

static void php_clickhouse_client_options_free(zend_object *object)
{
    auto *intern = php_clickhouse_client_options_from_obj(object);
    intern->options.~unique_ptr();
    zend_object_std_dtor(object);
}

struct php_clickhouse_enum_case
{
    const char *name;
    zend_long value;
};

static const php_clickhouse_enum_case php_clickhouse_compression_cases[] = {
    {"None", -1},
    {"LZ4", 1},
    {"ZSTD", 2},
    {nullptr, 0},
};

static const php_clickhouse_enum_case php_clickhouse_type_cases[] = {
    {"Void", 0},          {"Int8", 1},        {"Int16", 2},
    {"Int32", 3},         {"Int64", 4},       {"UInt8", 5},
    {"UInt16", 6},        {"UInt32", 7},      {"UInt64", 8},
    {"Float32", 9},       {"Float64", 10},    {"String", 11},
    {"FixedString", 12},  {"DateTime", 13},   {"Date", 14},
    {"Array", 15},        {"Nullable", 16},   {"Tuple", 17},
    {"Enum8", 18},        {"Enum16", 19},     {"UUID", 20},
    {"IPv4", 21},         {"IPv6", 22},       {"Int128", 23},
    {"UInt128", 24},      {"Decimal", 25},    {"Decimal32", 26},
    {"Decimal64", 27},    {"Decimal128", 28}, {"LowCardinality", 29},
    {"DateTime64", 30},   {"Date32", 31},     {"Map", 32},
    {"Point", 33},        {"Ring", 34},       {"Polygon", 35},
    {"MultiPolygon", 36}, {nullptr, 0},
};

static bool php_clickhouse_enum_value_exists(const php_clickhouse_enum_case *cases, zend_long value)
{
    for (const php_clickhouse_enum_case *entry = cases; entry->name; ++entry) {
        if (entry->value == value) {
            return true;
        }
    }
    return false;
}

static void php_clickhouse_declare_class_constants(zend_class_entry *ce,
                                                   const php_clickhouse_enum_case *cases)
{
    for (const php_clickhouse_enum_case *entry = cases; entry->name; ++entry) {
        zend_declare_class_constant_long(ce, entry->name, strlen(entry->name), entry->value);
    }
}

#if PHP_VERSION_ID >= 80100
static void php_clickhouse_add_enum_cases(zend_class_entry *ce,
                                          const php_clickhouse_enum_case *cases)
{
    zval val;
    for (const php_clickhouse_enum_case *entry = cases; entry->name; ++entry) {
        ZVAL_LONG(&val, entry->value);
        zend_enum_add_case_cstr(ce, entry->name, &val);
    }
}
#endif

static bool php_clickhouse_zval_to_enum_value(zval *value, zend_class_entry *ce,
                                              const php_clickhouse_enum_case *cases, zend_long *out)
{
    if (!value) {
        return false;
    }

#if PHP_VERSION_ID >= 80100
    if (Z_TYPE_P(value) == IS_OBJECT && instanceof_function(Z_OBJCE_P(value), ce)) {
        zval *enum_value = zend_enum_fetch_case_value(Z_OBJ_P(value));
        if (enum_value && Z_TYPE_P(enum_value) == IS_LONG) {
            *out = Z_LVAL_P(enum_value);
            return php_clickhouse_enum_value_exists(cases, *out);
        }
        return false;
    }
#endif

    if (Z_TYPE_P(value) == IS_LONG) {
        *out = Z_LVAL_P(value);
        return php_clickhouse_enum_value_exists(cases, *out);
    }

    return false;
}

ZEND_METHOD(ClickHouse_Driver_ClientOptions, __construct)
{
    zend_string *host = nullptr;
    zend_long port = 9000;
    zend_string *database = nullptr;
    zend_string *user = nullptr;
    zend_string *password = nullptr;
    zval *compression = nullptr;
    zend_bool ping_before_query = false;
    zend_long send_retries = 1;
    zend_long retry_timeout_seconds = 5;
    zend_bool tcp_keepalive = false;
    zend_bool tcp_nodelay = true;
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
    Z_PARAM_ZVAL(compression)
    Z_PARAM_BOOL(ping_before_query)
    Z_PARAM_LONG(send_retries)
    Z_PARAM_LONG(retry_timeout_seconds)
    Z_PARAM_BOOL(tcp_keepalive)
    Z_PARAM_BOOL(tcp_nodelay)
    Z_PARAM_LONG(connect_timeout_ms)
    Z_PARAM_LONG(recv_timeout_ms)
    Z_PARAM_LONG(send_timeout_ms)
    Z_PARAM_ARRAY_EX(ssl, 1, 0)       /* nullable */
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
    opts->SetDefaultDatabase(database ? std::string(ZSTR_VAL(database), ZSTR_LEN(database))
                                      : "default");
    opts->SetUser(user ? std::string(ZSTR_VAL(user), ZSTR_LEN(user)) : "default");
    opts->SetPassword(password ? std::string(ZSTR_VAL(password), ZSTR_LEN(password)) : "");
    opts->SetPingBeforeQuery(ping_before_query != 0);
    opts->SetSendRetries(static_cast<unsigned int>(send_retries));
    opts->SetRetryTimeout(std::chrono::seconds(retry_timeout_seconds));
    opts->TcpKeepAlive(tcp_keepalive != 0);
    opts->TcpNoDelay(tcp_nodelay != 0);
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
        ZEND_HASH_FOREACH_VAL(ht, ep_entry)
        {
            if (Z_TYPE_P(ep_entry) != IS_ARRAY)
                continue;
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
        }
        ZEND_HASH_FOREACH_END();
        opts->SetEndpoints(std::move(eps));
    }

    /* Compression enum */
    if (compression) {
        zend_long compression_value = 0;
        if (!php_clickhouse_zval_to_enum_value(compression, clickhouse_ce_CompressionMethod,
                                               php_clickhouse_compression_cases,
                                               &compression_value)) {
            zend_throw_exception(clickhouse_ce_ValidationException, "Invalid compression method",
                                 0);
            return;
        }
        opts->SetCompressionMethod(static_cast<clickhouse::CompressionMethod>(compression_value));
    }

    /* SSL options (array or null) */
    if (ssl && Z_TYPE_P(ssl) == IS_ARRAY) {
        clickhouse::ClientOptions::SSLOptions ssl_opts;
        ssl_opts.SetUseDefaultCALocations(true);
        ssl_opts.SetUseSNI(true);

        HashTable *ht = Z_ARRVAL_P(ssl);
        zval *tmp;

        if ((tmp = zend_hash_str_find(ht, "skip_verification", sizeof("skip_verification") - 1)) !=
            nullptr) {
            ssl_opts.SetSkipVerification(zend_is_true(tmp));
        }
        if ((tmp = zend_hash_str_find(ht, "use_default_ca", sizeof("use_default_ca") - 1)) !=
            nullptr) {
            ssl_opts.SetUseDefaultCALocations(zend_is_true(tmp));
        }
        if ((tmp = zend_hash_str_find(ht, "ca_directory", sizeof("ca_directory") - 1)) != nullptr &&
            Z_TYPE_P(tmp) == IS_STRING) {
            ssl_opts.SetPathToCADirectory(std::string(Z_STRVAL_P(tmp), Z_STRLEN_P(tmp)));
        }
        std::vector<std::string> ca_files;
        if ((tmp = zend_hash_str_find(ht, "ca_file", sizeof("ca_file") - 1)) != nullptr &&
            Z_TYPE_P(tmp) == IS_STRING) {
            ca_files.emplace_back(Z_STRVAL_P(tmp), Z_STRLEN_P(tmp));
        }
        if ((tmp = zend_hash_str_find(ht, "ca_files", sizeof("ca_files") - 1)) != nullptr &&
            Z_TYPE_P(tmp) == IS_ARRAY) {
            zval *entry;
            ZEND_HASH_FOREACH_VAL(Z_ARRVAL_P(tmp), entry)
            {
                if (Z_TYPE_P(entry) == IS_STRING) {
                    ca_files.emplace_back(Z_STRVAL_P(entry), Z_STRLEN_P(entry));
                }
            }
            ZEND_HASH_FOREACH_END();
        }
        if (!ca_files.empty()) {
            ssl_opts.SetPathToCAFiles(ca_files);
        }
        if ((tmp = zend_hash_str_find(ht, "use_sni", sizeof("use_sni") - 1)) != nullptr) {
            ssl_opts.SetUseSNI(zend_is_true(tmp));
        }
        std::vector<clickhouse::ClientOptions::SSLOptions::CommandAndValue> ssl_config;
        if ((tmp = zend_hash_str_find(ht, "client_cert", sizeof("client_cert") - 1)) != nullptr &&
            Z_TYPE_P(tmp) == IS_STRING) {
            ssl_config.push_back({"Certificate", std::string(Z_STRVAL_P(tmp), Z_STRLEN_P(tmp))});
        }
        if ((tmp = zend_hash_str_find(ht, "client_key", sizeof("client_key") - 1)) != nullptr &&
            Z_TYPE_P(tmp) == IS_STRING) {
            ssl_config.push_back({"PrivateKey", std::string(Z_STRVAL_P(tmp), Z_STRLEN_P(tmp))});
        }
        if (!ssl_config.empty()) {
            ssl_opts.SetConfiguration(ssl_config);
        }

        opts->SetSSLOptions(std::move(ssl_opts));
    }

    intern->options = std::move(opts);

    CLICKHOUSE_CATCH
}

static const zend_function_entry class_ClickHouse_Driver_ClientOptions_methods[] = {ZEND_ME(
    ClickHouse_Driver_ClientOptions, __construct,
    arginfo_class_ClickHouse_Driver_ClientOptions___construct, ZEND_ACC_PUBLIC) ZEND_FE_END};

void php_clickhouse_register_client_options(int module_number)
{
    zend_class_entry ce;

    INIT_NS_CLASS_ENTRY(ce, "ClickHouse\\Driver", "ClientOptions",
                        class_ClickHouse_Driver_ClientOptions_methods);
    clickhouse_ce_ClientOptions = zend_register_internal_class(&ce);
    clickhouse_ce_ClientOptions->ce_flags |= ZEND_ACC_FINAL;
    clickhouse_ce_ClientOptions->create_object = php_clickhouse_client_options_create;
#if PHP_VERSION_ID >= 80300
    clickhouse_ce_ClientOptions->default_object_handlers = &clickhouse_client_options_handlers;
#endif

    memcpy(&clickhouse_client_options_handlers, zend_get_std_object_handlers(),
           sizeof(zend_object_handlers));
    clickhouse_client_options_handlers.offset = XtOffsetOf(php_clickhouse_client_options, std);
    clickhouse_client_options_handlers.free_obj = php_clickhouse_client_options_free;
    clickhouse_client_options_handlers.clone_obj = nullptr;
}

void php_clickhouse_register_enums(int module_number)
{
#if PHP_VERSION_ID >= 80100
    clickhouse_ce_CompressionMethod =
        zend_register_internal_enum("ClickHouse\\Driver\\CompressionMethod", IS_LONG, NULL);
    php_clickhouse_add_enum_cases(clickhouse_ce_CompressionMethod,
                                  php_clickhouse_compression_cases);

    clickhouse_ce_Type = zend_register_internal_enum("ClickHouse\\Driver\\Type", IS_LONG, NULL);
    php_clickhouse_add_enum_cases(clickhouse_ce_Type, php_clickhouse_type_cases);
#else
    zend_class_entry ce;

    INIT_NS_CLASS_ENTRY(ce, "ClickHouse\\Driver", "CompressionMethod", NULL);
    clickhouse_ce_CompressionMethod = zend_register_internal_class(&ce);
    clickhouse_ce_CompressionMethod->ce_flags |= ZEND_ACC_FINAL;
    php_clickhouse_declare_class_constants(clickhouse_ce_CompressionMethod,
                                           php_clickhouse_compression_cases);

    INIT_NS_CLASS_ENTRY(ce, "ClickHouse\\Driver", "Type", NULL);
    clickhouse_ce_Type = zend_register_internal_class(&ce);
    clickhouse_ce_Type->ce_flags |= ZEND_ACC_FINAL;
    php_clickhouse_declare_class_constants(clickhouse_ce_Type, php_clickhouse_type_cases);
#endif
}

void php_clickhouse_register_server_info(int module_number)
{
    zend_class_entry ce;

    INIT_NS_CLASS_ENTRY(ce, "ClickHouse\\Driver", "ServerInfo", NULL);
    clickhouse_ce_ServerInfo = zend_register_internal_class(&ce);
#if PHP_VERSION_ID >= 80300
    clickhouse_ce_ServerInfo->ce_flags |= ZEND_ACC_READONLY_CLASS;
#endif

    zend_declare_property_string(clickhouse_ce_ServerInfo, "name", sizeof("name") - 1, "",
                                 ZEND_ACC_PUBLIC | PHP_CLICKHOUSE_ACC_READONLY);
    zend_declare_property_string(clickhouse_ce_ServerInfo, "timezone", sizeof("timezone") - 1, "",
                                 ZEND_ACC_PUBLIC | PHP_CLICKHOUSE_ACC_READONLY);
    zend_declare_property_string(clickhouse_ce_ServerInfo, "displayName", sizeof("displayName") - 1,
                                 "", ZEND_ACC_PUBLIC | PHP_CLICKHOUSE_ACC_READONLY);
    zend_declare_property_long(clickhouse_ce_ServerInfo, "versionMajor", sizeof("versionMajor") - 1,
                               0, ZEND_ACC_PUBLIC | PHP_CLICKHOUSE_ACC_READONLY);
    zend_declare_property_long(clickhouse_ce_ServerInfo, "versionMinor", sizeof("versionMinor") - 1,
                               0, ZEND_ACC_PUBLIC | PHP_CLICKHOUSE_ACC_READONLY);
    zend_declare_property_long(clickhouse_ce_ServerInfo, "versionPatch", sizeof("versionPatch") - 1,
                               0, ZEND_ACC_PUBLIC | PHP_CLICKHOUSE_ACC_READONLY);
    zend_declare_property_long(clickhouse_ce_ServerInfo, "revision", sizeof("revision") - 1, 0,
                               ZEND_ACC_PUBLIC | PHP_CLICKHOUSE_ACC_READONLY);
}
