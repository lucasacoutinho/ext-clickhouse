#include "src/client.h"
#include "src/client_options.h"
#include "src/block.h"
#include "src/column.h"
#include "src/column_convert.h"
#include "src/common.h"
#include "clickhouse_arginfo.h"
#include "clickhouse/query.h"

zend_class_entry *clickhouse_ce_Client = nullptr;
static zend_object_handlers clickhouse_client_handlers;

static zend_object *php_clickhouse_client_create(zend_class_entry *ce)
{
    auto *intern = static_cast<php_clickhouse_client *>(
        zend_object_alloc(sizeof(php_clickhouse_client), ce));

    new (&intern->client) std::unique_ptr<clickhouse::Client>();

    zend_object_std_init(&intern->std, ce);
    object_properties_init(&intern->std, ce);
    intern->std.handlers = &clickhouse_client_handlers;

    return &intern->std;
}

static void php_clickhouse_client_free(zend_object *object)
{
    auto *intern = php_clickhouse_client_from_obj(object);
    intern->client.~unique_ptr();
    zend_object_std_dtor(object);
}

ZEND_METHOD(ClickHouse_Driver_Client, __construct)
{
    zval *options_zv = nullptr;

    ZEND_PARSE_PARAMETERS_START(1, 1)
        Z_PARAM_OBJECT_OF_CLASS(options_zv, clickhouse_ce_ClientOptions)
    ZEND_PARSE_PARAMETERS_END();

    auto *opts_intern = Z_CLICKHOUSE_OPTIONS_P(options_zv);
    if (!opts_intern->options) {
        zend_throw_exception(clickhouse_ce_ValidationException,
            "ClientOptions not properly initialized", 0);
        return;
    }

    auto *intern = Z_CLICKHOUSE_CLIENT_P(ZEND_THIS);

    CLICKHOUSE_TRY
        intern->client = std::make_unique<clickhouse::Client>(*opts_intern->options);
    CLICKHOUSE_CATCH
}

static void apply_query_options(clickhouse::Query &q, zval *params, zval *settings)
{
    /* params: ['name' => 'value', ...] → QueryParams */
    if (params && Z_TYPE_P(params) == IS_ARRAY) {
        HashTable *ht = Z_ARRVAL_P(params);
        zend_string *key;
        zval *val;
        ZEND_HASH_FOREACH_STR_KEY_VAL(ht, key, val) {
            if (!key) continue;
            std::string param_name(ZSTR_VAL(key), ZSTR_LEN(key));
            if (Z_TYPE_P(val) == IS_NULL) {
                q.SetParam(param_name, std::nullopt);
            } else {
                zend_string *str = zval_get_string(val);
                q.SetParam(param_name, std::string(ZSTR_VAL(str), ZSTR_LEN(str)));
                zend_string_release(str);
            }
        } ZEND_HASH_FOREACH_END();
    }

    /* settings: ['key' => 'value', ...] → QuerySettings */
    if (settings && Z_TYPE_P(settings) == IS_ARRAY) {
        HashTable *ht = Z_ARRVAL_P(settings);
        zend_string *key;
        zval *val;
        ZEND_HASH_FOREACH_STR_KEY_VAL(ht, key, val) {
            if (!key) continue;
            zend_string *str = zval_get_string(val);
            clickhouse::QuerySettingsField field;
            field.value = std::string(ZSTR_VAL(str), ZSTR_LEN(str));
            field.flags = clickhouse::QuerySettingsField::IMPORTANT;
            q.SetSetting(
                std::string(ZSTR_VAL(key), ZSTR_LEN(key)),
                field);
            zend_string_release(str);
        } ZEND_HASH_FOREACH_END();
    }
}

/* Build a Query object with optional query_id, params, and settings */
static clickhouse::Query build_query(zend_string *query_str, zval *params, zval *settings, zend_string *query_id)
{
    std::string sql(ZSTR_VAL(query_str), ZSTR_LEN(query_str));
    std::string qid = query_id ? std::string(ZSTR_VAL(query_id), ZSTR_LEN(query_id)) : std::string();

    clickhouse::Query q(sql, qid);
    apply_query_options(q, params, settings);
    return q;
}

ZEND_METHOD(ClickHouse_Driver_Client, execute)
{
    zend_string *query = nullptr;
    zval *params = nullptr;
    zval *settings = nullptr;
    zend_string *query_id = nullptr;

    ZEND_PARSE_PARAMETERS_START(1, 4)
        Z_PARAM_STR(query)
        Z_PARAM_OPTIONAL
        Z_PARAM_ARRAY_EX(params, 1, 0)
        Z_PARAM_ARRAY_EX(settings, 1, 0)
        Z_PARAM_STR_OR_NULL(query_id)
    ZEND_PARSE_PARAMETERS_END();

    auto *intern = Z_CLICKHOUSE_CLIENT_P(ZEND_THIS);
    if (!intern->client) {
        zend_throw_exception(clickhouse_ce_ClickHouseException, "Client not connected", 0);
        return;
    }

    CLICKHOUSE_TRY
        auto q = build_query(query, params, settings, query_id);
        intern->client->Execute(q);
    CLICKHOUSE_CATCH
}

ZEND_METHOD(ClickHouse_Driver_Client, select)
{
    zend_string *query = nullptr;
    zval *params = nullptr;
    zval *settings = nullptr;
    zend_string *query_id = nullptr;

    ZEND_PARSE_PARAMETERS_START(1, 4)
        Z_PARAM_STR(query)
        Z_PARAM_OPTIONAL
        Z_PARAM_ARRAY_EX(params, 1, 0)
        Z_PARAM_ARRAY_EX(settings, 1, 0)
        Z_PARAM_STR_OR_NULL(query_id)
    ZEND_PARSE_PARAMETERS_END();

    auto *intern = Z_CLICKHOUSE_CLIENT_P(ZEND_THIS);
    if (!intern->client) {
        zend_throw_exception(clickhouse_ce_ClickHouseException, "Client not connected", 0);
        return;
    }

    array_init(return_value);

    CLICKHOUSE_TRY
        auto q = build_query(query, params, settings, query_id);
        q.OnData([&](const clickhouse::Block &block) {
            size_t rows = block.GetRowCount();
            size_t cols = block.GetColumnCount();

            if (rows == 0) return;

            for (size_t r = 0; r < rows; ++r) {
                zval row;
                array_init_size(&row, cols);

                for (size_t c = 0; c < cols; ++c) {
                    zval val;
                    php_clickhouse_column_to_zval(block[c], r, &val);
                    add_assoc_zval_ex(&row,
                        block.GetColumnName(c).c_str(),
                        block.GetColumnName(c).size(),
                        &val);
                }

                add_next_index_zval(return_value, &row);
            }
        });
        intern->client->Execute(q);
    CLICKHOUSE_CATCH_RETURN
}

ZEND_METHOD(ClickHouse_Driver_Client, selectByBlock)
{
    zend_string *query = nullptr;
    zend_fcall_info fci;
    zend_fcall_info_cache fcc;
    zval *params = nullptr;
    zval *settings = nullptr;
    zend_string *query_id = nullptr;
    zend_fcall_info fci_progress = empty_fcall_info;
    zend_fcall_info_cache fcc_progress = empty_fcall_info_cache;
    zend_fcall_info fci_profile = empty_fcall_info;
    zend_fcall_info_cache fcc_profile = empty_fcall_info_cache;

    ZEND_PARSE_PARAMETERS_START(2, 7)
        Z_PARAM_STR(query)
        Z_PARAM_FUNC(fci, fcc)
        Z_PARAM_OPTIONAL
        Z_PARAM_ARRAY_EX(params, 1, 0)
        Z_PARAM_ARRAY_EX(settings, 1, 0)
        Z_PARAM_STR_OR_NULL(query_id)
        Z_PARAM_FUNC_OR_NULL(fci_progress, fcc_progress)
        Z_PARAM_FUNC_OR_NULL(fci_profile, fcc_profile)
    ZEND_PARSE_PARAMETERS_END();

    auto *intern = Z_CLICKHOUSE_CLIENT_P(ZEND_THIS);
    if (!intern->client) {
        zend_throw_exception(clickhouse_ce_ClickHouseException, "Client not connected", 0);
        return;
    }

    CLICKHOUSE_TRY
        auto q = build_query(query, params, settings, query_id);

        /* Data callback (cancelable) */
        q.OnDataCancelable(
            [&](const clickhouse::Block &block) -> bool {
                if (block.GetRowCount() == 0) return true;

                zval block_zv;
                php_clickhouse_create_block_from_cpp(&block_zv, block);

                zval retval;
                fci.param_count = 1;
                fci.params = &block_zv;
                fci.retval = &retval;

                if (zend_call_function(&fci, &fcc) == SUCCESS) {
                    bool should_continue = true;
                    if (EG(exception) || Z_TYPE(retval) == IS_FALSE) {
                        should_continue = false;
                    }
                    zval_ptr_dtor(&retval);
                    zval_ptr_dtor(&block_zv);
                    return should_continue;
                }

                zval_ptr_dtor(&block_zv);
                return false;
            });

        /* Progress callback */
        if (ZEND_FCI_INITIALIZED(fci_progress)) {
            q.OnProgress([&](const clickhouse::Progress &progress) {
                zval arg;
                array_init_size(&arg, 5);
                add_assoc_long(&arg, "rows", static_cast<zend_long>(progress.rows));
                add_assoc_long(&arg, "bytes", static_cast<zend_long>(progress.bytes));
                add_assoc_long(&arg, "total_rows", static_cast<zend_long>(progress.total_rows));
                add_assoc_long(&arg, "written_rows", static_cast<zend_long>(progress.written_rows));
                add_assoc_long(&arg, "written_bytes", static_cast<zend_long>(progress.written_bytes));

                zval retval;
                fci_progress.param_count = 1;
                fci_progress.params = &arg;
                fci_progress.retval = &retval;
                zend_call_function(&fci_progress, &fcc_progress);
                zval_ptr_dtor(&retval);
                zval_ptr_dtor(&arg);
            });
        }

        /* Profile callback */
        if (ZEND_FCI_INITIALIZED(fci_profile)) {
            q.OnProfile([&](const clickhouse::Profile &profile) {
                zval arg;
                array_init_size(&arg, 6);
                add_assoc_long(&arg, "rows", static_cast<zend_long>(profile.rows));
                add_assoc_long(&arg, "blocks", static_cast<zend_long>(profile.blocks));
                add_assoc_long(&arg, "bytes", static_cast<zend_long>(profile.bytes));
                add_assoc_long(&arg, "rows_before_limit", static_cast<zend_long>(profile.rows_before_limit));
                add_assoc_bool(&arg, "applied_limit", profile.applied_limit);
                add_assoc_bool(&arg, "calculated_rows_before_limit", profile.calculated_rows_before_limit);

                zval retval;
                fci_profile.param_count = 1;
                fci_profile.params = &arg;
                fci_profile.retval = &retval;
                zend_call_function(&fci_profile, &fcc_profile);
                zval_ptr_dtor(&retval);
                zval_ptr_dtor(&arg);
            });
        }

        intern->client->Execute(q);
    CLICKHOUSE_CATCH
}

ZEND_METHOD(ClickHouse_Driver_Client, selectWithExternalData)
{
    zend_string *query = nullptr;
    zval *ext_tables = nullptr;
    zval *params = nullptr;
    zval *settings = nullptr;
    zend_string *query_id = nullptr;

    ZEND_PARSE_PARAMETERS_START(2, 5)
        Z_PARAM_STR(query)
        Z_PARAM_ARRAY(ext_tables)
        Z_PARAM_OPTIONAL
        Z_PARAM_ARRAY_EX(params, 1, 0)
        Z_PARAM_ARRAY_EX(settings, 1, 0)
        Z_PARAM_STR_OR_NULL(query_id)
    ZEND_PARSE_PARAMETERS_END();

    auto *intern = Z_CLICKHOUSE_CLIENT_P(ZEND_THIS);
    if (!intern->client) {
        zend_throw_exception(clickhouse_ce_ClickHouseException, "Client not connected", 0);
        return;
    }

    array_init(return_value);

    CLICKHOUSE_TRY
        /* Build external tables vector from PHP array */
        clickhouse::ExternalTables tables;
        HashTable *ht = Z_ARRVAL_P(ext_tables);
        zval *entry;
        ZEND_HASH_FOREACH_VAL(ht, entry) {
            if (Z_TYPE_P(entry) != IS_ARRAY) continue;
            HashTable *t = Z_ARRVAL_P(entry);

            zval *name_zv = zend_hash_str_find(t, "name", sizeof("name") - 1);
            zval *data_zv = zend_hash_str_find(t, "data", sizeof("data") - 1);
            if (!name_zv || Z_TYPE_P(name_zv) != IS_STRING || !data_zv ||
                Z_TYPE_P(data_zv) != IS_OBJECT ||
                !instanceof_function(Z_OBJCE_P(data_zv), clickhouse_ce_Block)) {
                continue;
            }

            auto *block_intern = php_clickhouse_block_from_zval(data_zv);
            if (!block_intern->block) continue;

            tables.push_back(clickhouse::ExternalTable{
                std::string_view(Z_STRVAL_P(name_zv), Z_STRLEN_P(name_zv)),
                *block_intern->block
            });
        } ZEND_HASH_FOREACH_END();

        std::string sql(ZSTR_VAL(query), ZSTR_LEN(query));
        std::string qid = query_id ? std::string(ZSTR_VAL(query_id), ZSTR_LEN(query_id)) : std::string();

        intern->client->SelectWithExternalData(sql, qid, tables,
            [&](const clickhouse::Block &block) {
                size_t rows = block.GetRowCount();
                size_t cols = block.GetColumnCount();
                if (rows == 0) return;

                for (size_t r = 0; r < rows; ++r) {
                    zval row;
                    array_init_size(&row, cols);
                    for (size_t c = 0; c < cols; ++c) {
                        zval val;
                        php_clickhouse_column_to_zval(block[c], r, &val);
                        add_assoc_zval_ex(&row,
                            block.GetColumnName(c).c_str(),
                            block.GetColumnName(c).size(), &val);
                    }
                    add_next_index_zval(return_value, &row);
                }
            });
    CLICKHOUSE_CATCH_RETURN
}

ZEND_METHOD(ClickHouse_Driver_Client, insert)
{
    zend_string *table_name = nullptr;
    zval *block_zv = nullptr;
    zend_string *query_id = nullptr;

    ZEND_PARSE_PARAMETERS_START(2, 3)
        Z_PARAM_STR(table_name)
        Z_PARAM_OBJECT_OF_CLASS(block_zv, clickhouse_ce_Block)
        Z_PARAM_OPTIONAL
        Z_PARAM_STR_OR_NULL(query_id)
    ZEND_PARSE_PARAMETERS_END();

    auto *intern = Z_CLICKHOUSE_CLIENT_P(ZEND_THIS);
    if (!intern->client) {
        zend_throw_exception(clickhouse_ce_ClickHouseException, "Client not connected", 0);
        return;
    }

    auto *block_intern = php_clickhouse_block_from_zval(block_zv);
    if (!block_intern->block) {
        zend_throw_exception(clickhouse_ce_ValidationException, "Block not initialized", 0);
        return;
    }

    CLICKHOUSE_TRY
        std::string tbl(ZSTR_VAL(table_name), ZSTR_LEN(table_name));
        if (query_id) {
            std::string qid(ZSTR_VAL(query_id), ZSTR_LEN(query_id));
            intern->client->Insert(tbl, qid, *block_intern->block);
        } else {
            intern->client->Insert(tbl, *block_intern->block);
        }
    CLICKHOUSE_CATCH
}

ZEND_METHOD(ClickHouse_Driver_Client, ping)
{
    ZEND_PARSE_PARAMETERS_NONE();

    auto *intern = Z_CLICKHOUSE_CLIENT_P(ZEND_THIS);
    if (!intern->client) {
        zend_throw_exception(clickhouse_ce_ClickHouseException, "Client not connected", 0);
        return;
    }

    CLICKHOUSE_TRY
        intern->client->Ping();
    CLICKHOUSE_CATCH
}

ZEND_METHOD(ClickHouse_Driver_Client, resetConnection)
{
    ZEND_PARSE_PARAMETERS_NONE();

    auto *intern = Z_CLICKHOUSE_CLIENT_P(ZEND_THIS);
    if (!intern->client) {
        zend_throw_exception(clickhouse_ce_ClickHouseException, "Client not connected", 0);
        return;
    }

    CLICKHOUSE_TRY
        intern->client->ResetConnection();
    CLICKHOUSE_CATCH
}

ZEND_METHOD(ClickHouse_Driver_Client, resetConnectionEndpoint)
{
    ZEND_PARSE_PARAMETERS_NONE();

    auto *intern = Z_CLICKHOUSE_CLIENT_P(ZEND_THIS);
    if (!intern->client) {
        zend_throw_exception(clickhouse_ce_ClickHouseException, "Client not connected", 0);
        return;
    }

    CLICKHOUSE_TRY
        intern->client->ResetConnectionEndpoint();
    CLICKHOUSE_CATCH
}

ZEND_METHOD(ClickHouse_Driver_Client, getCurrentEndpoint)
{
    ZEND_PARSE_PARAMETERS_NONE();

    auto *intern = Z_CLICKHOUSE_CLIENT_P(ZEND_THIS);
    if (!intern->client) {
        RETURN_NULL();
    }

    const auto &ep = intern->client->GetCurrentEndpoint();
    if (!ep.has_value()) {
        RETURN_NULL();
    }

    array_init(return_value);
    add_assoc_stringl(return_value, "host", ep->host.c_str(), ep->host.size());
    add_assoc_long(return_value, "port", static_cast<zend_long>(ep->port));
}

ZEND_METHOD(ClickHouse_Driver_Client, getServerInfo)
{
    ZEND_PARSE_PARAMETERS_NONE();

    auto *intern = Z_CLICKHOUSE_CLIENT_P(ZEND_THIS);
    if (!intern->client) {
        zend_throw_exception(clickhouse_ce_ClickHouseException, "Client not connected", 0);
        return;
    }

    CLICKHOUSE_TRY
        const auto &info = intern->client->GetServerInfo();

        object_init_ex(return_value, clickhouse_ce_ServerInfo);

        zend_update_property_stringl(clickhouse_ce_ServerInfo, Z_OBJ_P(return_value),
            "name", sizeof("name") - 1,
            info.name.c_str(), info.name.size());
        zend_update_property_stringl(clickhouse_ce_ServerInfo, Z_OBJ_P(return_value),
            "timezone", sizeof("timezone") - 1,
            info.timezone.c_str(), info.timezone.size());
        zend_update_property_stringl(clickhouse_ce_ServerInfo, Z_OBJ_P(return_value),
            "displayName", sizeof("displayName") - 1,
            info.display_name.c_str(), info.display_name.size());
        zend_update_property_long(clickhouse_ce_ServerInfo, Z_OBJ_P(return_value),
            "versionMajor", sizeof("versionMajor") - 1,
            static_cast<zend_long>(info.version_major));
        zend_update_property_long(clickhouse_ce_ServerInfo, Z_OBJ_P(return_value),
            "versionMinor", sizeof("versionMinor") - 1,
            static_cast<zend_long>(info.version_minor));
        zend_update_property_long(clickhouse_ce_ServerInfo, Z_OBJ_P(return_value),
            "versionPatch", sizeof("versionPatch") - 1,
            static_cast<zend_long>(info.version_patch));
        zend_update_property_long(clickhouse_ce_ServerInfo, Z_OBJ_P(return_value),
            "revision", sizeof("revision") - 1,
            static_cast<zend_long>(info.revision));
    CLICKHOUSE_CATCH_RETURN
}

static const zend_function_entry class_ClickHouse_Driver_Client_methods[] = {
    ZEND_ME(ClickHouse_Driver_Client, __construct, arginfo_class_ClickHouse_Driver_Client___construct, ZEND_ACC_PUBLIC)
    ZEND_ME(ClickHouse_Driver_Client, execute, arginfo_class_ClickHouse_Driver_Client_execute, ZEND_ACC_PUBLIC)
    ZEND_ME(ClickHouse_Driver_Client, select, arginfo_class_ClickHouse_Driver_Client_select, ZEND_ACC_PUBLIC)
    ZEND_ME(ClickHouse_Driver_Client, selectByBlock, arginfo_class_ClickHouse_Driver_Client_selectByBlock, ZEND_ACC_PUBLIC)
    ZEND_ME(ClickHouse_Driver_Client, insert, arginfo_class_ClickHouse_Driver_Client_insert, ZEND_ACC_PUBLIC)
    ZEND_ME(ClickHouse_Driver_Client, selectWithExternalData, arginfo_class_ClickHouse_Driver_Client_selectWithExternalData, ZEND_ACC_PUBLIC)
    ZEND_ME(ClickHouse_Driver_Client, ping, arginfo_class_ClickHouse_Driver_Client_ping, ZEND_ACC_PUBLIC)
    ZEND_ME(ClickHouse_Driver_Client, resetConnection, arginfo_class_ClickHouse_Driver_Client_resetConnection, ZEND_ACC_PUBLIC)
    ZEND_ME(ClickHouse_Driver_Client, resetConnectionEndpoint, arginfo_class_ClickHouse_Driver_Client_resetConnectionEndpoint, ZEND_ACC_PUBLIC)
    ZEND_ME(ClickHouse_Driver_Client, getCurrentEndpoint, arginfo_class_ClickHouse_Driver_Client_getCurrentEndpoint, ZEND_ACC_PUBLIC)
    ZEND_ME(ClickHouse_Driver_Client, getServerInfo, arginfo_class_ClickHouse_Driver_Client_getServerInfo, ZEND_ACC_PUBLIC)
    ZEND_FE_END
};

void php_clickhouse_register_client(int module_number)
{
    zend_class_entry ce;

    INIT_NS_CLASS_ENTRY(ce, "ClickHouse\\Driver", "Client",
        class_ClickHouse_Driver_Client_methods);
    clickhouse_ce_Client = zend_register_internal_class(&ce);
    clickhouse_ce_Client->ce_flags |= ZEND_ACC_FINAL;
    clickhouse_ce_Client->create_object = php_clickhouse_client_create;
#if PHP_VERSION_ID >= 80300
    clickhouse_ce_Client->default_object_handlers = &clickhouse_client_handlers;
#endif

    memcpy(&clickhouse_client_handlers, zend_get_std_object_handlers(), sizeof(zend_object_handlers));
    clickhouse_client_handlers.offset = XtOffsetOf(php_clickhouse_client, std);
    clickhouse_client_handlers.free_obj = php_clickhouse_client_free;
    clickhouse_client_handlers.clone_obj = nullptr;
}
