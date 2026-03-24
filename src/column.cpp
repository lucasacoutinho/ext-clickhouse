#include "src/column.h"
#include "src/column_convert.h"
#include "src/column_write.h"
#include "src/common.h"
#include "clickhouse_arginfo.h"
#include "clickhouse/columns/factory.h"

zend_class_entry *clickhouse_ce_Column = nullptr;
static zend_object_handlers clickhouse_column_handlers;

static zend_object *php_clickhouse_column_create(zend_class_entry *ce)
{
    auto *intern = static_cast<php_clickhouse_column *>(
        zend_object_alloc(sizeof(php_clickhouse_column), ce));

    new (&intern->column) clickhouse::ColumnRef();

    zend_object_std_init(&intern->std, ce);
    object_properties_init(&intern->std, ce);

    return &intern->std;
}

static void php_clickhouse_column_free(zend_object *object)
{
    auto *intern = php_clickhouse_column_from_obj(object);
    intern->column.~shared_ptr();
    zend_object_std_dtor(object);
}

ZEND_METHOD(ClickHouse_Driver_Column, create)
{
    zend_string *type_name = nullptr;
    zval *values = nullptr;

    ZEND_PARSE_PARAMETERS_START(2, 2)
        Z_PARAM_STR(type_name)
        Z_PARAM_ARRAY(values)
    ZEND_PARSE_PARAMETERS_END();

    CLICKHOUSE_TRY
        std::string cpp_type_name(ZSTR_VAL(type_name), ZSTR_LEN(type_name));
        clickhouse::ColumnRef col = clickhouse::CreateColumnByType(cpp_type_name);

        if (!col) {
            zend_throw_exception_ex(clickhouse_ce_ValidationException, 0,
                "Unknown ClickHouse type: %s", ZSTR_VAL(type_name));
            return;
        }

        /* Populate from PHP array */
        HashTable *ht = Z_ARRVAL_P(values);
        zval *entry;
        ZEND_HASH_FOREACH_VAL(ht, entry) {
            php_clickhouse_zval_to_column(col, entry);
            if (EG(exception)) return;
        } ZEND_HASH_FOREACH_END();

        php_clickhouse_create_column_from_ref(return_value, col);
    CLICKHOUSE_CATCH_RETURN
}

ZEND_METHOD(ClickHouse_Driver_Column, getTypeName)
{
    ZEND_PARSE_PARAMETERS_NONE();

    auto *intern = Z_CLICKHOUSE_COLUMN_P(ZEND_THIS);
    if (!intern->column) {
        RETURN_EMPTY_STRING();
    }

    std::string name = intern->column->Type()->GetName();
    RETURN_STRINGL(name.c_str(), name.size());
}

ZEND_METHOD(ClickHouse_Driver_Column, getType)
{
    ZEND_PARSE_PARAMETERS_NONE();

    auto *intern = Z_CLICKHOUSE_COLUMN_P(ZEND_THIS);
    if (!intern->column) {
        RETURN_NULL();
    }

    zend_long code = static_cast<zend_long>(intern->column->Type()->GetCode());

    zend_object *enum_obj = nullptr;
#if PHP_VERSION_ID >= 80200
    if (zend_enum_get_case_by_value(&enum_obj, clickhouse_ce_Type, code, nullptr, false) == SUCCESS && enum_obj) {
#else
    if (php_clickhouse_enum_get_case(&enum_obj, clickhouse_ce_Type, code) == SUCCESS && enum_obj) {
#endif
        RETURN_OBJ_COPY(enum_obj);
    }
    RETURN_NULL();
}

ZEND_METHOD(ClickHouse_Driver_Column, size)
{
    ZEND_PARSE_PARAMETERS_NONE();

    auto *intern = Z_CLICKHOUSE_COLUMN_P(ZEND_THIS);
    if (!intern->column) {
        RETURN_LONG(0);
    }
    RETURN_LONG(static_cast<zend_long>(intern->column->Size()));
}

ZEND_METHOD(ClickHouse_Driver_Column, at)
{
    zend_long index = 0;

    ZEND_PARSE_PARAMETERS_START(1, 1)
        Z_PARAM_LONG(index)
    ZEND_PARSE_PARAMETERS_END();

    auto *intern = Z_CLICKHOUSE_COLUMN_P(ZEND_THIS);
    if (!intern->column || index < 0 || static_cast<size_t>(index) >= intern->column->Size()) {
        zend_throw_exception(clickhouse_ce_ValidationException, "Index out of range", 0);
        return;
    }

    php_clickhouse_column_to_zval(intern->column, static_cast<size_t>(index), return_value);
}

ZEND_METHOD(ClickHouse_Driver_Column, toArray)
{
    ZEND_PARSE_PARAMETERS_NONE();

    auto *intern = Z_CLICKHOUSE_COLUMN_P(ZEND_THIS);
    if (!intern->column) {
        array_init(return_value);
        return;
    }

    size_t n = intern->column->Size();
    array_init_size(return_value, n);

    for (size_t i = 0; i < n; ++i) {
        zval val;
        php_clickhouse_column_to_zval(intern->column, i, &val);
        add_next_index_zval(return_value, &val);
    }
}

void php_clickhouse_create_column_from_ref(zval *return_value, clickhouse::ColumnRef col_ref)
{
    object_init_ex(return_value, clickhouse_ce_Column);
    auto *intern = Z_CLICKHOUSE_COLUMN_P(return_value);
    intern->column = std::move(col_ref);
}

static const zend_function_entry class_ClickHouse_Driver_Column_methods[] = {
    ZEND_ME(ClickHouse_Driver_Column, create, arginfo_class_ClickHouse_Driver_Column_create, ZEND_ACC_PUBLIC | ZEND_ACC_STATIC)
    ZEND_ME(ClickHouse_Driver_Column, getTypeName, arginfo_class_ClickHouse_Driver_Column_getTypeName, ZEND_ACC_PUBLIC)
    ZEND_ME(ClickHouse_Driver_Column, getType, arginfo_class_ClickHouse_Driver_Column_getType, ZEND_ACC_PUBLIC)
    ZEND_ME(ClickHouse_Driver_Column, size, arginfo_class_ClickHouse_Driver_Column_size, ZEND_ACC_PUBLIC)
    ZEND_ME(ClickHouse_Driver_Column, at, arginfo_class_ClickHouse_Driver_Column_at, ZEND_ACC_PUBLIC)
    ZEND_ME(ClickHouse_Driver_Column, toArray, arginfo_class_ClickHouse_Driver_Column_toArray, ZEND_ACC_PUBLIC)
    ZEND_FE_END
};

void php_clickhouse_register_column(int module_number)
{
    zend_class_entry ce;

    INIT_NS_CLASS_ENTRY(ce, "ClickHouse\\Driver", "Column",
        class_ClickHouse_Driver_Column_methods);
    clickhouse_ce_Column = zend_register_internal_class(&ce);
    clickhouse_ce_Column->ce_flags |= ZEND_ACC_FINAL;
    clickhouse_ce_Column->create_object = php_clickhouse_column_create;
#if PHP_VERSION_ID >= 80200
    clickhouse_ce_Column->default_object_handlers = &clickhouse_column_handlers;
#endif

    memcpy(&clickhouse_column_handlers, zend_get_std_object_handlers(), sizeof(zend_object_handlers));
    clickhouse_column_handlers.offset = XtOffsetOf(php_clickhouse_column, std);
    clickhouse_column_handlers.free_obj = php_clickhouse_column_free;
    clickhouse_column_handlers.clone_obj = nullptr;
}
