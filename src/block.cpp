#include "src/block.h"
#include "src/column.h"
#include "src/column_convert.h"
#include "src/common.h"
#include "clickhouse_arginfo.h"

zend_class_entry *clickhouse_ce_Block = nullptr;
static zend_object_handlers clickhouse_block_handlers;

static zend_object *php_clickhouse_block_create(zend_class_entry *ce)
{
    auto *intern = static_cast<php_clickhouse_block *>(
        zend_object_alloc(sizeof(php_clickhouse_block), ce));

    new (&intern->block) std::unique_ptr<clickhouse::Block>();

    zend_object_std_init(&intern->std, ce);
    object_properties_init(&intern->std, ce);

    return &intern->std;
}

static void php_clickhouse_block_free(zend_object *object)
{
    auto *intern = php_clickhouse_block_from_obj(object);
    intern->block.~unique_ptr();
    zend_object_std_dtor(object);
}

ZEND_METHOD(ClickHouse_Driver_Block, __construct)
{
    ZEND_PARSE_PARAMETERS_NONE();

    auto *intern = Z_CLICKHOUSE_BLOCK_P(ZEND_THIS);
    intern->block = std::make_unique<clickhouse::Block>();
}

ZEND_METHOD(ClickHouse_Driver_Block, appendColumn)
{
    zend_string *name = nullptr;
    zval *col_zv = nullptr;

    ZEND_PARSE_PARAMETERS_START(2, 2)
        Z_PARAM_STR(name)
        Z_PARAM_OBJECT_OF_CLASS(col_zv, clickhouse_ce_Column)
    ZEND_PARSE_PARAMETERS_END();

    auto *intern = Z_CLICKHOUSE_BLOCK_P(ZEND_THIS);
    if (!intern->block) {
        zend_throw_exception(clickhouse_ce_ValidationException, "Block not initialized", 0);
        return;
    }

    auto *col_intern = php_clickhouse_column_from_zval(col_zv);
    if (!col_intern->column) {
        zend_throw_exception(clickhouse_ce_ValidationException, "Column not initialized", 0);
        return;
    }

    CLICKHOUSE_TRY
        intern->block->AppendColumn(
            std::string(ZSTR_VAL(name), ZSTR_LEN(name)),
            col_intern->column);
    CLICKHOUSE_CATCH
}

ZEND_METHOD(ClickHouse_Driver_Block, getColumnCount)
{
    ZEND_PARSE_PARAMETERS_NONE();

    auto *intern = Z_CLICKHOUSE_BLOCK_P(ZEND_THIS);
    if (!intern->block) {
        RETURN_LONG(0);
    }
    RETURN_LONG(static_cast<zend_long>(intern->block->GetColumnCount()));
}

ZEND_METHOD(ClickHouse_Driver_Block, getRowCount)
{
    ZEND_PARSE_PARAMETERS_NONE();

    auto *intern = Z_CLICKHOUSE_BLOCK_P(ZEND_THIS);
    if (!intern->block) {
        RETURN_LONG(0);
    }
    RETURN_LONG(static_cast<zend_long>(intern->block->GetRowCount()));
}

ZEND_METHOD(ClickHouse_Driver_Block, getColumn)
{
    zend_long index = 0;

    ZEND_PARSE_PARAMETERS_START(1, 1)
        Z_PARAM_LONG(index)
    ZEND_PARSE_PARAMETERS_END();

    auto *intern = Z_CLICKHOUSE_BLOCK_P(ZEND_THIS);
    if (!intern->block || index < 0 || static_cast<size_t>(index) >= intern->block->GetColumnCount()) {
        zend_throw_exception(clickhouse_ce_ValidationException, "Column index out of range", 0);
        return;
    }

    clickhouse::ColumnRef col_ref = (*intern->block)[static_cast<size_t>(index)];
    php_clickhouse_create_column_from_ref(return_value, col_ref);
}

ZEND_METHOD(ClickHouse_Driver_Block, getColumnName)
{
    zend_long index = 0;

    ZEND_PARSE_PARAMETERS_START(1, 1)
        Z_PARAM_LONG(index)
    ZEND_PARSE_PARAMETERS_END();

    auto *intern = Z_CLICKHOUSE_BLOCK_P(ZEND_THIS);
    if (!intern->block || index < 0 || static_cast<size_t>(index) >= intern->block->GetColumnCount()) {
        zend_throw_exception(clickhouse_ce_ValidationException, "Column index out of range", 0);
        return;
    }

    const auto &name = intern->block->GetColumnName(static_cast<size_t>(index));
    RETURN_STRINGL(name.c_str(), name.size());
}

ZEND_METHOD(ClickHouse_Driver_Block, getColumnType)
{
    zend_long index = 0;

    ZEND_PARSE_PARAMETERS_START(1, 1)
        Z_PARAM_LONG(index)
    ZEND_PARSE_PARAMETERS_END();

    auto *intern = Z_CLICKHOUSE_BLOCK_P(ZEND_THIS);
    if (!intern->block || index < 0 || static_cast<size_t>(index) >= intern->block->GetColumnCount()) {
        zend_throw_exception(clickhouse_ce_ValidationException, "Column index out of range", 0);
        return;
    }

    auto col = (*intern->block)[static_cast<size_t>(index)];
    zend_long code = static_cast<zend_long>(col->Type()->GetCode());
    zend_object *enum_obj = nullptr;
    if (zend_enum_get_case_by_value(&enum_obj, clickhouse_ce_Type, code, nullptr, false) == SUCCESS && enum_obj) {
        RETURN_OBJ_COPY(enum_obj);
    }
    RETURN_NULL();
}

ZEND_METHOD(ClickHouse_Driver_Block, getColumnTypeName)
{
    zend_long index = 0;

    ZEND_PARSE_PARAMETERS_START(1, 1)
        Z_PARAM_LONG(index)
    ZEND_PARSE_PARAMETERS_END();

    auto *intern = Z_CLICKHOUSE_BLOCK_P(ZEND_THIS);
    if (!intern->block || index < 0 || static_cast<size_t>(index) >= intern->block->GetColumnCount()) {
        zend_throw_exception(clickhouse_ce_ValidationException, "Column index out of range", 0);
        return;
    }

    auto col = (*intern->block)[static_cast<size_t>(index)];
    std::string type_name = col->Type()->GetName();
    RETURN_STRINGL(type_name.c_str(), type_name.size());
}

ZEND_METHOD(ClickHouse_Driver_Block, toArray)
{
    ZEND_PARSE_PARAMETERS_NONE();

    auto *intern = Z_CLICKHOUSE_BLOCK_P(ZEND_THIS);
    if (!intern->block) {
        array_init(return_value);
        return;
    }

    size_t rows = intern->block->GetRowCount();
    size_t cols = intern->block->GetColumnCount();

    array_init_size(return_value, rows);

    for (size_t r = 0; r < rows; ++r) {
        zval row;
        array_init_size(&row, cols);

        for (size_t c = 0; c < cols; ++c) {
            zval val;
            php_clickhouse_column_to_zval((*intern->block)[c], r, &val);
            add_assoc_zval_ex(&row,
                intern->block->GetColumnName(c).c_str(),
                intern->block->GetColumnName(c).size(),
                &val);
        }

        add_next_index_zval(return_value, &row);
    }
}

void php_clickhouse_create_block_from_cpp(zval *return_value, const clickhouse::Block &cpp_block)
{
    object_init_ex(return_value, clickhouse_ce_Block);
    auto *intern = Z_CLICKHOUSE_BLOCK_P(return_value);

    /* Copy the block — shares column refs via shared_ptr */
    intern->block = std::make_unique<clickhouse::Block>();
    for (size_t i = 0; i < cpp_block.GetColumnCount(); ++i) {
        intern->block->AppendColumn(
            cpp_block.GetColumnName(i),
            cpp_block[i]);
    }
}

static const zend_function_entry class_ClickHouse_Driver_Block_methods[] = {
    ZEND_ME(ClickHouse_Driver_Block, __construct, arginfo_class_ClickHouse_Driver_Block___construct, ZEND_ACC_PUBLIC)
    ZEND_ME(ClickHouse_Driver_Block, appendColumn, arginfo_class_ClickHouse_Driver_Block_appendColumn, ZEND_ACC_PUBLIC)
    ZEND_ME(ClickHouse_Driver_Block, getColumnCount, arginfo_class_ClickHouse_Driver_Block_getColumnCount, ZEND_ACC_PUBLIC)
    ZEND_ME(ClickHouse_Driver_Block, getRowCount, arginfo_class_ClickHouse_Driver_Block_getRowCount, ZEND_ACC_PUBLIC)
    ZEND_ME(ClickHouse_Driver_Block, getColumn, arginfo_class_ClickHouse_Driver_Block_getColumn, ZEND_ACC_PUBLIC)
    ZEND_ME(ClickHouse_Driver_Block, getColumnName, arginfo_class_ClickHouse_Driver_Block_getColumnName, ZEND_ACC_PUBLIC)
    ZEND_ME(ClickHouse_Driver_Block, getColumnType, arginfo_class_ClickHouse_Driver_Block_getColumnType, ZEND_ACC_PUBLIC)
    ZEND_ME(ClickHouse_Driver_Block, getColumnTypeName, arginfo_class_ClickHouse_Driver_Block_getColumnTypeName, ZEND_ACC_PUBLIC)
    ZEND_ME(ClickHouse_Driver_Block, toArray, arginfo_class_ClickHouse_Driver_Block_toArray, ZEND_ACC_PUBLIC)
    ZEND_FE_END
};

void php_clickhouse_register_block(int module_number)
{
    zend_class_entry ce;

    INIT_NS_CLASS_ENTRY(ce, "ClickHouse\\Driver", "Block",
        class_ClickHouse_Driver_Block_methods);
    clickhouse_ce_Block = zend_register_internal_class(&ce);
    clickhouse_ce_Block->ce_flags |= ZEND_ACC_FINAL;
    clickhouse_ce_Block->create_object = php_clickhouse_block_create;
#if PHP_VERSION_ID >= 80200
    clickhouse_ce_Block->default_object_handlers = &clickhouse_block_handlers;
#endif

    memcpy(&clickhouse_block_handlers, zend_get_std_object_handlers(), sizeof(zend_object_handlers));
    clickhouse_block_handlers.offset = XtOffsetOf(php_clickhouse_block, std);
    clickhouse_block_handlers.free_obj = php_clickhouse_block_free;
    clickhouse_block_handlers.clone_obj = nullptr;
}
