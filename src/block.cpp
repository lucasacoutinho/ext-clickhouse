#include "src/block.h"
#include "src/column.h"
#include "src/column_convert.h"
#include "src/common.h"
#include "clickhouse_arginfo.h"
#include "clickhouse/columns/numeric.h"
#include "clickhouse/columns/string.h"

#include <algorithm>
#include <cstddef>
#include <string_view>
#include <vector>

zend_class_entry *clickhouse_ce_Block = nullptr;
static zend_object_handlers clickhouse_block_handlers;

static zend_object *php_clickhouse_block_create(zend_class_entry *ce)
{
    auto *intern =
        static_cast<php_clickhouse_block *>(zend_object_alloc(sizeof(php_clickhouse_block), ce));

    new (&intern->block) std::unique_ptr<clickhouse::Block>();

    zend_object_std_init(&intern->std, ce);
    object_properties_init(&intern->std, ce);
    intern->std.handlers = &clickhouse_block_handlers;

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
    intern->block->AppendColumn(std::string(ZSTR_VAL(name), ZSTR_LEN(name)), col_intern->column);
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
    if (!intern->block || index < 0 ||
        static_cast<size_t>(index) >= intern->block->GetColumnCount()) {
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
    if (!intern->block || index < 0 ||
        static_cast<size_t>(index) >= intern->block->GetColumnCount()) {
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
    if (!intern->block || index < 0 ||
        static_cast<size_t>(index) >= intern->block->GetColumnCount()) {
        zend_throw_exception(clickhouse_ce_ValidationException, "Column index out of range", 0);
        return;
    }

    auto col = (*intern->block)[static_cast<size_t>(index)];
    zend_long code = static_cast<zend_long>(col->Type()->GetCode());
#if PHP_VERSION_ID < 80100
    RETURN_LONG(code);
#else
    zend_object *enum_obj = nullptr;
#if PHP_VERSION_ID >= 80300
    if (zend_enum_get_case_by_value(&enum_obj, clickhouse_ce_Type, code, nullptr, false) ==
            SUCCESS &&
        enum_obj) {
#else
    if (php_clickhouse_enum_get_case(&enum_obj, clickhouse_ce_Type, code) == SUCCESS && enum_obj) {
#endif
        RETURN_OBJ_COPY(enum_obj);
    }
    RETURN_NULL();
#endif
}

ZEND_METHOD(ClickHouse_Driver_Block, getColumnTypeName)
{
    zend_long index = 0;

    ZEND_PARSE_PARAMETERS_START(1, 1)
    Z_PARAM_LONG(index)
    ZEND_PARSE_PARAMETERS_END();

    auto *intern = Z_CLICKHOUSE_BLOCK_P(ZEND_THIS);
    if (!intern->block || index < 0 ||
        static_cast<size_t>(index) >= intern->block->GetColumnCount()) {
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

    array_init_size(return_value, intern->block->GetRowCount());

    php_clickhouse_block_append_rows(*intern->block, return_value);
}

namespace {

/* A row with the result keys and null values. Duplicating it skips hashing every key per row. */
class RowTemplate
{
  public:
    explicit RowTemplate(const clickhouse::Block &block)
        : row_(zend_new_array(block.GetColumnCount())), slots_(block.GetColumnCount())
    {
        zend_hash_real_init_mixed(row_);

        zval null_value;
        ZVAL_NULL(&null_value);

        for (size_t c = 0; c < slots_.size(); ++c) {
            const std::string &name = block.GetColumnName(c);
            zend_string *key = zend_string_init(name.data(), name.size(), 0);
            const zval *slot = zend_symtable_update(row_, key, &null_value);
            zend_string_release(key);
            slots_[c] =
                static_cast<uint32_t>(reinterpret_cast<const Bucket *>(slot) - row_->arData);
        }
    }

    ~RowTemplate()
    {
        zend_array_destroy(row_);
    }

    RowTemplate(const RowTemplate &) = delete;
    RowTemplate &operator=(const RowTemplate &) = delete;

    zend_array *NewRow() const
    {
        return zend_array_dup(row_);
    }

    uint32_t Slot(size_t c) const
    {
        return slots_[c];
    }

    /* With duplicate names the last column wins, and each slot must be written only once. */
    bool OwnsSlot(size_t c) const
    {
        return std::find(slots_.begin() + static_cast<std::ptrdiff_t>(c) + 1, slots_.end(),
                         slots_[c]) == slots_.end();
    }

  private:
    zend_array *row_;
    std::vector<uint32_t> slots_;
};

using RowList = std::vector<zend_array *>;

template <typename Write> inline void fill_slots(const RowList &rows, uint32_t slot, Write write)
{
    for (size_t r = 0; r < rows.size(); ++r) {
        write(r, &rows[r]->arData[slot].val);
    }
}

template <typename T> inline const T *numeric_data(const clickhouse::ColumnRef &column)
{
    return static_cast<clickhouse::ColumnVector<T> *>(column.get())->GetWritableData().data();
}

template <typename T>
inline void fill_long(const clickhouse::ColumnRef &column, const RowList &rows, uint32_t slot)
{
    const T *data = numeric_data<T>(column);
    fill_slots(rows, slot,
               [data](size_t r, zval *out) { ZVAL_LONG(out, static_cast<zend_long>(data[r])); });
}

template <typename T>
inline void fill_double(const clickhouse::ColumnRef &column, const RowList &rows, uint32_t slot)
{
    const T *data = numeric_data<T>(column);
    fill_slots(rows, slot,
               [data](size_t r, zval *out) { ZVAL_DOUBLE(out, static_cast<double>(data[r])); });
}

inline void fill_uint64(const clickhouse::ColumnRef &column, const RowList &rows, uint32_t slot)
{
    const uint64_t *data = numeric_data<uint64_t>(column);
    fill_slots(rows, slot, [&column, data](size_t r, zval *out) {
        if (EXPECTED(data[r] <= static_cast<uint64_t>(ZEND_LONG_MAX))) {
            ZVAL_LONG(out, static_cast<zend_long>(data[r]));
        } else {
            php_clickhouse_column_to_zval(column, r, out);
        }
    });
}

/* Resolve the type once per block. Types without a loop here convert one cell at a time. */
void fill_column(const clickhouse::ColumnRef &column, const RowList &rows, uint32_t slot)
{
    using clickhouse::Type;

    switch (column->GetType().GetCode()) {
    case Type::Int8:
        return fill_long<int8_t>(column, rows, slot);
    case Type::Int16:
        return fill_long<int16_t>(column, rows, slot);
    case Type::Int32:
        return fill_long<int32_t>(column, rows, slot);
    case Type::Int64:
        return fill_long<int64_t>(column, rows, slot);
    case Type::UInt8:
        return fill_long<uint8_t>(column, rows, slot);
    case Type::UInt16:
        return fill_long<uint16_t>(column, rows, slot);
    case Type::UInt32:
        return fill_long<uint32_t>(column, rows, slot);
    case Type::UInt64:
        return fill_uint64(column, rows, slot);
    case Type::Float32:
        return fill_double<float>(column, rows, slot);
    case Type::Float64:
        return fill_double<double>(column, rows, slot);
    case Type::String: {
        const auto *strings = static_cast<const clickhouse::ColumnString *>(column.get());
        return fill_slots(rows, slot, [strings](size_t r, zval *out) {
            const std::string_view value = strings->At(r);
            ZVAL_STRINGL(out, value.data(), value.size());
        });
    }
    default:
        return fill_slots(rows, slot, [&column](size_t r, zval *out) {
            php_clickhouse_column_to_zval(column, r, out);
        });
    }
}

} // namespace

void php_clickhouse_block_append_rows(const clickhouse::Block &block, zval *rows_out)
{
    const size_t row_count = block.GetRowCount();

    if (row_count == 0) {
        return;
    }

    HashTable *result = Z_ARRVAL_P(rows_out);
    zend_hash_extend(result, zend_hash_num_elements(result) + row_count, 1);

    const RowTemplate row_template(block);

    /* Rows join the result before they are filled, so a conversion failure cannot leak them. */
    RowList rows(row_count);
    for (size_t r = 0; r < row_count; ++r) {
        rows[r] = row_template.NewRow();
        zval row;
        ZVAL_ARR(&row, rows[r]);
        zend_hash_next_index_insert_new(result, &row);
    }

    for (size_t c = 0; c < block.GetColumnCount(); ++c) {
        if (row_template.OwnsSlot(c)) {
            fill_column(block[c], rows, row_template.Slot(c));
        }
    }
}

void php_clickhouse_create_block_from_cpp(zval *return_value, const clickhouse::Block &cpp_block)
{
    object_init_ex(return_value, clickhouse_ce_Block);
    auto *intern = Z_CLICKHOUSE_BLOCK_P(return_value);

    /* Copy the block. This shares column refs through shared_ptr. */
    intern->block = std::make_unique<clickhouse::Block>();
    for (size_t i = 0; i < cpp_block.GetColumnCount(); ++i) {
        intern->block->AppendColumn(cpp_block.GetColumnName(i), cpp_block[i]);
    }
}

static const zend_function_entry class_ClickHouse_Driver_Block_methods[] = {
    ZEND_ME(ClickHouse_Driver_Block, __construct, arginfo_class_ClickHouse_Driver_Block___construct,
            ZEND_ACC_PUBLIC) ZEND_ME(ClickHouse_Driver_Block, appendColumn,
                                     arginfo_class_ClickHouse_Driver_Block_appendColumn,
                                     ZEND_ACC_PUBLIC)
        ZEND_ME(ClickHouse_Driver_Block, getColumnCount,
                arginfo_class_ClickHouse_Driver_Block_getColumnCount, ZEND_ACC_PUBLIC)
            ZEND_ME(ClickHouse_Driver_Block, getRowCount,
                    arginfo_class_ClickHouse_Driver_Block_getRowCount,
                    ZEND_ACC_PUBLIC) ZEND_ME(ClickHouse_Driver_Block, getColumn,
                                             arginfo_class_ClickHouse_Driver_Block_getColumn,
                                             ZEND_ACC_PUBLIC)
                ZEND_ME(ClickHouse_Driver_Block, getColumnName,
                        arginfo_class_ClickHouse_Driver_Block_getColumnName, ZEND_ACC_PUBLIC)
                    ZEND_ME(ClickHouse_Driver_Block, getColumnType,
                            arginfo_class_ClickHouse_Driver_Block_getColumnType, ZEND_ACC_PUBLIC)
                        ZEND_ME(ClickHouse_Driver_Block, getColumnTypeName,
                                arginfo_class_ClickHouse_Driver_Block_getColumnTypeName,
                                ZEND_ACC_PUBLIC)
                            ZEND_ME(ClickHouse_Driver_Block, toArray,
                                    arginfo_class_ClickHouse_Driver_Block_toArray, ZEND_ACC_PUBLIC)
                                ZEND_FE_END};

void php_clickhouse_register_block(int module_number)
{
    zend_class_entry ce;

    INIT_NS_CLASS_ENTRY(ce, "ClickHouse\\Driver", "Block", class_ClickHouse_Driver_Block_methods);
    clickhouse_ce_Block = zend_register_internal_class(&ce);
    clickhouse_ce_Block->ce_flags |= ZEND_ACC_FINAL;
    clickhouse_ce_Block->create_object = php_clickhouse_block_create;
#if PHP_VERSION_ID >= 80300
    clickhouse_ce_Block->default_object_handlers = &clickhouse_block_handlers;
#endif

    memcpy(&clickhouse_block_handlers, zend_get_std_object_handlers(),
           sizeof(zend_object_handlers));
    clickhouse_block_handlers.offset = XtOffsetOf(php_clickhouse_block, std);
    clickhouse_block_handlers.free_obj = php_clickhouse_block_free;
    clickhouse_block_handlers.clone_obj = nullptr;
}
