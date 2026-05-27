#ifndef PHP_CLICKHOUSE_COMMON_H
#define PHP_CLICKHOUSE_COMMON_H

#include "php_clickhouse.h"
#include "clickhouse/exceptions.h"

#include <memory>
#include <new>

void php_clickhouse_throw_server_exception(const clickhouse::ServerException &e);
void php_clickhouse_throw_exception(const char *message, zend_class_entry *ce);

#define CLICKHOUSE_TRY try {

#define CLICKHOUSE_CATCH                                                                           \
    }                                                                                              \
    catch (const clickhouse::ServerException &e)                                                   \
    {                                                                                              \
        php_clickhouse_throw_server_exception(e);                                                  \
        return;                                                                                    \
    }                                                                                              \
    catch (const clickhouse::ValidationError &e)                                                   \
    {                                                                                              \
        php_clickhouse_throw_exception(e.what(), clickhouse_ce_ValidationException);               \
        return;                                                                                    \
    }                                                                                              \
    catch (const clickhouse::ProtocolError &e)                                                     \
    {                                                                                              \
        php_clickhouse_throw_exception(e.what(), clickhouse_ce_ProtocolException);                 \
        return;                                                                                    \
    }                                                                                              \
    catch (const clickhouse::Error &e)                                                             \
    {                                                                                              \
        php_clickhouse_throw_exception(e.what(), clickhouse_ce_ClickHouseException);               \
        return;                                                                                    \
    }                                                                                              \
    catch (const std::exception &e)                                                                \
    {                                                                                              \
        php_clickhouse_throw_exception(e.what(), clickhouse_ce_ClickHouseException);               \
        return;                                                                                    \
    }

/* Variant that returns after throwing (for methods with RETURN_*) */
#define CLICKHOUSE_CATCH_RETURN                                                                    \
    }                                                                                              \
    catch (const clickhouse::ServerException &e)                                                   \
    {                                                                                              \
        php_clickhouse_throw_server_exception(e);                                                  \
        return;                                                                                    \
    }                                                                                              \
    catch (const clickhouse::ValidationError &e)                                                   \
    {                                                                                              \
        php_clickhouse_throw_exception(e.what(), clickhouse_ce_ValidationException);               \
        return;                                                                                    \
    }                                                                                              \
    catch (const clickhouse::ProtocolError &e)                                                     \
    {                                                                                              \
        php_clickhouse_throw_exception(e.what(), clickhouse_ce_ProtocolException);                 \
        return;                                                                                    \
    }                                                                                              \
    catch (const clickhouse::Error &e)                                                             \
    {                                                                                              \
        php_clickhouse_throw_exception(e.what(), clickhouse_ce_ClickHouseException);               \
        return;                                                                                    \
    }                                                                                              \
    catch (const std::exception &e)                                                                \
    {                                                                                              \
        php_clickhouse_throw_exception(e.what(), clickhouse_ce_ClickHouseException);               \
        return;                                                                                    \
    }

/* PHP 8.1/8.2 compat: zend_enum_get_case_by_value is not available everywhere. */
#if PHP_VERSION_ID >= 80100 && PHP_VERSION_ID < 80300
static inline zend_result php_clickhouse_enum_get_case(zend_object **result, zend_class_entry *ce,
                                                       zend_long value)
{
    zend_string *cases_name = zend_string_init("cases", sizeof("cases") - 1, 0);
    zend_function *cases_fn =
        static_cast<zend_function *>(zend_hash_find_ptr(&ce->function_table, cases_name));
    zend_string_release(cases_name);
    if (!cases_fn)
        return FAILURE;

    zval cases_rv;
    zend_call_known_function(cases_fn, NULL, ce, &cases_rv, 0, NULL, NULL);
    if (Z_TYPE(cases_rv) != IS_ARRAY) {
        zval_ptr_dtor(&cases_rv);
        return FAILURE;
    }

    zval *entry;
    ZEND_HASH_FOREACH_VAL(Z_ARRVAL(cases_rv), entry)
    {
        if (Z_TYPE_P(entry) == IS_OBJECT) {
            zval *val = zend_enum_fetch_case_value(Z_OBJ_P(entry));
            if (val && Z_TYPE_P(val) == IS_LONG && Z_LVAL_P(val) == value) {
                *result = Z_OBJ_P(entry);
                GC_ADDREF(*result);
                zval_ptr_dtor(&cases_rv);
                return SUCCESS;
            }
        }
    }
    ZEND_HASH_FOREACH_END();
    zval_ptr_dtor(&cases_rv);
    return FAILURE;
}
#endif

/* Generic from_obj using XtOffsetOf — used by per-class macros */
template <typename T> static inline T *php_clickhouse_from_obj(zend_object *obj, size_t offset)
{
    return reinterpret_cast<T *>(reinterpret_cast<char *>(obj) - offset);
}

#endif /* PHP_CLICKHOUSE_COMMON_H */
