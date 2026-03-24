#ifndef PHP_CLICKHOUSE_COMMON_H
#define PHP_CLICKHOUSE_COMMON_H

#include "php_clickhouse.h"
#include "clickhouse/exceptions.h"

#include <memory>
#include <new>

void php_clickhouse_throw_server_exception(const clickhouse::ServerException &e);
void php_clickhouse_throw_exception(const char *message, zend_class_entry *ce);

#define CLICKHOUSE_TRY try {

#define CLICKHOUSE_CATCH \
    } catch (const clickhouse::ServerException &e) { \
        php_clickhouse_throw_server_exception(e); \
        return; \
    } catch (const clickhouse::ValidationError &e) { \
        php_clickhouse_throw_exception(e.what(), clickhouse_ce_ValidationException); \
        return; \
    } catch (const clickhouse::ProtocolError &e) { \
        php_clickhouse_throw_exception(e.what(), clickhouse_ce_ProtocolException); \
        return; \
    } catch (const clickhouse::Error &e) { \
        php_clickhouse_throw_exception(e.what(), clickhouse_ce_ClickHouseException); \
        return; \
    } catch (const std::exception &e) { \
        php_clickhouse_throw_exception(e.what(), clickhouse_ce_ClickHouseException); \
        return; \
    }

/* Variant that returns after throwing (for methods with RETURN_*) */
#define CLICKHOUSE_CATCH_RETURN \
    } catch (const clickhouse::ServerException &e) { \
        php_clickhouse_throw_server_exception(e); \
        return; \
    } catch (const clickhouse::ValidationError &e) { \
        php_clickhouse_throw_exception(e.what(), clickhouse_ce_ValidationException); \
        return; \
    } catch (const clickhouse::ProtocolError &e) { \
        php_clickhouse_throw_exception(e.what(), clickhouse_ce_ProtocolException); \
        return; \
    } catch (const clickhouse::Error &e) { \
        php_clickhouse_throw_exception(e.what(), clickhouse_ce_ClickHouseException); \
        return; \
    } catch (const std::exception &e) { \
        php_clickhouse_throw_exception(e.what(), clickhouse_ce_ClickHouseException); \
        return; \
    }

/* Generic from_obj using XtOffsetOf — used by per-class macros */
template <typename T>
static inline T *php_clickhouse_from_obj(zend_object *obj, size_t offset) {
    return reinterpret_cast<T *>(reinterpret_cast<char *>(obj) - offset);
}

#endif /* PHP_CLICKHOUSE_COMMON_H */
