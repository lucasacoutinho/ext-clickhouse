#include "src/exceptions.h"
#include "src/common.h"
#include "clickhouse_arginfo.h"
#include "clickhouse/exceptions.h"

extern "C" {
#include "ext/spl/spl_exceptions.h"
}

zend_class_entry *clickhouse_ce_ClickHouseException = nullptr;
zend_class_entry *clickhouse_ce_ConnectionException = nullptr;
zend_class_entry *clickhouse_ce_ServerException = nullptr;
zend_class_entry *clickhouse_ce_ProtocolException = nullptr;
zend_class_entry *clickhouse_ce_ValidationException = nullptr;

ZEND_METHOD(ClickHouse_Driver_Exception_ServerException, getClickHouseCode)
{
    ZEND_PARSE_PARAMETERS_NONE();

    zval *code_prop = zend_read_property(
        clickhouse_ce_ServerException, Z_OBJ_P(ZEND_THIS),
        "clickHouseCode", sizeof("clickHouseCode") - 1, 1, NULL);

    if (code_prop && Z_TYPE_P(code_prop) == IS_LONG) {
        RETURN_LONG(Z_LVAL_P(code_prop));
    }
    RETURN_LONG(0);
}

static const zend_function_entry class_ClickHouse_Driver_Exception_ServerException_methods[] = {
    ZEND_ME(ClickHouse_Driver_Exception_ServerException, getClickHouseCode, arginfo_class_ClickHouse_Driver_Exception_ServerException_getClickHouseCode, ZEND_ACC_PUBLIC)
    ZEND_FE_END
};

void php_clickhouse_register_exceptions(int module_number)
{
    zend_class_entry ce;

    /* ClickHouse\Driver\Exception\ClickHouseException extends \Exception */
    INIT_NS_CLASS_ENTRY(ce, "ClickHouse\\Driver\\Exception", "ClickHouseException", NULL);
    clickhouse_ce_ClickHouseException = zend_register_internal_class_ex(&ce, zend_ce_exception);

    /* ConnectionException extends ClickHouseException */
    INIT_NS_CLASS_ENTRY(ce, "ClickHouse\\Driver\\Exception", "ConnectionException", NULL);
    clickhouse_ce_ConnectionException = zend_register_internal_class_ex(&ce, clickhouse_ce_ClickHouseException);

    /* ServerException extends ClickHouseException */
    INIT_NS_CLASS_ENTRY(ce, "ClickHouse\\Driver\\Exception", "ServerException",
        class_ClickHouse_Driver_Exception_ServerException_methods);
    clickhouse_ce_ServerException = zend_register_internal_class_ex(&ce, clickhouse_ce_ClickHouseException);
    zend_declare_property_long(clickhouse_ce_ServerException, "clickHouseCode", sizeof("clickHouseCode") - 1, 0, ZEND_ACC_PROTECTED);

    /* ProtocolException extends ClickHouseException */
    INIT_NS_CLASS_ENTRY(ce, "ClickHouse\\Driver\\Exception", "ProtocolException", NULL);
    clickhouse_ce_ProtocolException = zend_register_internal_class_ex(&ce, clickhouse_ce_ClickHouseException);

    /* ValidationException extends \InvalidArgumentException */
    INIT_NS_CLASS_ENTRY(ce, "ClickHouse\\Driver\\Exception", "ValidationException", NULL);
    clickhouse_ce_ValidationException = zend_register_internal_class_ex(&ce, spl_ce_InvalidArgumentException);
}

void php_clickhouse_throw_server_exception(const clickhouse::ServerException &e)
{
    zend_object *exception_obj = zend_throw_exception(
        clickhouse_ce_ServerException,
        e.what(),
        static_cast<zend_long>(e.GetCode()));

    if (exception_obj) {
        zend_update_property_long(
            clickhouse_ce_ServerException, exception_obj,
            "clickHouseCode", sizeof("clickHouseCode") - 1,
            static_cast<zend_long>(e.GetCode()));
    }
}

void php_clickhouse_throw_exception(const char *message, zend_class_entry *ce)
{
    zend_throw_exception(ce, message, 0);
}
