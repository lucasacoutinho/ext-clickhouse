/*
  +----------------------------------------------------------------------+
  | ClickHouse Native Driver for PHP                                    |
  +----------------------------------------------------------------------+
  | Copyright (c) 2024                                                  |
  +----------------------------------------------------------------------+
  | This source file is subject to the MIT license that is bundled      |
  | with this package in the file LICENSE.                              |
  +----------------------------------------------------------------------+
*/

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include "php.h"
#include "php_ini.h"
#include "ext/standard/info.h"
#include "ext/standard/url.h"
#include "ext/spl/spl_iterators.h"
#include "ext/json/php_json.h"
#include "zend_exceptions.h"
#include "zend_interfaces.h"
#include "zend_smart_str.h"
#include "zend_ptr_stack.h"
#include "php_clickhouse.h"
#include <stdio.h>
#include <string.h>

/* Module globals */
ZEND_DECLARE_MODULE_GLOBALS(clickhouse)

/* Persistent connection resource type */
static int le_pclickhouse;

/* Class entries */
zend_class_entry *clickhouse_client_ce;
zend_class_entry *clickhouse_statement_ce;
zend_class_entry *clickhouse_asyncresult_ce;
zend_class_entry *clickhouse_resultiterator_ce;
zend_class_entry *clickhouse_streamingiterator_ce;
zend_class_entry *clickhouse_exception_ce;

/* Object handlers */
static zend_object_handlers clickhouse_client_handlers;
static zend_object_handlers clickhouse_statement_handlers;
static zend_object_handlers clickhouse_asyncresult_handlers;
static zend_object_handlers clickhouse_resultiterator_handlers;
static zend_object_handlers clickhouse_streamingiterator_handlers;

/* Client object structure */
typedef struct {
    clickhouse_connection *conn;
    zend_string *hash_key;           /* For persistent connections */
    zend_bool persistent;            /* Is this a persistent connection? */
    uint8_t compression;             /* Compression method (0=none, 1=LZ4, 2=ZSTD) */
    char *session_id;                /* Session ID for stateful queries */
    char *default_query_id;          /* Default query ID prefix */
    zend_long query_timeout_ms;      /* Query timeout in milliseconds (0=no timeout) */
    /* Reconnection support */
    zend_bool auto_reconnect;        /* Auto-reconnect on connection loss */
    char *saved_host;                /* Saved connection parameters for reconnect */
    uint16_t saved_port;
    char *saved_user;
    char *saved_password;
    char *saved_database;
    /* Retry configuration */
    zend_long max_retry_attempts;    /* Maximum number of retry attempts (0=no limit) */
    double retry_base_delay;         /* Base delay in seconds for exponential backoff */
    double retry_max_delay;          /* Maximum delay in seconds between retries */
    zend_bool retry_jitter;          /* Enable jitter in retry delays */
    zend_long total_retry_attempts;  /* Total retry attempts made (for metrics) */
    /* Query tracking and settings */
    char *last_query_id;             /* Query ID from last executed query */
    clickhouse_settings *query_settings;  /* Per-query settings to apply to all queries */
    /* Callbacks */
    zval progress_callback;          /* Progress callback for query execution */
    zval profile_callback;           /* Profile callback for query profiling info */
    zval log_callback;               /* Log callback for server log messages */
    /* Safety features */
    zend_bool readonly;              /* Read-only mode - prevents write operations */
    /* Transaction support (EXPERIMENTAL) */
    zend_bool in_transaction;        /* Currently in a transaction */
    char *transaction_id;            /* Transaction ID (session based) */
    /* Metrics tracking */
    zend_bool metrics_enabled;       /* Enable metrics collection */
    zend_long queries_executed;      /* Total queries executed */
    zend_long queries_failed;        /* Total queries failed */
    double total_query_time;         /* Total query execution time (seconds) */
    zend_long total_rows_read;       /* Total rows read */
    zend_long total_bytes_read;      /* Total bytes read */
    zend_long slow_queries;          /* Queries exceeding slow threshold */
    double slow_query_threshold;     /* Threshold in seconds for slow queries (0=disabled) */
    zend_object std;
} clickhouse_client_object;

/* Statement object structure */
typedef struct {
    zval client_zv;                  /* Reference to client object */
    clickhouse_connection *conn;     /* Borrowed reference */
    char *query;                     /* Query template */
    clickhouse_query_options *opts;  /* Query options with params */
    zend_object std;
} clickhouse_statement_object;

/* AsyncResult object structure */
typedef struct {
    zval client_zv;                  /* Reference to client object */
    clickhouse_connection *conn;     /* Borrowed reference */
    clickhouse_async_query *async;   /* Async query state */
    zval cached_result;              /* Cached result after wait() */
    zend_bool has_result;            /* Whether result has been retrieved */
    zend_object std;
} clickhouse_asyncresult_object;

/* ResultIterator object structure - for buffered iteration (loads all data first) */
typedef struct {
    zval client_zv;                  /* Reference to client object */
    clickhouse_connection *conn;     /* Borrowed reference */
    clickhouse_result *result;       /* Full result (blocks loaded incrementally) */
    size_t current_block;            /* Current block index */
    size_t current_row;              /* Current row within block */
    zend_long current_key;           /* Current iterator key (total row index) */
    zend_bool valid;                 /* Is current position valid? */
    zend_bool finished;              /* Has iteration finished? */
    zend_object std;
} clickhouse_resultiterator_object;

/* StreamingIterator object structure - for true streaming (block-by-block from server) */
typedef struct {
    zval client_zv;                  /* Reference to client object */
    clickhouse_connection *conn;     /* Borrowed reference */
    clickhouse_streaming_query *sq;  /* Streaming query handle */
    char *query_sql;                 /* SQL query for re-execution on rewind */
    size_t current_row;              /* Current row within current block */
    zend_long current_key;           /* Current iterator key (total row index) */
    zend_long total_rows;            /* Total rows fetched so far */
    zend_bool valid;                 /* Is current position valid? */
    zend_bool started;               /* Has iteration started? */
    zend_object std;
} clickhouse_streamingiterator_object;

/* Helper to get client object from zend_object */
static inline clickhouse_client_object *clickhouse_client_from_obj(zend_object *obj) {
    return (clickhouse_client_object *)((char *)obj - XtOffsetOf(clickhouse_client_object, std));
}

static inline clickhouse_statement_object *clickhouse_statement_from_obj(zend_object *obj) {
    return (clickhouse_statement_object *)((char *)obj - XtOffsetOf(clickhouse_statement_object, std));
}

static inline clickhouse_asyncresult_object *clickhouse_asyncresult_from_obj(zend_object *obj) {
    return (clickhouse_asyncresult_object *)((char *)obj - XtOffsetOf(clickhouse_asyncresult_object, std));
}

static inline clickhouse_resultiterator_object *clickhouse_resultiterator_from_obj(zend_object *obj) {
    return (clickhouse_resultiterator_object *)((char *)obj - XtOffsetOf(clickhouse_resultiterator_object, std));
}

static inline clickhouse_streamingiterator_object *clickhouse_streamingiterator_from_obj(zend_object *obj) {
    return (clickhouse_streamingiterator_object *)((char *)obj - XtOffsetOf(clickhouse_streamingiterator_object, std));
}

#define Z_CLICKHOUSE_CLIENT_P(zv) clickhouse_client_from_obj(Z_OBJ_P(zv))
#define Z_CLICKHOUSE_STATEMENT_P(zv) clickhouse_statement_from_obj(Z_OBJ_P(zv))
#define Z_CLICKHOUSE_ASYNCRESULT_P(zv) clickhouse_asyncresult_from_obj(Z_OBJ_P(zv))
#define Z_CLICKHOUSE_RESULTITERATOR_P(zv) clickhouse_resultiterator_from_obj(Z_OBJ_P(zv))
#define Z_CLICKHOUSE_STREAMINGITERATOR_P(zv) clickhouse_streamingiterator_from_obj(Z_OBJ_P(zv))

/* {{{ Free client object */
static void clickhouse_client_free(zend_object *object) {
    clickhouse_client_object *intern = clickhouse_client_from_obj(object);

    if (intern->conn) {
        if (intern->persistent && intern->hash_key) {
            /* Return connection to persistent pool */
            zend_resource *le;
            if ((le = zend_hash_find_ptr(&EG(persistent_list), intern->hash_key)) != NULL) {
                clickhouse_plist_entry *plist = (clickhouse_plist_entry *)le->ptr;
                /* Only return if connection is still valid */
                if (clickhouse_connection_ping(intern->conn) == 0) {
                    zend_ptr_stack_push(&plist->free_connections, intern->conn);
                    CLICKHOUSE_G(num_persistent)--;
                    intern->conn = NULL;
                } else {
                    /* Connection dead, close it */
                    clickhouse_connection_close(intern->conn);
                    clickhouse_connection_free(intern->conn);
                    intern->conn = NULL;
                    CLICKHOUSE_G(num_persistent)--;
                }
            }
        }

        if (intern->conn) {
            clickhouse_connection_close(intern->conn);
            clickhouse_connection_free(intern->conn);
            intern->conn = NULL;
        }

        CLICKHOUSE_G(num_links)--;
    }

    if (intern->hash_key) {
        zend_string_release(intern->hash_key);
        intern->hash_key = NULL;
    }

    if (intern->session_id) {
        efree(intern->session_id);
        intern->session_id = NULL;
    }

    if (intern->default_query_id) {
        efree(intern->default_query_id);
        intern->default_query_id = NULL;
    }

    /* Free saved connection parameters for reconnect */
    if (intern->saved_host) {
        efree(intern->saved_host);
        intern->saved_host = NULL;
    }
    if (intern->saved_user) {
        efree(intern->saved_user);
        intern->saved_user = NULL;
    }
    if (intern->saved_password) {
        efree(intern->saved_password);
        intern->saved_password = NULL;
    }
    if (intern->saved_database) {
        efree(intern->saved_database);
        intern->saved_database = NULL;
    }

    /* Free query tracking and settings */
    if (intern->last_query_id) {
        efree(intern->last_query_id);
        intern->last_query_id = NULL;
    }
    if (intern->query_settings) {
        clickhouse_settings_free(intern->query_settings);
        intern->query_settings = NULL;
    }

    /* Free callbacks */
    if (!Z_ISUNDEF(intern->progress_callback)) {
        zval_ptr_dtor(&intern->progress_callback);
        ZVAL_UNDEF(&intern->progress_callback);
    }
    if (!Z_ISUNDEF(intern->profile_callback)) {
        zval_ptr_dtor(&intern->profile_callback);
        ZVAL_UNDEF(&intern->profile_callback);
    }
    if (!Z_ISUNDEF(intern->log_callback)) {
        zval_ptr_dtor(&intern->log_callback);
        ZVAL_UNDEF(&intern->log_callback);
    }

    zend_object_std_dtor(&intern->std);
}
/* }}} */

/* {{{ Create client object */
static zend_object *clickhouse_client_create(zend_class_entry *ce) {
    clickhouse_client_object *intern = zend_object_alloc(sizeof(clickhouse_client_object), ce);

    intern->conn = NULL;
    intern->hash_key = NULL;
    intern->persistent = 0;
    intern->session_id = NULL;
    intern->default_query_id = NULL;
    intern->query_timeout_ms = 0;
    /* Initialize reconnection fields */
    intern->auto_reconnect = 0;
    intern->saved_host = NULL;
    intern->saved_port = 0;
    intern->saved_user = NULL;
    intern->saved_password = NULL;
    intern->saved_database = NULL;
    /* Initialize retry configuration */
    intern->max_retry_attempts = 3;      /* Default: 3 retries */
    intern->retry_base_delay = 0.1;      /* Default: 100ms base delay */
    intern->retry_max_delay = 5.0;       /* Default: 5 seconds max delay */
    intern->retry_jitter = 1;            /* Default: jitter enabled */
    intern->total_retry_attempts = 0;
    /* Initialize query tracking and settings */
    intern->last_query_id = NULL;
    intern->query_settings = NULL;
    /* Initialize callbacks */
    ZVAL_UNDEF(&intern->progress_callback);
    ZVAL_UNDEF(&intern->profile_callback);
    ZVAL_UNDEF(&intern->log_callback);
    /* Initialize safety features */
    intern->readonly = 0;
    /* Initialize transaction support */
    intern->in_transaction = 0;
    intern->transaction_id = NULL;
    /* Initialize metrics */
    intern->metrics_enabled = 0;
    intern->queries_executed = 0;
    intern->queries_failed = 0;
    intern->total_query_time = 0.0;
    intern->total_rows_read = 0;
    intern->total_bytes_read = 0;
    intern->slow_queries = 0;
    intern->slow_query_threshold = 0.0;

    zend_object_std_init(&intern->std, ce);
    object_properties_init(&intern->std, ce);

    intern->std.handlers = &clickhouse_client_handlers;

    return &intern->std;
}
/* }}} */

/* {{{ Free statement object */
static void clickhouse_statement_free(zend_object *object) {
    clickhouse_statement_object *intern = clickhouse_statement_from_obj(object);

    if (intern->query) {
        efree(intern->query);
        intern->query = NULL;
    }

    if (intern->opts) {
        clickhouse_query_options_free(intern->opts);
        intern->opts = NULL;
    }

    zval_ptr_dtor(&intern->client_zv);
    ZVAL_UNDEF(&intern->client_zv);

    zend_object_std_dtor(&intern->std);
}
/* }}} */

/* {{{ Create statement object */
static zend_object *clickhouse_statement_create(zend_class_entry *ce) {
    clickhouse_statement_object *intern = zend_object_alloc(sizeof(clickhouse_statement_object), ce);

    intern->conn = NULL;
    intern->query = NULL;
    intern->opts = NULL;
    ZVAL_UNDEF(&intern->client_zv);

    zend_object_std_init(&intern->std, ce);
    object_properties_init(&intern->std, ce);

    intern->std.handlers = &clickhouse_statement_handlers;

    return &intern->std;
}
/* }}} */

/* {{{ Free async result object */
static void clickhouse_asyncresult_free(zend_object *object) {
    clickhouse_asyncresult_object *intern = clickhouse_asyncresult_from_obj(object);

    if (intern->async) {
        clickhouse_async_query_free(intern->async);
        intern->async = NULL;
    }

    if (intern->has_result) {
        zval_ptr_dtor(&intern->cached_result);
    }
    ZVAL_UNDEF(&intern->cached_result);

    zval_ptr_dtor(&intern->client_zv);
    ZVAL_UNDEF(&intern->client_zv);

    zend_object_std_dtor(&intern->std);
}
/* }}} */

/* {{{ Create async result object */
static zend_object *clickhouse_asyncresult_create(zend_class_entry *ce) {
    clickhouse_asyncresult_object *intern = zend_object_alloc(sizeof(clickhouse_asyncresult_object), ce);

    intern->conn = NULL;
    intern->async = NULL;
    intern->has_result = 0;
    ZVAL_UNDEF(&intern->client_zv);
    ZVAL_UNDEF(&intern->cached_result);

    zend_object_std_init(&intern->std, ce);
    object_properties_init(&intern->std, ce);

    intern->std.handlers = &clickhouse_asyncresult_handlers;

    return &intern->std;
}
/* }}} */

/* {{{ Free result iterator object */
static void clickhouse_resultiterator_free(zend_object *object) {
    clickhouse_resultiterator_object *intern = clickhouse_resultiterator_from_obj(object);

    if (intern->result) {
        clickhouse_result_free(intern->result);
        intern->result = NULL;
    }

    zval_ptr_dtor(&intern->client_zv);
    ZVAL_UNDEF(&intern->client_zv);

    zend_object_std_dtor(&intern->std);
}
/* }}} */

/* {{{ Free streaming iterator object */
static void clickhouse_streamingiterator_free(zend_object *object) {
    clickhouse_streamingiterator_object *intern = clickhouse_streamingiterator_from_obj(object);

    if (intern->sq) {
        clickhouse_streaming_query_free(intern->sq);
        intern->sq = NULL;
    }

    if (intern->query_sql) {
        efree(intern->query_sql);
        intern->query_sql = NULL;
    }

    zval_ptr_dtor(&intern->client_zv);
    ZVAL_UNDEF(&intern->client_zv);

    zend_object_std_dtor(&intern->std);
}
/* }}} */

/* {{{ Create streaming iterator object */
static zend_object *clickhouse_streamingiterator_create(zend_class_entry *ce) {
    clickhouse_streamingiterator_object *intern = zend_object_alloc(sizeof(clickhouse_streamingiterator_object), ce);

    intern->conn = NULL;
    intern->sq = NULL;
    intern->query_sql = NULL;
    intern->current_row = 0;
    intern->current_key = 0;
    intern->total_rows = 0;
    intern->valid = 0;
    intern->started = 0;
    ZVAL_UNDEF(&intern->client_zv);

    zend_object_std_init(&intern->std, ce);
    object_properties_init(&intern->std, ce);

    intern->std.handlers = &clickhouse_streamingiterator_handlers;

    return &intern->std;
}
/* }}} */

/* {{{ Create result iterator object */
static zend_object *clickhouse_resultiterator_create(zend_class_entry *ce) {
    clickhouse_resultiterator_object *intern = zend_object_alloc(sizeof(clickhouse_resultiterator_object), ce);

    intern->conn = NULL;
    intern->result = NULL;
    intern->current_block = 0;
    intern->current_row = 0;
    intern->current_key = 0;
    intern->valid = 0;
    intern->finished = 0;
    ZVAL_UNDEF(&intern->client_zv);

    zend_object_std_init(&intern->std, ce);
    object_properties_init(&intern->std, ce);

    intern->std.handlers = &clickhouse_resultiterator_handlers;

    return &intern->std;
}
/* }}} */

/* {{{ proto void ClickHouse\Client::__construct(string $host, int $port = 9000, string $user = "default", string $password = "", string $database = "default")
   Host can be prefixed with "p:" for persistent connections (e.g., "p:localhost") */
PHP_METHOD(ClickHouse_Client, __construct) {
    char *host;
    size_t host_len;
    zend_long port = 9000;
    char *user = "default";
    size_t user_len = sizeof("default") - 1;
    char *password = "";
    size_t password_len = 0;
    char *database = "default";
    size_t database_len = sizeof("default") - 1;

    ZEND_PARSE_PARAMETERS_START(1, 5)
        Z_PARAM_STRING(host, host_len)
        Z_PARAM_OPTIONAL
        Z_PARAM_LONG(port)
        Z_PARAM_STRING(user, user_len)
        Z_PARAM_STRING(password, password_len)
        Z_PARAM_STRING(database, database_len)
    ZEND_PARSE_PARAMETERS_END();

    clickhouse_client_object *intern = Z_CLICKHOUSE_CLIENT_P(ZEND_THIS);
    zend_bool persistent = 0;
    zend_string *hash_key = NULL;
    const char *actual_host = host;

    /* Check for "p:" prefix for persistent connections */
    if (host_len > 2 && host[0] == 'p' && host[1] == ':') {
        actual_host = host + 2;
        host_len -= 2;

        if (!CLICKHOUSE_G(allow_persistent)) {
            php_error_docref(NULL, E_WARNING,
                "Persistent connections are disabled. Downgrading to normal connection");
        } else {
            persistent = 1;

            /* Create hash key for this connection */
            hash_key = strpprintf(0, "clickhouse_%s_%ld_%s_%s_%s",
                actual_host, (long)port, user, database, password);

            intern->hash_key = zend_string_copy(hash_key);
            intern->persistent = 1;

            /* Check if we can reuse an existing connection */
            zend_resource *le;
            if ((le = zend_hash_find_ptr(&EG(persistent_list), hash_key)) != NULL) {
                if (le->type == le_pclickhouse) {
                    clickhouse_plist_entry *plist = (clickhouse_plist_entry *)le->ptr;

                    if (zend_ptr_stack_num_elements(&plist->free_connections) > 0) {
                        intern->conn = zend_ptr_stack_pop(&plist->free_connections);

                        /* Verify connection is still valid */
                        if (clickhouse_connection_ping(intern->conn) == 0) {
                            /* Save connection parameters for potential reconnection */
                            intern->saved_host = estrndup(actual_host, host_len);
                            intern->saved_port = (uint16_t)port;
                            intern->saved_user = estrndup(user, user_len);
                            intern->saved_password = estrndup(password, password_len);
                            intern->saved_database = estrndup(database, database_len);
                            CLICKHOUSE_G(num_persistent)++;
                            CLICKHOUSE_G(num_links)++;
                            zend_string_release(hash_key);
                            return; /* Reusing existing connection */
                        } else {
                            /* Connection dead, close it and create new */
                            clickhouse_connection_close(intern->conn);
                            clickhouse_connection_free(intern->conn);
                            intern->conn = NULL;
                        }
                    }
                }
            } else {
                /* Create new persistent list entry */
                clickhouse_plist_entry *plist = malloc(sizeof(clickhouse_plist_entry));
                zend_ptr_stack_init_ex(&plist->free_connections, 1);
                zend_register_persistent_resource(
                    ZSTR_VAL(hash_key), ZSTR_LEN(hash_key), plist, le_pclickhouse);
            }

            zend_string_release(hash_key);
        }
    }

    /* Check max links */
    if (CLICKHOUSE_G(max_links) != -1 && CLICKHOUSE_G(num_links) >= CLICKHOUSE_G(max_links)) {
        zend_throw_exception(clickhouse_exception_ce,
            "Too many open connections", 0);
        return;
    }

    /* Check max persistent */
    if (persistent && CLICKHOUSE_G(max_persistent) != -1 &&
        CLICKHOUSE_G(num_persistent) >= CLICKHOUSE_G(max_persistent)) {
        php_error_docref(NULL, E_WARNING,
            "Too many open persistent connections. Downgrading to normal connection");
        persistent = 0;
        intern->persistent = 0;
        if (intern->hash_key) {
            zend_string_release(intern->hash_key);
            intern->hash_key = NULL;
        }
    }

    /* Create new connection */
    intern->conn = clickhouse_connection_create(actual_host, (uint16_t)port, user, password, database);
    if (!intern->conn) {
        zend_throw_exception(clickhouse_exception_ce, "Failed to create connection", 0);
        return;
    }

    int result = clickhouse_connection_connect(intern->conn);
    if (result != 0) {
        const char *error = clickhouse_connection_get_error(intern->conn);
        zend_throw_exception(clickhouse_exception_ce, error ? error : "Connection failed", 0);
        clickhouse_connection_free(intern->conn);
        intern->conn = NULL;
        return;
    }

    /* Save connection parameters for potential reconnection */
    intern->saved_host = estrndup(actual_host, host_len);
    intern->saved_port = (uint16_t)port;
    intern->saved_user = estrndup(user, user_len);
    intern->saved_password = estrndup(password, password_len);
    intern->saved_database = estrndup(database, database_len);

    CLICKHOUSE_G(num_links)++;
    if (persistent) {
        CLICKHOUSE_G(num_persistent)++;
    }
}
/* }}} */

/* Forward declaration for recursive Array conversion */
static void nested_value_to_zval(clickhouse_column *col, size_t index, clickhouse_type_info *type, zval *zv);

/* Forward declaration for parameter substitution */
static char *substitute_params(const char *query, clickhouse_params *params);

/* Helper: Convert PHP associative array to ClickHouse Map format */
static char *serialize_php_map_to_clickhouse(zval *arr) {
    smart_str result = {0};
    smart_str_appends(&result, "{");  /* ClickHouse Map literal syntax */

    HashTable *ht = Z_ARRVAL_P(arr);
    zval *val;
    zend_string *key;
    zend_ulong idx;
    int first = 1;

    ZEND_HASH_FOREACH_KEY_VAL(ht, idx, key, val) {
        if (!first) {
            smart_str_appendc(&result, ',');
        }
        first = 0;

        /* Key (always string) */
        smart_str_appendc(&result, '\'');
        if (key) {
            /* String key */
            for (const char *s = ZSTR_VAL(key); *s; s++) {
                if (*s == '\'') smart_str_appendc(&result, '\'');
                smart_str_appendc(&result, *s);
            }
        } else {
            /* Numeric key - convert to string */
            char buf[32];
            snprintf(buf, sizeof(buf), "%lu", idx);
            smart_str_appends(&result, buf);
        }
        smart_str_appendc(&result, '\'');
        smart_str_appendc(&result, ':');

        /* Value */
        switch (Z_TYPE_P(val)) {
            case IS_STRING:
                smart_str_appendc(&result, '\'');
                for (const char *s = Z_STRVAL_P(val); *s; s++) {
                    if (*s == '\'') smart_str_appendc(&result, '\'');
                    smart_str_appendc(&result, *s);
                }
                smart_str_appendc(&result, '\'');
                break;
            case IS_LONG:
                smart_str_append_long(&result, Z_LVAL_P(val));
                break;
            case IS_DOUBLE: {
                char buf[64];
                snprintf(buf, sizeof(buf), "%.16g", Z_DVAL_P(val));
                smart_str_appends(&result, buf);
                break;
            }
            case IS_TRUE:
                smart_str_appendc(&result, '1');
                break;
            case IS_FALSE:
                smart_str_appendc(&result, '0');
                break;
            default:
                smart_str_appends(&result, "''");
                break;
        }
    } ZEND_HASH_FOREACH_END();

    smart_str_appendc(&result, '}');
    smart_str_0(&result);

    char *ret = estrdup(ZSTR_VAL(result.s));
    smart_str_free(&result);
    return ret;
}

/* Helper: Convert PHP array to ClickHouse array string representation */
static char *serialize_php_array_to_clickhouse(zval *arr) {
    smart_str result = {0};
    smart_str_appendc(&result, '[');

    HashTable *ht = Z_ARRVAL_P(arr);
    zval *elem;
    int first = 1;

    ZEND_HASH_FOREACH_VAL(ht, elem) {
        if (!first) {
            smart_str_appendc(&result, ',');
        }
        first = 0;

        switch (Z_TYPE_P(elem)) {
            case IS_STRING:
                /* String elements need quotes and escaping */
                smart_str_appendc(&result, '\'');
                for (const char *s = Z_STRVAL_P(elem); *s; s++) {
                    if (*s == '\'') smart_str_appendc(&result, '\'');
                    smart_str_appendc(&result, *s);
                }
                smart_str_appendc(&result, '\'');
                break;
            case IS_LONG:
                smart_str_append_long(&result, Z_LVAL_P(elem));
                break;
            case IS_DOUBLE: {
                char buf[64];
                snprintf(buf, sizeof(buf), "%.16g", Z_DVAL_P(elem));
                smart_str_appends(&result, buf);
                break;
            }
            default:
                /* Convert to string */
                smart_str_appends(&result, "''");
                break;
        }
    } ZEND_HASH_FOREACH_END();

    smart_str_appendc(&result, ']');
    smart_str_0(&result);

    char *ret = estrdup(ZSTR_VAL(result.s));
    smart_str_free(&result);
    return ret;
}

/* Helper: Detect ClickHouse type from PHP zval */
static const char *detect_clickhouse_type_from_zval(zval *val) {
    switch (Z_TYPE_P(val)) {
        case IS_STRING: {
            const char *str = Z_STRVAL_P(val);
            size_t len = Z_STRLEN_P(val);

            /* UUID format: 8-4-4-4-12 (xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx) */
            if (len == 36 && str[8] == '-' && str[13] == '-' && str[18] == '-' && str[23] == '-') {
                int is_uuid = 1;
                for (size_t i = 0; i < len && is_uuid; i++) {
                    if (i == 8 || i == 13 || i == 18 || i == 23) continue;
                    char c = str[i];
                    if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F'))) {
                        is_uuid = 0;
                    }
                }
                if (is_uuid) return "UUID";
            }

            /* IPv4 format: xxx.xxx.xxx.xxx */
            if (len >= 7 && len <= 15) {
                int dots = 0;
                int is_ipv4 = 1;
                for (size_t i = 0; i < len; i++) {
                    if (str[i] == '.') dots++;
                    else if (str[i] < '0' || str[i] > '9') {
                        is_ipv4 = 0;
                        break;
                    }
                }
                if (is_ipv4 && dots == 3) return "IPv4";
            }

            /* IPv6 format: contains colons and hex digits */
            if (len >= 2 && len <= 39) {
                int colons = 0;
                int is_ipv6 = 1;
                for (size_t i = 0; i < len; i++) {
                    char c = str[i];
                    if (c == ':') colons++;
                    else if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F'))) {
                        is_ipv6 = 0;
                        break;
                    }
                }
                if (is_ipv6 && colons >= 2) return "IPv6";
            }

            /* DateTime format: YYYY-MM-DD HH:MM:SS or with milliseconds */
            if (len >= 19 && len <= 26) {
                if (str[4] == '-' && str[7] == '-' && str[10] == ' ' && str[13] == ':' && str[16] == ':') {
                    /* Check for microseconds: YYYY-MM-DD HH:MM:SS.ffffff */
                    if (len > 19 && str[19] == '.') {
                        return "DateTime64(6)";  /* Microsecond precision */
                    } else if (len > 19 && str[19] == '.') {
                        return "DateTime64(3)";  /* Millisecond precision */
                    }
                    return "DateTime";
                }
            }

            /* Date format: YYYY-MM-DD */
            if (len == 10 && str[4] == '-' && str[7] == '-') {
                int is_date = 1;
                for (size_t i = 0; i < len && is_date; i++) {
                    if (i == 4 || i == 7) continue;
                    if (str[i] < '0' || str[i] > '9') is_date = 0;
                }
                if (is_date) return "Date";
            }

            /* Decimal detection - numeric string with decimal point */
            /* Only detect if it looks like a decimal number: digits, optional sign, one decimal point */
            if (len >= 3 && len <= 40) {  /* Reasonable length for decimal */
                int has_decimal = 0;
                int has_digits = 0;
                int is_decimal = 1;
                size_t start = 0;

                /* Skip leading sign */
                if (str[0] == '-' || str[0] == '+') start = 1;

                for (size_t i = start; i < len && is_decimal; i++) {
                    if (str[i] == '.') {
                        if (has_decimal) {
                            /* Multiple decimal points */
                            is_decimal = 0;
                        }
                        has_decimal = 1;
                    } else if (str[i] >= '0' && str[i] <= '9') {
                        has_digits = 1;
                    } else {
                        /* Non-numeric character */
                        is_decimal = 0;
                    }
                }

                if (is_decimal && has_decimal && has_digits) {
                    /* Count decimal places to determine precision */
                    const char *decimal_point = strchr(str, '.');
                    if (decimal_point) {
                        size_t decimal_places = len - (decimal_point - str) - 1;
                        size_t total_digits = has_digits ? (len - start - 1) : 0;  /* Subtract decimal point */

                        /* Infer Decimal(P, S) where P is total precision and S is scale */
                        /* Use common patterns: Decimal(18,2) for currency, Decimal(10,4) for rates */
                        if (decimal_places <= 2 && total_digits <= 18) {
                            return "Decimal(18,2)";  /* Common for currency */
                        } else if (decimal_places <= 4 && total_digits <= 10) {
                            return "Decimal(10,4)";  /* Common for rates/percentages */
                        } else if (decimal_places <= 6 && total_digits <= 18) {
                            return "Decimal(18,6)";  /* High precision */
                        } else {
                            return "Decimal(38,9)";  /* Max precision */
                        }
                    }
                }
            }

            return "String";
        }
        case IS_LONG: {
            /* Auto-detect integer size based on value range */
            zend_long val_long = Z_LVAL_P(val);

            if (val_long >= 0) {
                /* Unsigned range detection */
                if (val_long <= 255) return "UInt8";
                if (val_long <= 65535) return "UInt16";
                if (val_long <= 4294967295L) return "UInt32";
                return "UInt64";
            } else {
                /* Signed range detection */
                if (val_long >= -128 && val_long <= 127) return "Int8";
                if (val_long >= -32768 && val_long <= 32767) return "Int16";
                if (val_long >= -2147483648L && val_long <= 2147483647L) return "Int32";
                return "Int64";
            }
        }
        case IS_DOUBLE:
            return "Float64";
        case IS_TRUE:
        case IS_FALSE:
            return "UInt8";
        case IS_NULL:
            return "Nullable(String)";
        case IS_ARRAY: {
            HashTable *ht = Z_ARRVAL_P(val);

            /* Check if it's an associative array (for JSON/Map) vs numeric array */
            int is_list = 1;
            int has_string_keys = 0;
            zend_ulong expected_idx = 0;
            zend_string *key;
            zend_ulong idx;

            ZEND_HASH_FOREACH_KEY(ht, idx, key) {
                if (key != NULL) {
                    /* String key found - it's associative */
                    is_list = 0;
                    has_string_keys = 1;
                    break;
                }
                if (idx != expected_idx) {
                    /* Non-sequential numeric keys - it's associative */
                    is_list = 0;
                    break;
                }
                expected_idx++;
            } ZEND_HASH_FOREACH_END();

            /* Associative array - try to detect as Map or fallback to JSON */
            if (!is_list && zend_hash_num_elements(ht) > 0) {
                /* Check if all values are of consistent type for Map detection */
                int value_type = -1;  /* -1 = unknown, 0 = mixed, >0 = consistent */
                int consistent = 1;
                int is_simple = 1;  /* Only simple types (not nested arrays/objects) */
                zval *map_val;

                ZEND_HASH_FOREACH_VAL(ht, map_val) {
                    int current_type = Z_TYPE_P(map_val);

                    /* Check if value is simple (not array/object) */
                    if (current_type == IS_ARRAY || current_type == IS_OBJECT) {
                        is_simple = 0;
                        break;
                    }

                    if (value_type == -1) {
                        value_type = current_type;
                    } else if (value_type != current_type) {
                        consistent = 0;
                        break;
                    }
                } ZEND_HASH_FOREACH_END();

                /* If values are consistent simple types, treat as Map */
                if (consistent && is_simple && value_type != -1) {
                    /* Determine value type for Map */
                    const char *map_value_type;
                    switch (value_type) {
                        case IS_LONG:
                            map_value_type = "Int64";
                            break;
                        case IS_DOUBLE:
                            map_value_type = "Float64";
                            break;
                        case IS_STRING:
                            map_value_type = "String";
                            break;
                        case IS_TRUE:
                        case IS_FALSE:
                            map_value_type = "UInt8";
                            break;
                        default:
                            /* Fallback to JSON for complex types */
                            return "String";
                    }

                    /* Return Map(String, ValueType) - keys are always strings in PHP arrays */
                    static char map_type[64];
                    snprintf(map_type, sizeof(map_type), "Map(String,%s)", map_value_type);
                    return map_type;
                }

                /* Mixed types or complex values - use JSON encoding */
                return "String";  /* Will be JSON-encoded */
            }

            /* Numeric array - detect element type */
            if (zend_hash_num_elements(ht) > 0) {
                zval *first_elem = NULL;
                ZEND_HASH_FOREACH_VAL(ht, first_elem) {
                    if (first_elem) {
                        switch (Z_TYPE_P(first_elem)) {
                            case IS_LONG:
                                return "Array(Int64)";
                            case IS_DOUBLE:
                                return "Array(Float64)";
                            case IS_STRING:
                                return "Array(String)";
                            default:
                                return "Array(String)";
                        }
                    }
                    break;
                } ZEND_HASH_FOREACH_END();
            }
            return "Array(String)";
        }
        default:
            return "String";
    }
}

/* Helper: Convert ClickHouse column value to PHP zval */
static void column_value_to_zval(clickhouse_column *col, size_t row, zval *zv) {
    clickhouse_type_info *type = col->type;

    /* Handle Nullable */
    if (type->type_id == CH_TYPE_NULLABLE) {
        if (col->nulls && col->nulls[row]) {
            ZVAL_NULL(zv);
            return;
        }
        type = type->nested;
    }

    switch (type->type_id) {
        case CH_TYPE_INT8: {
            int8_t *data = (int8_t *)col->data;
            ZVAL_LONG(zv, data[row]);
            break;
        }
        case CH_TYPE_INT16: {
            int16_t *data = (int16_t *)col->data;
            ZVAL_LONG(zv, data[row]);
            break;
        }
        case CH_TYPE_INT32: {
            int32_t *data = (int32_t *)col->data;
            ZVAL_LONG(zv, data[row]);
            break;
        }
        case CH_TYPE_INT64:
        case CH_TYPE_INTERVAL_NANOSECOND:
        case CH_TYPE_INTERVAL_MICROSECOND:
        case CH_TYPE_INTERVAL_MILLISECOND:
        case CH_TYPE_INTERVAL_SECOND:
        case CH_TYPE_INTERVAL_MINUTE:
        case CH_TYPE_INTERVAL_HOUR:
        case CH_TYPE_INTERVAL_DAY:
        case CH_TYPE_INTERVAL_WEEK:
        case CH_TYPE_INTERVAL_MONTH:
        case CH_TYPE_INTERVAL_QUARTER:
        case CH_TYPE_INTERVAL_YEAR: {
            int64_t *data = (int64_t *)col->data;
            ZVAL_LONG(zv, (zend_long)data[row]);
            break;
        }
        case CH_TYPE_UINT8: {
            uint8_t *data = (uint8_t *)col->data;
            ZVAL_LONG(zv, data[row]);
            break;
        }
        case CH_TYPE_BOOL: {
            uint8_t *data = (uint8_t *)col->data;
            ZVAL_BOOL(zv, data[row]);
            break;
        }
        case CH_TYPE_UINT16: {
            uint16_t *data = (uint16_t *)col->data;
            ZVAL_LONG(zv, data[row]);
            break;
        }
        case CH_TYPE_UINT32: {
            uint32_t *data = (uint32_t *)col->data;
            ZVAL_LONG(zv, data[row]);
            break;
        }
        case CH_TYPE_UINT64: {
            uint64_t *data = (uint64_t *)col->data;
            ZVAL_LONG(zv, (zend_long)data[row]);
            break;
        }
        case CH_TYPE_INT128: {
            /* 128-bit signed integer - convert to string using proper arithmetic */
            uint8_t *data = (uint8_t *)col->data + (row * 16);
            /* Check if negative (high bit of last byte in little-endian) */
            int is_negative = data[15] & 0x80;

            /* Work with a copy, potentially negated */
            uint8_t work[16];
            memcpy(work, data, 16);
            if (is_negative) {
                /* Two's complement to get absolute value */
                int carry = 1;
                for (int i = 0; i < 16; i++) {
                    int tmp = (~work[i] & 0xFF) + carry;
                    work[i] = tmp & 0xFF;
                    carry = tmp >> 8;
                }
            }

            /* Convert to decimal using repeated division by 10 */
            char digits[50];
            int digit_count = 0;

            /* Repeat until the number is zero */
            int is_zero;
            do {
                /* Divide work[] by 10, store remainder as next digit */
                uint32_t remainder = 0;
                for (int i = 15; i >= 0; i--) {
                    uint32_t val = (remainder << 8) | work[i];
                    work[i] = val / 10;
                    remainder = val % 10;
                }
                digits[digit_count++] = '0' + remainder;

                /* Check if number is now zero */
                is_zero = 1;
                for (int i = 0; i < 16; i++) {
                    if (work[i] != 0) { is_zero = 0; break; }
                }
            } while (!is_zero && digit_count < 49);

            /* Build result string (digits are in reverse order) */
            char result[51];
            int pos = 0;
            if (is_negative) result[pos++] = '-';
            for (int i = digit_count - 1; i >= 0; i--) {
                result[pos++] = digits[i];
            }
            result[pos] = '\0';
            ZVAL_STRING(zv, result);
            break;
        }
        case CH_TYPE_UINT128: {
            /* 128-bit unsigned integer - convert to string using proper arithmetic */
            uint8_t *data = (uint8_t *)col->data + (row * 16);
            uint8_t work[16];
            memcpy(work, data, 16);

            /* Convert to decimal using repeated division by 10 */
            char digits[50];
            int digit_count = 0;

            int is_zero;
            do {
                uint32_t remainder = 0;
                for (int i = 15; i >= 0; i--) {
                    uint32_t val = (remainder << 8) | work[i];
                    work[i] = val / 10;
                    remainder = val % 10;
                }
                digits[digit_count++] = '0' + remainder;

                is_zero = 1;
                for (int i = 0; i < 16; i++) {
                    if (work[i] != 0) { is_zero = 0; break; }
                }
            } while (!is_zero && digit_count < 49);

            /* Build result string (digits are in reverse order) */
            char result[50];
            int pos = 0;
            for (int i = digit_count - 1; i >= 0; i--) {
                result[pos++] = digits[i];
            }
            result[pos] = '\0';
            ZVAL_STRING(zv, result);
            break;
        }
        case CH_TYPE_INT256: {
            /* 256-bit signed integer - convert to string using proper arithmetic */
            uint8_t *data = (uint8_t *)col->data + (row * 32);
            int is_negative = data[31] & 0x80;

            uint8_t work[32];
            memcpy(work, data, 32);
            if (is_negative) {
                /* Two's complement to get absolute value */
                int carry = 1;
                for (int i = 0; i < 32; i++) {
                    int tmp = (~work[i] & 0xFF) + carry;
                    work[i] = tmp & 0xFF;
                    carry = tmp >> 8;
                }
            }

            /* Convert to decimal using repeated division by 10 */
            char digits[100];
            int digit_count = 0;

            int is_zero;
            do {
                uint32_t remainder = 0;
                for (int i = 31; i >= 0; i--) {
                    uint32_t val = (remainder << 8) | work[i];
                    work[i] = val / 10;
                    remainder = val % 10;
                }
                digits[digit_count++] = '0' + remainder;

                is_zero = 1;
                for (int i = 0; i < 32; i++) {
                    if (work[i] != 0) { is_zero = 0; break; }
                }
            } while (!is_zero && digit_count < 99);

            /* Build result string (digits are in reverse order) */
            char result[101];
            int pos = 0;
            if (is_negative) result[pos++] = '-';
            for (int i = digit_count - 1; i >= 0; i--) {
                result[pos++] = digits[i];
            }
            result[pos] = '\0';
            ZVAL_STRING(zv, result);
            break;
        }
        case CH_TYPE_UINT256: {
            /* 256-bit unsigned integer - convert to string using proper arithmetic */
            uint8_t *data = (uint8_t *)col->data + (row * 32);
            uint8_t work[32];
            memcpy(work, data, 32);

            /* Convert to decimal using repeated division by 10 */
            char digits[100];
            int digit_count = 0;

            int is_zero;
            do {
                uint32_t remainder = 0;
                for (int i = 31; i >= 0; i--) {
                    uint32_t val = (remainder << 8) | work[i];
                    work[i] = val / 10;
                    remainder = val % 10;
                }
                digits[digit_count++] = '0' + remainder;

                is_zero = 1;
                for (int i = 0; i < 32; i++) {
                    if (work[i] != 0) { is_zero = 0; break; }
                }
            } while (!is_zero && digit_count < 99);

            /* Build result string (digits are in reverse order) */
            char result[100];
            int pos = 0;
            for (int i = digit_count - 1; i >= 0; i--) {
                result[pos++] = digits[i];
            }
            result[pos] = '\0';
            ZVAL_STRING(zv, result);
            break;
        }
        case CH_TYPE_FLOAT32: {
            float *data = (float *)col->data;
            ZVAL_DOUBLE(zv, (double)data[row]);
            break;
        }
        case CH_TYPE_FLOAT64: {
            double *data = (double *)col->data;
            ZVAL_DOUBLE(zv, data[row]);
            break;
        }
        case CH_TYPE_BFLOAT16: {
            /* BFloat16: 16-bit brain float - same exponent as float32, truncated mantissa */
            uint16_t *data = (uint16_t *)col->data;
            uint32_t as_float32 = ((uint32_t)data[row]) << 16;
            float f;
            memcpy(&f, &as_float32, sizeof(float));
            ZVAL_DOUBLE(zv, (double)f);
            break;
        }
        case CH_TYPE_STRING: {
            char **strings = (char **)col->data;
            if (strings[row]) {
                ZVAL_STRING(zv, strings[row]);
            } else {
                ZVAL_EMPTY_STRING(zv);
            }
            break;
        }
        case CH_TYPE_FIXED_STRING: {
            size_t fixed_len = type->fixed_size;
            char *data = (char *)col->data + (row * fixed_len);
            ZVAL_STRINGL(zv, data, fixed_len);
            break;
        }
        case CH_TYPE_DATE: {
            /* Days since 1970-01-01 - convert to YYYY-MM-DD */
            uint16_t *data = (uint16_t *)col->data;
            uint16_t days = data[row];
            /* Calculate date from days since epoch */
            time_t ts = (time_t)days * 86400;
            struct tm *tm = gmtime(&ts);
            char buf[11];
            snprintf(buf, sizeof(buf), "%04d-%02d-%02d",
                tm->tm_year + 1900, tm->tm_mon + 1, tm->tm_mday);
            ZVAL_STRING(zv, buf);
            break;
        }
        case CH_TYPE_DATE32: {
            /* Days since 1970-01-01 (signed, supports dates before epoch) */
            int32_t *data = (int32_t *)col->data;
            int32_t days = data[row];
            time_t ts = (time_t)days * 86400;
            struct tm *tm = gmtime(&ts);
            char buf[11];
            snprintf(buf, sizeof(buf), "%04d-%02d-%02d",
                tm->tm_year + 1900, tm->tm_mon + 1, tm->tm_mday);
            ZVAL_STRING(zv, buf);
            break;
        }
        case CH_TYPE_DATETIME: {
            /* Unix timestamp - convert to YYYY-MM-DD HH:MM:SS */
            uint32_t *data = (uint32_t *)col->data;
            time_t ts = (time_t)data[row];
            struct tm *tm = gmtime(&ts);
            char buf[20];
            snprintf(buf, sizeof(buf), "%04d-%02d-%02d %02d:%02d:%02d",
                tm->tm_year + 1900, tm->tm_mon + 1, tm->tm_mday,
                tm->tm_hour, tm->tm_min, tm->tm_sec);
            ZVAL_STRING(zv, buf);
            break;
        }
        case CH_TYPE_UUID: {
            /* 16 bytes UUID - ClickHouse stores as two 64-bit values in big-endian */
            /* The format is: high 64 bits (bytes 0-7 reversed), low 64 bits (bytes 8-15 reversed) */
            uint8_t *data = (uint8_t *)col->data + (row * 16);
            char uuid_str[37];
            /* ClickHouse stores UUID with each 8-byte half in reverse order */
            snprintf(uuid_str, sizeof(uuid_str),
                "%02x%02x%02x%02x-%02x%02x-%02x%02x-%02x%02x-%02x%02x%02x%02x%02x%02x",
                data[7], data[6], data[5], data[4],
                data[3], data[2], data[1], data[0],
                data[15], data[14], data[13], data[12],
                data[11], data[10], data[9], data[8]);
            ZVAL_STRING(zv, uuid_str);
            break;
        }
        case CH_TYPE_IPV4: {
            /* 4 bytes IPv4 - stored as little-endian uint32 */
            uint8_t *data = (uint8_t *)col->data + (row * 4);
            char ipv4_str[16];
            /* ClickHouse stores IPv4 in little-endian, so reverse for display */
            snprintf(ipv4_str, sizeof(ipv4_str), "%u.%u.%u.%u",
                data[3], data[2], data[1], data[0]);
            ZVAL_STRING(zv, ipv4_str);
            break;
        }
        case CH_TYPE_IPV6: {
            /* 16 bytes IPv6 - format as hex string with colons */
            uint8_t *data = (uint8_t *)col->data + (row * 16);
            char ipv6_str[40];
            snprintf(ipv6_str, sizeof(ipv6_str),
                "%02x%02x:%02x%02x:%02x%02x:%02x%02x:%02x%02x:%02x%02x:%02x%02x:%02x%02x",
                data[0], data[1], data[2], data[3],
                data[4], data[5], data[6], data[7],
                data[8], data[9], data[10], data[11],
                data[12], data[13], data[14], data[15]);
            ZVAL_STRING(zv, ipv6_str);
            break;
        }
        case CH_TYPE_DATETIME64: {
            /* DateTime64 - stored as int64, scaled by precision */
            int64_t *data = (int64_t *)col->data;
            int64_t raw = data[row];
            size_t precision = type->fixed_size;
            int64_t divisor = 1;
            for (size_t i = 0; i < precision; i++) divisor *= 10;
            time_t ts = (time_t)(raw / divisor);
            int64_t frac = raw % divisor;
            if (frac < 0) frac = -frac; /* Handle negative timestamps */
            struct tm *tm = gmtime(&ts);
            char buf[32];
            if (precision > 0) {
                snprintf(buf, sizeof(buf), "%04d-%02d-%02d %02d:%02d:%02d.%0*lld",
                    tm->tm_year + 1900, tm->tm_mon + 1, tm->tm_mday,
                    tm->tm_hour, tm->tm_min, tm->tm_sec,
                    (int)precision, (long long)frac);
            } else {
                snprintf(buf, sizeof(buf), "%04d-%02d-%02d %02d:%02d:%02d",
                    tm->tm_year + 1900, tm->tm_mon + 1, tm->tm_mday,
                    tm->tm_hour, tm->tm_min, tm->tm_sec);
            }
            ZVAL_STRING(zv, buf);
            break;
        }
        case CH_TYPE_DECIMAL32: {
            /* Decimal32 - stored as int32 with scale */
            int32_t *data = (int32_t *)col->data;
            int32_t raw = data[row];
            size_t scale = col->type ? col->type->fixed_size : 0;
            if (scale > 0) {
                char buf[50];
                int is_neg = raw < 0;
                int64_t abs_val = is_neg ? -(int64_t)raw : (int64_t)raw;
                int64_t int_part = abs_val;
                for (size_t i = 0; i < scale; i++) int_part /= 10;
                int64_t frac_part = abs_val;
                for (size_t i = 0; i < scale; i++) frac_part %= (int64_t)pow(10, scale);
                /* Rebuild frac_part properly */
                int64_t divisor = 1;
                for (size_t i = 0; i < scale; i++) divisor *= 10;
                frac_part = abs_val % divisor;
                snprintf(buf, sizeof(buf), "%s%lld.%0*lld",
                    is_neg ? "-" : "", (long long)int_part, (int)scale, (long long)frac_part);
                ZVAL_STRING(zv, buf);
            } else {
                ZVAL_LONG(zv, raw);
            }
            break;
        }
        case CH_TYPE_DECIMAL64: {
            /* Decimal64 - stored as int64 with scale */
            int64_t *data = (int64_t *)col->data;
            int64_t raw = data[row];
            size_t scale = col->type ? col->type->fixed_size : 0;
            if (scale > 0) {
                char buf[50];
                int is_neg = raw < 0;
                uint64_t abs_val = is_neg ? (uint64_t)(-(raw + 1)) + 1 : (uint64_t)raw;
                uint64_t divisor = 1;
                for (size_t i = 0; i < scale; i++) divisor *= 10;
                uint64_t int_part = abs_val / divisor;
                uint64_t frac_part = abs_val % divisor;
                snprintf(buf, sizeof(buf), "%s%llu.%0*llu",
                    is_neg ? "-" : "", (unsigned long long)int_part, (int)scale, (unsigned long long)frac_part);
                ZVAL_STRING(zv, buf);
            } else {
                ZVAL_LONG(zv, (zend_long)raw);
            }
            break;
        }
        case CH_TYPE_DECIMAL128: {
            /* Decimal128 - stored as int128 with scale, convert to string */
            uint8_t *data = (uint8_t *)col->data + (row * 16);
            size_t scale = col->type ? col->type->fixed_size : 0;
            int is_negative = data[15] & 0x80;

            uint8_t work[16];
            memcpy(work, data, 16);
            if (is_negative) {
                int carry = 1;
                for (int i = 0; i < 16; i++) {
                    int tmp = (~work[i] & 0xFF) + carry;
                    work[i] = tmp & 0xFF;
                    carry = tmp >> 8;
                }
            }

            /* Convert to decimal string using division by 10 */
            char digits[50];
            int digit_count = 0;
            int is_zero;
            do {
                uint32_t remainder = 0;
                for (int i = 15; i >= 0; i--) {
                    uint32_t val = (remainder << 8) | work[i];
                    work[i] = val / 10;
                    remainder = val % 10;
                }
                digits[digit_count++] = '0' + remainder;
                is_zero = 1;
                for (int i = 0; i < 16; i++) {
                    if (work[i] != 0) { is_zero = 0; break; }
                }
            } while (!is_zero && digit_count < 49);

            /* Build result with decimal point */
            char result[60];
            int pos = 0;
            if (is_negative) result[pos++] = '-';

            if (scale > 0 && scale < (size_t)digit_count) {
                /* Insert decimal point */
                for (int i = digit_count - 1; i >= 0; i--) {
                    if (i == (int)scale - 1 && pos > (is_negative ? 1 : 0)) {
                        result[pos++] = '.';
                    }
                    result[pos++] = digits[i];
                }
            } else if (scale > 0 && scale >= (size_t)digit_count) {
                /* Need leading zeros after decimal */
                result[pos++] = '0';
                result[pos++] = '.';
                for (size_t i = digit_count; i < scale; i++) {
                    result[pos++] = '0';
                }
                for (int i = digit_count - 1; i >= 0; i--) {
                    result[pos++] = digits[i];
                }
            } else {
                for (int i = digit_count - 1; i >= 0; i--) {
                    result[pos++] = digits[i];
                }
            }
            result[pos] = '\0';
            ZVAL_STRING(zv, result);
            break;
        }
        case CH_TYPE_DECIMAL256: {
            /* Decimal256 - stored as int256 with scale, convert to string */
            uint8_t *data = (uint8_t *)col->data + (row * 32);
            size_t scale = col->type ? col->type->fixed_size : 0;
            int is_negative = data[31] & 0x80;

            uint8_t work[32];
            memcpy(work, data, 32);
            if (is_negative) {
                int carry = 1;
                for (int i = 0; i < 32; i++) {
                    int tmp = (~work[i] & 0xFF) + carry;
                    work[i] = tmp & 0xFF;
                    carry = tmp >> 8;
                }
            }

            /* Convert to decimal string using division by 10 */
            char digits[100];
            int digit_count = 0;
            int is_zero;
            do {
                uint32_t remainder = 0;
                for (int i = 31; i >= 0; i--) {
                    uint32_t val = (remainder << 8) | work[i];
                    work[i] = val / 10;
                    remainder = val % 10;
                }
                digits[digit_count++] = '0' + remainder;
                is_zero = 1;
                for (int i = 0; i < 32; i++) {
                    if (work[i] != 0) { is_zero = 0; break; }
                }
            } while (!is_zero && digit_count < 99);

            /* Build result with decimal point */
            char result[120];
            int pos = 0;
            if (is_negative) result[pos++] = '-';

            if (scale > 0 && scale < (size_t)digit_count) {
                for (int i = digit_count - 1; i >= 0; i--) {
                    if (i == (int)scale - 1 && pos > (is_negative ? 1 : 0)) {
                        result[pos++] = '.';
                    }
                    result[pos++] = digits[i];
                }
            } else if (scale > 0 && scale >= (size_t)digit_count) {
                result[pos++] = '0';
                result[pos++] = '.';
                for (size_t i = digit_count; i < scale; i++) {
                    result[pos++] = '0';
                }
                for (int i = digit_count - 1; i >= 0; i--) {
                    result[pos++] = digits[i];
                }
            } else {
                for (int i = digit_count - 1; i >= 0; i--) {
                    result[pos++] = digits[i];
                }
            }
            result[pos] = '\0';
            ZVAL_STRING(zv, result);
            break;
        }
        case CH_TYPE_ENUM8: {
            /* Enum8 - stored as int8, return string name if available */
            int8_t *data = (int8_t *)col->data;
            int8_t value = data[row];
            int found = 0;

            /* Look up enum string name */
            if (col->type && col->type->enum_values && col->type->enum_count > 0) {
                for (size_t i = 0; i < col->type->enum_count; i++) {
                    if (col->type->enum_values[i].value == value) {
                        ZVAL_STRING(zv, col->type->enum_values[i].name);
                        found = 1;
                        break;
                    }
                }
            }
            /* Fallback to numeric value if name not found */
            if (!found) {
                ZVAL_LONG(zv, value);
            }
            break;
        }
        case CH_TYPE_ENUM16: {
            /* Enum16 - stored as int16, return string name if available */
            int16_t *data = (int16_t *)col->data;
            int16_t value = data[row];
            int found = 0;

            /* Look up enum string name */
            if (col->type && col->type->enum_values && col->type->enum_count > 0) {
                for (size_t i = 0; i < col->type->enum_count; i++) {
                    if (col->type->enum_values[i].value == value) {
                        ZVAL_STRING(zv, col->type->enum_values[i].name);
                        found = 1;
                        break;
                    }
                }
            }
            /* Fallback to numeric value if name not found */
            if (!found) {
                ZVAL_LONG(zv, value);
            }
            break;
        }
        case CH_TYPE_ARRAY:
        case CH_TYPE_RING:
        case CH_TYPE_POLYGON:
        case CH_TYPE_MULTIPOLYGON: {
            /* Array/Ring/Polygon/MultiPolygon type - convert to PHP array */
            array_init(zv);

            if (col->offsets && col->nested_column && col->type->nested) {
                /* Calculate start and end indices for this row's array */
                size_t start = (row == 0) ? 0 : (size_t)col->offsets[row - 1];
                size_t end = (size_t)col->offsets[row];

                /* Add each element to the PHP array */
                for (size_t i = start; i < end; i++) {
                    zval elem;
                    nested_value_to_zval(col->nested_column, i, col->type->nested, &elem);
                    add_next_index_zval(zv, &elem);
                }
            }
            break;
        }
        case CH_TYPE_TUPLE:
        case CH_TYPE_POINT: {
            /* Tuple/Point type - convert to PHP array */
            array_init(zv);

            if (col->tuple_columns && col->tuple_column_count > 0) {
                for (size_t i = 0; i < col->tuple_column_count; i++) {
                    if (col->tuple_columns[i] && col->type->tuple_elements && col->type->tuple_elements[i]) {
                        zval elem;
                        /* Read from the tuple column at the same row index */
                        nested_value_to_zval(col->tuple_columns[i], row, col->type->tuple_elements[i], &elem);
                        add_next_index_zval(zv, &elem);
                    }
                }
            }
            break;
        }
        case CH_TYPE_MAP: {
            /* Map type - convert to PHP associative array */
            array_init(zv);

            if (col->offsets && col->tuple_columns && col->tuple_column_count == 2) {
                /* Calculate start and end indices for this row's map */
                size_t start = (row == 0) ? 0 : (size_t)col->offsets[row - 1];
                size_t end = (size_t)col->offsets[row];

                clickhouse_column *keys_col = col->tuple_columns[0];
                clickhouse_column *vals_col = col->tuple_columns[1];

                for (size_t i = start; i < end; i++) {
                    zval key, val;
                    nested_value_to_zval(keys_col, i, col->type->tuple_elements[0], &key);
                    nested_value_to_zval(vals_col, i, col->type->tuple_elements[1], &val);

                    /* Use key as array key - convert to string if needed */
                    if (Z_TYPE(key) == IS_LONG) {
                        add_index_zval(zv, Z_LVAL(key), &val);
                    } else {
                        zend_string *key_str = zval_get_string(&key);
                        add_assoc_zval(zv, ZSTR_VAL(key_str), &val);
                        zend_string_release(key_str);
                    }
                    zval_ptr_dtor(&key);
                }
            }
            break;
        }
        case CH_TYPE_LOWCARDINALITY: {
            /* LowCardinality - use dictionary index to get actual value */
            if (col->offsets && col->nested_column && col->type->nested) {
                size_t dict_index = (size_t)col->offsets[row];
                /* For LowCardinality(Nullable(T)), index 0 represents NULL */
                if (col->type->nested->type_id == CH_TYPE_NULLABLE && dict_index == 0) {
                    ZVAL_NULL(zv);
                } else {
                    /* Use the actual dictionary type (inner type for Nullable) */
                    clickhouse_type_info *dict_type = col->type->nested;
                    if (dict_type->type_id == CH_TYPE_NULLABLE && dict_type->nested) {
                        dict_type = dict_type->nested;
                    }
                    nested_value_to_zval(col->nested_column, dict_index, dict_type, zv);
                }
            } else {
                ZVAL_NULL(zv);
            }
            break;
        }
        case CH_TYPE_SIMPLEAGGREGATEFUNCTION: {
            /* SimpleAggregateFunction - delegate to nested column */
            if (col->nested_column && col->type->nested) {
                nested_value_to_zval(col->nested_column, row, col->type->nested, zv);
            } else {
                ZVAL_NULL(zv);
            }
            break;
        }
        case CH_TYPE_JSON:
        case CH_TYPE_OBJECT: {
            /* JSON/Object - parse JSON string to PHP array/object */
            char **strings = (char **)col->data;
            if (strings && strings[row]) {
                size_t len = strlen(strings[row]);
                php_json_decode_ex(zv, strings[row], len, PHP_JSON_OBJECT_AS_ARRAY, 512);
                /* If JSON parsing failed, return as string */
                if (Z_TYPE_P(zv) == IS_NULL && len > 0) {
                    ZVAL_STRING(zv, strings[row]);
                }
            } else {
                ZVAL_NULL(zv);
            }
            break;
        }
        case CH_TYPE_DYNAMIC: {
            /* Dynamic - stored as string, return as string (type info not available) */
            char **strings = (char **)col->data;
            if (strings && strings[row]) {
                ZVAL_STRING(zv, strings[row]);
            } else {
                ZVAL_NULL(zv);
            }
            break;
        }
        case CH_TYPE_VARIANT: {
            /* Variant - discriminator determines actual type */
            if (!col->discriminators || !col->tuple_columns) {
                ZVAL_NULL(zv);
                break;
            }

            uint8_t discrim = col->discriminators[row];
            if (discrim == 0xFF) {
                /* NULL value */
                ZVAL_NULL(zv);
            } else if (discrim < col->tuple_column_count && col->tuple_columns[discrim]) {
                /* Get the index within this variant type's data */
                size_t type_index = 0;
                for (size_t i = 0; i < row; i++) {
                    if (col->discriminators[i] == discrim) {
                        type_index++;
                    }
                }
                nested_value_to_zval(col->tuple_columns[discrim], type_index,
                                     col->type->tuple_elements[discrim], zv);
            } else {
                ZVAL_NULL(zv);
            }
            break;
        }
        default:
            ZVAL_NULL(zv);
            break;
    }
}

/* Helper: Convert nested column value to zval (for Array elements) */
static void nested_value_to_zval(clickhouse_column *col, size_t index, clickhouse_type_info *type, zval *zv) {
    if (!col || !type) {
        ZVAL_NULL(zv);
        return;
    }

    switch (type->type_id) {
        case CH_TYPE_INT8: {
            int8_t *data = (int8_t *)col->data;
            ZVAL_LONG(zv, data[index]);
            break;
        }
        case CH_TYPE_INT16: {
            int16_t *data = (int16_t *)col->data;
            ZVAL_LONG(zv, data[index]);
            break;
        }
        case CH_TYPE_INT32: {
            int32_t *data = (int32_t *)col->data;
            ZVAL_LONG(zv, data[index]);
            break;
        }
        case CH_TYPE_INT64:
        case CH_TYPE_INTERVAL_NANOSECOND:
        case CH_TYPE_INTERVAL_MICROSECOND:
        case CH_TYPE_INTERVAL_MILLISECOND:
        case CH_TYPE_INTERVAL_SECOND:
        case CH_TYPE_INTERVAL_MINUTE:
        case CH_TYPE_INTERVAL_HOUR:
        case CH_TYPE_INTERVAL_DAY:
        case CH_TYPE_INTERVAL_WEEK:
        case CH_TYPE_INTERVAL_MONTH:
        case CH_TYPE_INTERVAL_QUARTER:
        case CH_TYPE_INTERVAL_YEAR: {
            int64_t *data = (int64_t *)col->data;
            ZVAL_LONG(zv, (zend_long)data[index]);
            break;
        }
        case CH_TYPE_UINT8: {
            uint8_t *data = (uint8_t *)col->data;
            ZVAL_LONG(zv, data[index]);
            break;
        }
        case CH_TYPE_BOOL: {
            uint8_t *data = (uint8_t *)col->data;
            ZVAL_BOOL(zv, data[index]);
            break;
        }
        case CH_TYPE_UINT16: {
            uint16_t *data = (uint16_t *)col->data;
            ZVAL_LONG(zv, data[index]);
            break;
        }
        case CH_TYPE_UINT32: {
            uint32_t *data = (uint32_t *)col->data;
            ZVAL_LONG(zv, data[index]);
            break;
        }
        case CH_TYPE_UINT64: {
            uint64_t *data = (uint64_t *)col->data;
            ZVAL_LONG(zv, (zend_long)data[index]);
            break;
        }
        case CH_TYPE_INT128:
        case CH_TYPE_UINT128: {
            /* 128-bit integer - convert to string using proper arithmetic */
            uint8_t *data = (uint8_t *)col->data + (index * 16);
            int is_signed = (type->type_id == CH_TYPE_INT128);
            int is_negative = is_signed && (data[15] & 0x80);
            uint8_t work[16];
            memcpy(work, data, 16);
            if (is_negative) {
                int carry = 1;
                for (int i = 0; i < 16; i++) {
                    int tmp = (~work[i] & 0xFF) + carry;
                    work[i] = tmp & 0xFF;
                    carry = tmp >> 8;
                }
            }
            /* Convert to decimal using repeated division by 10 */
            char digits[50];
            int digit_count = 0;
            int is_zero;
            do {
                uint32_t remainder = 0;
                for (int i = 15; i >= 0; i--) {
                    uint32_t val = (remainder << 8) | work[i];
                    work[i] = val / 10;
                    remainder = val % 10;
                }
                digits[digit_count++] = '0' + remainder;
                is_zero = 1;
                for (int i = 0; i < 16; i++) {
                    if (work[i] != 0) { is_zero = 0; break; }
                }
            } while (!is_zero && digit_count < 49);
            /* Build result string (digits are in reverse order) */
            char result[51];
            int pos = 0;
            if (is_negative) result[pos++] = '-';
            for (int i = digit_count - 1; i >= 0; i--) {
                result[pos++] = digits[i];
            }
            result[pos] = '\0';
            ZVAL_STRING(zv, result);
            break;
        }
        case CH_TYPE_INT256:
        case CH_TYPE_UINT256: {
            /* 256-bit integer - convert to string using proper arithmetic */
            uint8_t *data = (uint8_t *)col->data + (index * 32);
            int is_signed = (type->type_id == CH_TYPE_INT256);
            int is_negative = is_signed && (data[31] & 0x80);
            uint8_t work[32];
            memcpy(work, data, 32);
            if (is_negative) {
                int carry = 1;
                for (int i = 0; i < 32; i++) {
                    int tmp = (~work[i] & 0xFF) + carry;
                    work[i] = tmp & 0xFF;
                    carry = tmp >> 8;
                }
            }
            /* Convert to decimal using repeated division by 10 */
            char digits[100];
            int digit_count = 0;
            int is_zero;
            do {
                uint32_t remainder = 0;
                for (int i = 31; i >= 0; i--) {
                    uint32_t val = (remainder << 8) | work[i];
                    work[i] = val / 10;
                    remainder = val % 10;
                }
                digits[digit_count++] = '0' + remainder;
                is_zero = 1;
                for (int i = 0; i < 32; i++) {
                    if (work[i] != 0) { is_zero = 0; break; }
                }
            } while (!is_zero && digit_count < 99);
            /* Build result string (digits are in reverse order) */
            char result[101];
            int pos = 0;
            if (is_negative) result[pos++] = '-';
            for (int i = digit_count - 1; i >= 0; i--) {
                result[pos++] = digits[i];
            }
            result[pos] = '\0';
            ZVAL_STRING(zv, result);
            break;
        }
        case CH_TYPE_FLOAT32: {
            float *data = (float *)col->data;
            ZVAL_DOUBLE(zv, (double)data[index]);
            break;
        }
        case CH_TYPE_FLOAT64: {
            double *data = (double *)col->data;
            ZVAL_DOUBLE(zv, data[index]);
            break;
        }
        case CH_TYPE_BFLOAT16: {
            /* BFloat16: 16-bit brain float - same exponent as float32, truncated mantissa */
            uint16_t *data = (uint16_t *)col->data;
            uint32_t as_float32 = ((uint32_t)data[index]) << 16;
            float f;
            memcpy(&f, &as_float32, sizeof(float));
            ZVAL_DOUBLE(zv, (double)f);
            break;
        }
        case CH_TYPE_STRING: {
            char **strings = (char **)col->data;
            if (strings[index]) {
                ZVAL_STRING(zv, strings[index]);
            } else {
                ZVAL_EMPTY_STRING(zv);
            }
            break;
        }
        case CH_TYPE_DATE: {
            uint16_t *data = (uint16_t *)col->data;
            ZVAL_LONG(zv, data[index]);
            break;
        }
        case CH_TYPE_DATETIME: {
            uint32_t *data = (uint32_t *)col->data;
            ZVAL_LONG(zv, data[index]);
            break;
        }
        case CH_TYPE_UUID: {
            uint8_t *data = (uint8_t *)col->data + (index * 16);
            char uuid_str[37];
            snprintf(uuid_str, sizeof(uuid_str),
                "%02x%02x%02x%02x-%02x%02x-%02x%02x-%02x%02x-%02x%02x%02x%02x%02x%02x",
                data[7], data[6], data[5], data[4],
                data[3], data[2], data[1], data[0],
                data[15], data[14], data[13], data[12],
                data[11], data[10], data[9], data[8]);
            ZVAL_STRING(zv, uuid_str);
            break;
        }
        case CH_TYPE_IPV4: {
            uint8_t *data = (uint8_t *)col->data + (index * 4);
            char ipv4_str[16];
            snprintf(ipv4_str, sizeof(ipv4_str), "%u.%u.%u.%u",
                data[3], data[2], data[1], data[0]);
            ZVAL_STRING(zv, ipv4_str);
            break;
        }
        case CH_TYPE_IPV6: {
            uint8_t *data = (uint8_t *)col->data + (index * 16);
            char ipv6_str[40];
            snprintf(ipv6_str, sizeof(ipv6_str),
                "%02x%02x:%02x%02x:%02x%02x:%02x%02x:%02x%02x:%02x%02x:%02x%02x:%02x%02x",
                data[0], data[1], data[2], data[3],
                data[4], data[5], data[6], data[7],
                data[8], data[9], data[10], data[11],
                data[12], data[13], data[14], data[15]);
            ZVAL_STRING(zv, ipv6_str);
            break;
        }
        case CH_TYPE_ENUM8: {
            /* Enum8 - stored as int8, return string name if available */
            int8_t *data = (int8_t *)col->data;
            int8_t value = data[index];
            int found = 0;

            /* Look up enum string name */
            if (type->enum_values && type->enum_count > 0) {
                for (size_t i = 0; i < type->enum_count; i++) {
                    if (type->enum_values[i].value == value) {
                        ZVAL_STRING(zv, type->enum_values[i].name);
                        found = 1;
                        break;
                    }
                }
            }
            /* Fallback to numeric value if name not found */
            if (!found) {
                ZVAL_LONG(zv, value);
            }
            break;
        }
        case CH_TYPE_ENUM16: {
            /* Enum16 - stored as int16, return string name if available */
            int16_t *data = (int16_t *)col->data;
            int16_t value = data[index];
            int found = 0;

            /* Look up enum string name */
            if (type->enum_values && type->enum_count > 0) {
                for (size_t i = 0; i < type->enum_count; i++) {
                    if (type->enum_values[i].value == value) {
                        ZVAL_STRING(zv, type->enum_values[i].name);
                        found = 1;
                        break;
                    }
                }
            }
            /* Fallback to numeric value if name not found */
            if (!found) {
                ZVAL_LONG(zv, value);
            }
            break;
        }
        case CH_TYPE_POINT: {
            /* Point - Tuple(Float64, Float64) as nested column */
            array_init(zv);
            if (col->tuple_columns && col->tuple_column_count >= 2) {
                for (size_t i = 0; i < col->tuple_column_count; i++) {
                    if (col->tuple_columns[i] && type->tuple_elements && type->tuple_elements[i]) {
                        zval elem;
                        nested_value_to_zval(col->tuple_columns[i], index, type->tuple_elements[i], &elem);
                        add_next_index_zval(zv, &elem);
                    }
                }
            }
            break;
        }
        case CH_TYPE_ARRAY:
        case CH_TYPE_RING:
        case CH_TYPE_POLYGON:
        case CH_TYPE_MULTIPOLYGON: {
            /* Array/Ring/Polygon/MultiPolygon - Array of nested type */
            array_init(zv);
            if (col->offsets && col->nested_column && type->nested) {
                size_t start = (index == 0) ? 0 : (size_t)col->offsets[index - 1];
                size_t end = (size_t)col->offsets[index];
                for (size_t i = start; i < end; i++) {
                    zval elem;
                    nested_value_to_zval(col->nested_column, i, type->nested, &elem);
                    add_next_index_zval(zv, &elem);
                }
            }
            break;
        }
        case CH_TYPE_TUPLE: {
            /* Tuple - array of different typed elements */
            array_init(zv);
            if (col->tuple_columns && col->tuple_column_count > 0) {
                for (size_t i = 0; i < col->tuple_column_count; i++) {
                    if (col->tuple_columns[i] && type->tuple_elements && type->tuple_elements[i]) {
                        zval elem;
                        nested_value_to_zval(col->tuple_columns[i], index, type->tuple_elements[i], &elem);
                        add_next_index_zval(zv, &elem);
                    }
                }
            }
            break;
        }
        case CH_TYPE_MAP: {
            /* Map type - convert to PHP associative array */
            array_init(zv);
            if (col->offsets && col->tuple_columns && col->tuple_column_count == 2) {
                size_t start = (index == 0) ? 0 : (size_t)col->offsets[index - 1];
                size_t end = (size_t)col->offsets[index];
                clickhouse_column *keys_col = col->tuple_columns[0];
                clickhouse_column *vals_col = col->tuple_columns[1];
                for (size_t i = start; i < end; i++) {
                    zval key, val;
                    nested_value_to_zval(keys_col, i, type->tuple_elements[0], &key);
                    nested_value_to_zval(vals_col, i, type->tuple_elements[1], &val);
                    if (Z_TYPE(key) == IS_STRING) {
                        add_assoc_zval_ex(zv, Z_STRVAL(key), Z_STRLEN(key), &val);
                        zval_ptr_dtor(&key);
                    } else if (Z_TYPE(key) == IS_LONG) {
                        add_index_zval(zv, Z_LVAL(key), &val);
                    } else {
                        zend_string *str = zval_get_string(&key);
                        add_assoc_zval_ex(zv, ZSTR_VAL(str), ZSTR_LEN(str), &val);
                        zend_string_release(str);
                        zval_ptr_dtor(&key);
                    }
                }
            }
            break;
        }
        default:
            ZVAL_NULL(zv);
            break;
    }
}

/* Helper: Convert a single block to PHP array */
static void block_to_php_array(clickhouse_block *block, zval *return_value) {
    array_init(return_value);
    if (!block) return;

    for (size_t row = 0; row < block->row_count; row++) {
        zval row_array;
        array_init(&row_array);

        for (size_t c = 0; c < block->column_count; c++) {
            clickhouse_column *col = block->columns[c];
            zval cell;
            column_value_to_zval(col, row, &cell);
            add_assoc_zval(&row_array, col->name, &cell);
        }

        add_next_index_zval(return_value, &row_array);
    }
}

/* Helper: Convert result blocks to PHP array */
static void result_to_php_array(clickhouse_result *result, zval *return_value) {
    array_init(return_value);

    /* Defensive null check */
    if (!result) {
        return;
    }

    for (size_t b = 0; b < result->block_count; b++) {
        clickhouse_block *block = result->blocks[b];

        for (size_t row = 0; row < block->row_count; row++) {
            zval row_array;
            array_init(&row_array);

            for (size_t c = 0; c < block->column_count; c++) {
                clickhouse_column *col = block->columns[c];
                zval cell;
                column_value_to_zval(col, row, &cell);
                add_assoc_zval(&row_array, col->name, &cell);
            }

            add_next_index_zval(return_value, &row_array);
        }
    }
}

/* Helper: Check if error indicates connection loss */
static int is_connection_error(const char *error) {
    if (!error) return 0;
    return (strstr(error, "Connection closed") != NULL ||
            strstr(error, "Not connected") != NULL ||
            strstr(error, "Broken pipe") != NULL ||
            strstr(error, "Connection reset") != NULL ||
            strstr(error, "Connection refused") != NULL ||
            strstr(error, "Network is unreachable") != NULL);
}

/* Helper: Calculate retry delay with exponential backoff and optional jitter */
static double calculate_retry_delay(clickhouse_client_object *intern, int attempt) {
    /* Calculate exponential backoff: base_delay * 2^attempt */
    double delay = intern->retry_base_delay * (1 << attempt);

    /* Cap at max_delay */
    if (delay > intern->retry_max_delay) {
        delay = intern->retry_max_delay;
    }

    /* Add jitter if enabled (randomize between 50% and 100% of delay) */
    if (intern->retry_jitter) {
        /* Simple jitter: delay * (0.5 + random(0.5)) */
        double jitter_factor = 0.5 + ((double)rand() / RAND_MAX) * 0.5;
        delay *= jitter_factor;
    }

    return delay;
}

/* Helper: Sleep for specified seconds (with microsecond precision) */
static void retry_sleep(double seconds) {
    if (seconds > 0) {
        struct timeval tv;
        tv.tv_sec = (time_t)seconds;
        tv.tv_usec = (suseconds_t)((seconds - tv.tv_sec) * 1000000);
        select(0, NULL, NULL, NULL, &tv);
    }
}

/* Helper: Attempt to reconnect with retry logic */
static int attempt_reconnect(clickhouse_client_object *intern) {
    /* Check if we have saved connection parameters */
    if (!intern->saved_host || !intern->saved_user ||
        !intern->saved_password || !intern->saved_database) {
        return 0;
    }

    /* Determine max attempts (0 = unlimited, but cap at 10 for safety) */
    zend_long max_attempts = intern->max_retry_attempts > 0 ? intern->max_retry_attempts : 10;

    for (zend_long attempt = 0; attempt < max_attempts; attempt++) {
        /* Add delay before retry (except for first attempt) */
        if (attempt > 0) {
            double delay = calculate_retry_delay(intern, attempt - 1);
            retry_sleep(delay);
        }

        /* Close existing connection if any */
        if (intern->conn) {
            clickhouse_connection_close(intern->conn);
            clickhouse_connection_free(intern->conn);
            intern->conn = NULL;
        }

        /* Create new connection with saved parameters */
        intern->conn = clickhouse_connection_create(
            intern->saved_host,
            intern->saved_port,
            intern->saved_user,
            intern->saved_password,
            intern->saved_database
        );

        if (!intern->conn) {
            intern->total_retry_attempts++;
            continue;  /* Try next attempt */
        }

        int result = clickhouse_connection_connect(intern->conn);
        if (result == 0) {
            /* Success! */
            if (attempt > 0) {
                intern->total_retry_attempts += attempt;
            }
            return 1;
        }

        /* Connection failed */
        clickhouse_connection_free(intern->conn);
        intern->conn = NULL;
        intern->total_retry_attempts++;
    }

    /* All attempts exhausted */
    return 0;
}

/* Helper: Update metrics after query execution */
static void update_query_metrics(clickhouse_client_object *intern, double query_time, clickhouse_result *result, int success) {
    if (!intern->metrics_enabled) {
        return;
    }

    if (success) {
        intern->queries_executed++;
        intern->total_query_time += query_time;

        /* Track rows and bytes from result profile if available */
        if (result) {
            intern->total_rows_read += result->profile.rows;
            intern->total_bytes_read += result->profile.bytes;
        }

        /* Check if this is a slow query */
        if (intern->slow_query_threshold > 0 && query_time >= intern->slow_query_threshold) {
            intern->slow_queries++;
        }
    } else {
        intern->queries_failed++;
    }
}

/* Helper: Build detailed error message with query context */
static void throw_query_error(const char *error, const char *original_query, const char *final_query) {
    smart_str error_msg = {0};

    /* Add the original error message */
    smart_str_appends(&error_msg, error ? error : "Query failed");

    /* Add query context if available */
    if (original_query || final_query) {
        smart_str_appends(&error_msg, "\n\n");
        smart_str_appends(&error_msg, "Query Context:\n");

        if (original_query && final_query && strcmp(original_query, final_query) != 0) {
            /* Show both original and final query if they differ (parameters were substituted) */
            smart_str_appends(&error_msg, "  Original: ");
            smart_str_appends(&error_msg, original_query);
            smart_str_appends(&error_msg, "\n");
            smart_str_appends(&error_msg, "  Executed: ");
            smart_str_appends(&error_msg, final_query);
        } else {
            /* Show just the query if no substitution occurred */
            smart_str_appends(&error_msg, "  Query: ");
            smart_str_appends(&error_msg, final_query ? final_query : original_query);
        }
    }

    smart_str_0(&error_msg);
    zend_throw_exception(clickhouse_exception_ce, ZSTR_VAL(error_msg.s), 0);
    smart_str_free(&error_msg);
}

/* Helper: Build detailed error message for parameter substitution */
static void throw_param_error(const char *param_name, const char *query, size_t param_position, const char *message) {
    smart_str error_msg = {0};

    /* Build error message with context */
    smart_str_appends(&error_msg, "Parameter Error: ");
    smart_str_appends(&error_msg, message);
    smart_str_appends(&error_msg, "\n");

    /* Add parameter details */
    smart_str_appends(&error_msg, "  Parameter: '");
    smart_str_appends(&error_msg, param_name);
    smart_str_appends(&error_msg, "' at position ");
    char pos_str[32];
    snprintf(pos_str, sizeof(pos_str), "%zu", param_position);
    smart_str_appends(&error_msg, pos_str);
    smart_str_appends(&error_msg, "\n");

    /* Add query context - show the query with error location */
    smart_str_appends(&error_msg, "  Query: ");
    smart_str_appends(&error_msg, query);
    smart_str_appends(&error_msg, "\n");

    /* Add pointer to error location */
    smart_str_appends(&error_msg, "         ");
    for (size_t i = 0; i < param_position; i++) {
        smart_str_appendc(&error_msg, ' ');
    }
    smart_str_appends(&error_msg, "^");

    smart_str_0(&error_msg);
    zend_throw_exception(clickhouse_exception_ce, ZSTR_VAL(error_msg.s), 0);
    smart_str_free(&error_msg);
}

/* Bridge function to call PHP log callback from C */
static void php_log_callback_bridge(clickhouse_log_entry *entry, void *user_data) {
    clickhouse_client_object *intern = (clickhouse_client_object *)user_data;

    if (!intern || Z_ISUNDEF(intern->log_callback)) {
        return;
    }

    zval args[6], retval;
    ZVAL_LONG(&args[0], entry->time);
    ZVAL_LONG(&args[1], entry->time_microseconds);
    ZVAL_LONG(&args[2], entry->thread_id);
    ZVAL_LONG(&args[3], entry->priority);
    ZVAL_STRING(&args[4], entry->source ? entry->source : "");
    ZVAL_STRING(&args[5], entry->text ? entry->text : "");

    if (call_user_function(NULL, NULL, &intern->log_callback, &retval, 6, args) == SUCCESS) {
        zval_ptr_dtor(&retval);
    }

    /* Clean up zval strings */
    zval_ptr_dtor(&args[4]);
    zval_ptr_dtor(&args[5]);
}

/* Helper to check if a query is a write operation */
static int is_write_query(const char *sql) {
    /* Skip leading whitespace and comments */
    while (*sql && (*sql == ' ' || *sql == '\t' || *sql == '\n' || *sql == '\r')) {
        sql++;
    }

    /* Check for write operations (case-insensitive) */
    if (strncasecmp(sql, "INSERT", 6) == 0 ||
        strncasecmp(sql, "CREATE", 6) == 0 ||
        strncasecmp(sql, "DROP", 4) == 0 ||
        strncasecmp(sql, "ALTER", 5) == 0 ||
        strncasecmp(sql, "TRUNCATE", 8) == 0 ||
        strncasecmp(sql, "RENAME", 6) == 0 ||
        strncasecmp(sql, "OPTIMIZE", 8) == 0 ||
        strncasecmp(sql, "SYSTEM", 6) == 0) {
        return 1;
    }

    return 0;
}

/* {{{ proto array ClickHouse\Client::query(string $sql) */
PHP_METHOD(ClickHouse_Client, query) {
    char *sql;
    size_t sql_len;

    ZEND_PARSE_PARAMETERS_START(1, 1)
        Z_PARAM_STRING(sql, sql_len)
    ZEND_PARSE_PARAMETERS_END();

    clickhouse_client_object *intern = Z_CLICKHOUSE_CLIENT_P(ZEND_THIS);

    if (!intern->conn) {
        zend_throw_exception(clickhouse_exception_ce, "Not connected", 0);
        return;
    }

    /* Check read-only mode */
    if (intern->readonly && is_write_query(sql)) {
        zend_throw_exception(clickhouse_exception_ce, "Write operations not allowed in read-only mode", 0);
        return;
    }

    clickhouse_result *result = NULL;
    int status;
    int retry_attempted = 0;
    double start_time = 0, query_time = 0;

    /* Start timing if metrics are enabled */
    if (intern->metrics_enabled) {
        struct timeval tv;
        gettimeofday(&tv, NULL);
        start_time = tv.tv_sec + tv.tv_usec / 1000000.0;
    }

retry_query:
    /* Apply query timeout if set */
    if (intern->query_timeout_ms > 0) {
        clickhouse_connection_set_query_timeout_ms(intern->conn, (int)intern->query_timeout_ms);
    }

    /* Use extended query when compression, session, query ID, query settings, or callbacks are set */
    if (intern->compression != CH_COMPRESS_NONE || intern->session_id || intern->default_query_id ||
        intern->query_settings || !Z_ISUNDEF(intern->log_callback)) {
        clickhouse_query_options *opts = clickhouse_query_options_create();
        if (opts) {
            opts->compression = intern->compression;
            if (intern->session_id) {
                opts->session_id = intern->session_id;
            }
            if (intern->default_query_id) {
                opts->query_id = intern->default_query_id;
            }
            /* Copy query settings from client to options */
            if (intern->query_settings) {
                clickhouse_setting *setting = intern->query_settings->head;
                while (setting) {
                    clickhouse_query_options_set_setting(opts, setting->name, setting->value);
                    setting = setting->next;
                }
            }
            /* Set up log callback if present */
            if (!Z_ISUNDEF(intern->log_callback)) {
                opts->log_callback = php_log_callback_bridge;
                opts->log_user_data = intern;
            }
            status = clickhouse_connection_execute_query_ext(intern->conn, sql, opts, &result);
            /* Don't free session_id/query_id as they're borrowed from intern */
            opts->session_id = NULL;
            opts->query_id = NULL;
            clickhouse_query_options_free(opts);
        } else {
            status = clickhouse_connection_execute_query(intern->conn, sql, &result);
        }
    } else {
        status = clickhouse_connection_execute_query(intern->conn, sql, &result);
    }

    /* Reset timeout to default if it was set */
    if (intern->query_timeout_ms > 0) {
        clickhouse_connection_set_query_timeout_ms(intern->conn, 0);
    }

    if (status != 0) {
        const char *error = clickhouse_connection_get_error(intern->conn);

        /* Check if auto-reconnect is enabled and this is a connection error */
        if (intern->auto_reconnect && !retry_attempted && is_connection_error(error)) {
            if (result) {
                clickhouse_result_free(result);
                result = NULL;
            }
            /* Attempt to reconnect and retry once */
            if (attempt_reconnect(intern)) {
                retry_attempted = 1;
                goto retry_query;
            }
        }

        if (result) {
            clickhouse_result_free(result);
        }
        /* Update metrics for failed query */
        if (intern->metrics_enabled) {
            struct timeval tv;
            gettimeofday(&tv, NULL);
            query_time = (tv.tv_sec + tv.tv_usec / 1000000.0) - start_time;
            update_query_metrics(intern, query_time, NULL, 0);
        }
        /* Use enhanced error message with query context */
        throw_query_error(error, sql, sql);
        return;
    }

    /* Calculate query time if metrics enabled */
    if (intern->metrics_enabled) {
        struct timeval tv;
        gettimeofday(&tv, NULL);
        query_time = (tv.tv_sec + tv.tv_usec / 1000000.0) - start_time;
        update_query_metrics(intern, query_time, result, 1);
    }

    /* Store query ID from result for tracking */
    if (intern->last_query_id) {
        efree(intern->last_query_id);
        intern->last_query_id = NULL;
    }
    if (result->query_id) {
        intern->last_query_id = estrdup(result->query_id);
    }

    /* Call profile callback if set and profile data is available */
    if (!Z_ISUNDEF(intern->profile_callback)) {
        zval args[6], retval;
        ZVAL_LONG(&args[0], result->profile.rows);
        ZVAL_LONG(&args[1], result->profile.blocks);
        ZVAL_LONG(&args[2], result->profile.bytes);
        ZVAL_BOOL(&args[3], result->profile.applied_limit);
        ZVAL_LONG(&args[4], result->profile.rows_before_limit);
        ZVAL_BOOL(&args[5], result->profile.calculated_rows_before_limit);

        if (call_user_function(NULL, NULL, &intern->profile_callback, &retval, 6, args) == SUCCESS) {
            zval_ptr_dtor(&retval);
        }
    }

    result_to_php_array(result, return_value);
    clickhouse_result_free(result);
}
/* }}} */

/* {{{ proto array ClickHouse\Client::execute(string $sql [, array $params]) */
PHP_METHOD(ClickHouse_Client, execute) {
    char *sql;
    size_t sql_len;
    zval *params = NULL;

    ZEND_PARSE_PARAMETERS_START(1, 2)
        Z_PARAM_STRING(sql, sql_len)
        Z_PARAM_OPTIONAL
        Z_PARAM_ARRAY(params)
    ZEND_PARSE_PARAMETERS_END();

    clickhouse_client_object *intern = Z_CLICKHOUSE_CLIENT_P(ZEND_THIS);

    if (!intern->conn) {
        zend_throw_exception(clickhouse_exception_ce, "Not connected", 0);
        return;
    }

    /* Check read-only mode */
    if (intern->readonly && is_write_query(sql)) {
        zend_throw_exception(clickhouse_exception_ce, "Write operations not allowed in read-only mode", 0);
        return;
    }

    double start_time = 0, query_time = 0;

    /* Start timing if metrics are enabled */
    if (intern->metrics_enabled) {
        struct timeval tv;
        gettimeofday(&tv, NULL);
        start_time = tv.tv_sec + tv.tv_usec / 1000000.0;
    }

    /* Build final query with parameter substitution */
    char *final_query = NULL;
    if (params && Z_TYPE_P(params) == IS_ARRAY && zend_hash_num_elements(Z_ARRVAL_P(params)) > 0) {
        /* Convert PHP array to clickhouse_params */
        clickhouse_params *ch_params = clickhouse_params_create();
        zend_string *key;
        zval *val;
        ZEND_HASH_FOREACH_STR_KEY_VAL(Z_ARRVAL_P(params), key, val) {
            if (key) {
                /* Auto-detect type from PHP value */
                const char *type = detect_clickhouse_type_from_zval(val);
                char *str_val;

                /* Handle arrays specially */
                if (Z_TYPE_P(val) == IS_ARRAY) {
                    /* Check if it's Array, Map, or JSON type */
                    if (strncmp(type, "Array", 5) == 0) {
                        /* Numeric array - serialize to ClickHouse array syntax */
                        str_val = serialize_php_array_to_clickhouse(val);
                        clickhouse_params_add(ch_params, ZSTR_VAL(key), str_val, type);
                        efree(str_val);
                    } else if (strncmp(type, "Map", 3) == 0) {
                        /* Map type - serialize to ClickHouse Map syntax */
                        str_val = serialize_php_map_to_clickhouse(val);
                        clickhouse_params_add(ch_params, ZSTR_VAL(key), str_val, type);
                        efree(str_val);
                    } else {
                        /* Associative array - JSON encode it */
                        smart_str json = {0};
                        php_json_encode(&json, val, PHP_JSON_UNESCAPED_UNICODE | PHP_JSON_UNESCAPED_SLASHES);
                        smart_str_0(&json);
                        clickhouse_params_add(ch_params, ZSTR_VAL(key), ZSTR_VAL(json.s), type);
                        smart_str_free(&json);
                    }
                } else {
                    zend_string *tmp_str = zval_get_string(val);
                    clickhouse_params_add(ch_params, ZSTR_VAL(key), ZSTR_VAL(tmp_str), type);
                    zend_string_release(tmp_str);
                }
            }
        } ZEND_HASH_FOREACH_END();

        final_query = substitute_params(sql, ch_params);
        clickhouse_params_free(ch_params);
        if (!final_query) {
            /* Error already thrown by substitute_params */
            return;
        }
    } else {
        final_query = estrdup(sql);
    }

    clickhouse_result *result = NULL;
    int status;
    int retry_attempted = 0;

retry_execute:
    /* Apply query timeout if set */
    if (intern->query_timeout_ms > 0) {
        clickhouse_connection_set_query_timeout_ms(intern->conn, (int)intern->query_timeout_ms);
    }

    /* Use extended query when compression, session, query ID, query settings, or callbacks are set */
    if (intern->compression != CH_COMPRESS_NONE || intern->session_id || intern->default_query_id ||
        intern->query_settings || !Z_ISUNDEF(intern->log_callback)) {
        clickhouse_query_options *opts = clickhouse_query_options_create();
        if (opts) {
            opts->compression = intern->compression;
            if (intern->session_id) {
                opts->session_id = intern->session_id;
            }
            if (intern->default_query_id) {
                opts->query_id = intern->default_query_id;
            }
            /* Copy query settings from client to options */
            if (intern->query_settings) {
                clickhouse_setting *setting = intern->query_settings->head;
                while (setting) {
                    clickhouse_query_options_set_setting(opts, setting->name, setting->value);
                    setting = setting->next;
                }
            }
            /* Set up log callback if present */
            if (!Z_ISUNDEF(intern->log_callback)) {
                opts->log_callback = php_log_callback_bridge;
                opts->log_user_data = intern;
            }
            status = clickhouse_connection_execute_query_ext(intern->conn, final_query, opts, &result);
            opts->session_id = NULL;
            opts->query_id = NULL;
            clickhouse_query_options_free(opts);
        } else {
            status = clickhouse_connection_execute_query(intern->conn, final_query, &result);
        }
    } else {
        status = clickhouse_connection_execute_query(intern->conn, final_query, &result);
    }

    /* Reset timeout to default if it was set */
    if (intern->query_timeout_ms > 0) {
        clickhouse_connection_set_query_timeout_ms(intern->conn, 0);
    }

    if (status != 0) {
        const char *error = clickhouse_connection_get_error(intern->conn);

        /* Check if auto-reconnect is enabled and this is a connection error */
        if (intern->auto_reconnect && !retry_attempted && is_connection_error(error)) {
            if (result) {
                clickhouse_result_free(result);
                result = NULL;
            }
            /* Attempt to reconnect and retry once */
            if (attempt_reconnect(intern)) {
                retry_attempted = 1;
                goto retry_execute;
            }
        }

        if (result) {
            clickhouse_result_free(result);
        }
        /* Update metrics for failed query */
        if (intern->metrics_enabled) {
            struct timeval tv;
            gettimeofday(&tv, NULL);
            query_time = (tv.tv_sec + tv.tv_usec / 1000000.0) - start_time;
            update_query_metrics(intern, query_time, NULL, 0);
        }
        /* Use enhanced error message with query context */
        throw_query_error(error, sql, final_query);
        efree(final_query);
        return;
    }
    efree(final_query);

    /* Calculate query time if metrics enabled */
    if (intern->metrics_enabled) {
        struct timeval tv;
        gettimeofday(&tv, NULL);
        query_time = (tv.tv_sec + tv.tv_usec / 1000000.0) - start_time;
        update_query_metrics(intern, query_time, result, 1);
    }

    /* Store query ID from result for tracking */
    if (intern->last_query_id) {
        efree(intern->last_query_id);
        intern->last_query_id = NULL;
    }
    if (result && result->query_id) {
        intern->last_query_id = estrdup(result->query_id);
    }

    /* Return results as array */
    array_init(return_value);

    if (result) {
        for (size_t block_idx = 0; block_idx < result->block_count; block_idx++) {
            clickhouse_block *block = result->blocks[block_idx];
            if (!block || block->row_count == 0) continue;

            for (size_t row = 0; row < block->row_count; row++) {
                zval row_arr;
                array_init(&row_arr);

                for (size_t col = 0; col < block->column_count; col++) {
                    clickhouse_column *column = block->columns[col];
                    zval value;
                    column_value_to_zval(column, row, &value);
                    add_assoc_zval(&row_arr, column->name, &value);
                }

                add_next_index_zval(return_value, &row_arr);
            }
        }
        clickhouse_result_free(result);
    }
}
/* }}} */

/* {{{ proto array ClickHouse\Client::executeBatch(array $queries [, array $options]) */
PHP_METHOD(ClickHouse_Client, executeBatch) {
    zval *queries;
    zval *options = NULL;

    ZEND_PARSE_PARAMETERS_START(1, 2)
        Z_PARAM_ARRAY(queries)
        Z_PARAM_OPTIONAL
        Z_PARAM_ARRAY(options)
    ZEND_PARSE_PARAMETERS_END();

    clickhouse_client_object *intern = Z_CLICKHOUSE_CLIENT_P(ZEND_THIS);

    if (!intern->conn) {
        zend_throw_exception(clickhouse_exception_ce, "Not connected", 0);
        return;
    }

    /* Parse options */
    zend_bool stop_on_error = 1;  /* Default: stop on first error */
    zend_bool return_results = 1;  /* Default: return results */

    if (options && Z_TYPE_P(options) == IS_ARRAY) {
        zval *val;
        if ((val = zend_hash_str_find(Z_ARRVAL_P(options), "stopOnError", sizeof("stopOnError") - 1)) != NULL) {
            stop_on_error = zend_is_true(val);
        }
        if ((val = zend_hash_str_find(Z_ARRVAL_P(options), "returnResults", sizeof("returnResults") - 1)) != NULL) {
            return_results = zend_is_true(val);
        }
    }

    /* Initialize return array */
    array_init(return_value);

    /* Iterate through queries */
    zval *query_item;
    zend_ulong idx = 0;
    ZEND_HASH_FOREACH_NUM_KEY_VAL(Z_ARRVAL_P(queries), idx, query_item) {
        char *sql = NULL;
        size_t sql_len = 0;
        zval *params = NULL;
        zval *settings = NULL;

        /* Parse query item - can be string or array */
        if (Z_TYPE_P(query_item) == IS_STRING) {
            /* Simple string query */
            sql = Z_STRVAL_P(query_item);
            sql_len = Z_STRLEN_P(query_item);
        } else if (Z_TYPE_P(query_item) == IS_ARRAY) {
            /* Query configuration array */
            zval *tmp;
            if ((tmp = zend_hash_str_find(Z_ARRVAL_P(query_item), "query", sizeof("query") - 1)) != NULL) {
                if (Z_TYPE_P(tmp) == IS_STRING) {
                    sql = Z_STRVAL_P(tmp);
                    sql_len = Z_STRLEN_P(tmp);
                }
            }
            if ((tmp = zend_hash_str_find(Z_ARRVAL_P(query_item), "params", sizeof("params") - 1)) != NULL) {
                if (Z_TYPE_P(tmp) == IS_ARRAY) {
                    params = tmp;
                }
            }
            if ((tmp = zend_hash_str_find(Z_ARRVAL_P(query_item), "settings", sizeof("settings") - 1)) != NULL) {
                if (Z_TYPE_P(tmp) == IS_ARRAY) {
                    settings = tmp;
                }
            }
        }

        if (!sql || sql_len == 0) {
            /* Invalid query item */
            zval error_result;
            object_init_ex(&error_result, zend_ce_exception);
            zend_update_property_string(zend_ce_exception, Z_OBJ_P(&error_result), "message", sizeof("message") - 1, "Invalid query item: missing 'query' field");
            add_index_zval(return_value, idx, &error_result);

            if (stop_on_error) {
                zend_throw_exception(clickhouse_exception_ce, "Invalid query item: missing 'query' field", 0);
                return;
            }
            continue;
        }

        /* Check read-only mode */
        if (intern->readonly && is_write_query(sql)) {
            zval error_result;
            object_init_ex(&error_result, zend_ce_exception);
            zend_update_property_string(zend_ce_exception, Z_OBJ_P(&error_result), "message", sizeof("message") - 1, "Write operations not allowed in read-only mode");
            add_index_zval(return_value, idx, &error_result);

            if (stop_on_error) {
                zend_throw_exception(clickhouse_exception_ce, "Write operations not allowed in read-only mode", 0);
                return;
            }
            continue;
        }

        /* Build final query with parameter substitution */
        char *final_query = NULL;
        if (params && Z_TYPE_P(params) == IS_ARRAY && zend_hash_num_elements(Z_ARRVAL_P(params)) > 0) {
            clickhouse_params *ch_params = clickhouse_params_create();
            zend_string *key;
            zval *val;
            ZEND_HASH_FOREACH_STR_KEY_VAL(Z_ARRVAL_P(params), key, val) {
                if (key) {
                    /* Auto-detect type from PHP value */
                    const char *type = detect_clickhouse_type_from_zval(val);
                    char *str_val;

                    /* Handle arrays specially */
                    if (Z_TYPE_P(val) == IS_ARRAY) {
                        /* Check if it's a numeric array (Array type) or associative (JSON/Map) */
                        if (strncmp(type, "Array", 5) == 0) {
                            /* Numeric array - serialize to ClickHouse array syntax */
                            str_val = serialize_php_array_to_clickhouse(val);
                            clickhouse_params_add(ch_params, ZSTR_VAL(key), str_val, type);
                            efree(str_val);
                        } else {
                            /* Associative array - JSON encode it */
                            smart_str json = {0};
                            php_json_encode(&json, val, PHP_JSON_UNESCAPED_UNICODE | PHP_JSON_UNESCAPED_SLASHES);
                            smart_str_0(&json);
                            clickhouse_params_add(ch_params, ZSTR_VAL(key), ZSTR_VAL(json.s), type);
                            smart_str_free(&json);
                        }
                    } else {
                        zend_string *tmp_str = zval_get_string(val);
                        clickhouse_params_add(ch_params, ZSTR_VAL(key), ZSTR_VAL(tmp_str), type);
                        zend_string_release(tmp_str);
                    }
                }
            } ZEND_HASH_FOREACH_END();

            final_query = substitute_params(sql, ch_params);
            clickhouse_params_free(ch_params);
        } else {
            final_query = estrdup(sql);
        }

        /* Apply per-query settings if provided */
        clickhouse_settings *saved_settings = NULL;
        if (settings) {
            /* Save current settings */
            saved_settings = intern->query_settings;
            intern->query_settings = NULL;

            /* Apply query-specific settings */
            zend_string *setting_key;
            zval *setting_val;
            ZEND_HASH_FOREACH_STR_KEY_VAL(Z_ARRVAL_P(settings), setting_key, setting_val) {
                if (setting_key) {
                    zend_string *str_val = zval_get_string(setting_val);
                    if (!intern->query_settings) {
                        intern->query_settings = clickhouse_settings_create();
                    }
                    clickhouse_settings_add(intern->query_settings, ZSTR_VAL(setting_key), ZSTR_VAL(str_val), 0);
                    zend_string_release(str_val);
                }
            } ZEND_HASH_FOREACH_END();
        }

        /* Execute query */
        clickhouse_result *result = NULL;
        int status;

        /* Apply query timeout if set */
        if (intern->query_timeout_ms > 0) {
            clickhouse_connection_set_query_timeout_ms(intern->conn, (int)intern->query_timeout_ms);
        }

        /* Use extended query when compression, session, query ID, query settings, or callbacks are set */
        if (intern->compression != CH_COMPRESS_NONE || intern->session_id || intern->default_query_id ||
            intern->query_settings || !Z_ISUNDEF(intern->log_callback)) {
            clickhouse_query_options *opts = clickhouse_query_options_create();
            if (opts) {
                opts->compression = intern->compression;
                if (intern->session_id) {
                    opts->session_id = intern->session_id;
                }
                if (intern->default_query_id) {
                    opts->query_id = intern->default_query_id;
                }
                /* Copy query settings from client to options */
                if (intern->query_settings) {
                    clickhouse_setting *setting = intern->query_settings->head;
                    while (setting) {
                        clickhouse_query_options_set_setting(opts, setting->name, setting->value);
                        setting = setting->next;
                    }
                }
                /* Set up log callback if present */
                if (!Z_ISUNDEF(intern->log_callback)) {
                    opts->log_callback = php_log_callback_bridge;
                    opts->log_user_data = intern;
                }
                status = clickhouse_connection_execute_query_ext(intern->conn, final_query, opts, &result);
                opts->session_id = NULL;
                opts->query_id = NULL;
                clickhouse_query_options_free(opts);
            } else {
                status = clickhouse_connection_execute_query(intern->conn, final_query, &result);
            }
        } else {
            status = clickhouse_connection_execute_query(intern->conn, final_query, &result);
        }

        /* Reset timeout to default if it was set */
        if (intern->query_timeout_ms > 0) {
            clickhouse_connection_set_query_timeout_ms(intern->conn, 0);
        }

        /* Restore saved settings if we applied per-query settings */
        if (settings) {
            if (intern->query_settings) {
                clickhouse_settings_free(intern->query_settings);
            }
            intern->query_settings = saved_settings;
        }

        /* Free final query */
        if (final_query) {
            efree(final_query);
        }

        /* Handle result */
        if (status != 0) {
            /* Query failed */
            const char *error = clickhouse_connection_get_error(intern->conn);
            if (result) {
                clickhouse_result_free(result);
            }

            if (stop_on_error) {
                zend_throw_exception(clickhouse_exception_ce, error ? error : "Query failed", 0);
                return;
            } else {
                /* Store exception object in results array */
                zval error_result;
                object_init_ex(&error_result, zend_ce_exception);
                zend_update_property_string(zend_ce_exception, Z_OBJ_P(&error_result), "message", sizeof("message") - 1, error ? error : "Query failed");
                add_index_zval(return_value, idx, &error_result);
            }
        } else {
            /* Query succeeded */
            /* Check if this is a SELECT query (has results) */
            if (return_results && result && result->block_count > 0) {
                /* Return result array */
                zval result_arr;
                array_init(&result_arr);

                for (size_t block_idx = 0; block_idx < result->block_count; block_idx++) {
                    clickhouse_block *block = result->blocks[block_idx];
                    if (!block || block->row_count == 0) continue;

                    for (size_t row = 0; row < block->row_count; row++) {
                        zval row_arr;
                        array_init(&row_arr);

                        for (size_t col = 0; col < block->column_count; col++) {
                            clickhouse_column *column = block->columns[col];
                            zval value;
                            column_value_to_zval(column, row, &value);
                            add_assoc_zval(&row_arr, column->name, &value);
                        }

                        add_next_index_zval(&result_arr, &row_arr);
                    }
                }

                add_index_zval(return_value, idx, &result_arr);
            } else {
                /* INSERT/DDL query - return true */
                add_index_bool(return_value, idx, 1);
            }

            if (result) {
                clickhouse_result_free(result);
            }
        }
    } ZEND_HASH_FOREACH_END();
}
/* }}} */

/* Helper: Build INSERT query string */
static char *build_insert_query(const char *table, zval *columns) {
    smart_str query = {0};
    smart_str_appends(&query, "INSERT INTO ");
    smart_str_appends(&query, table);
    smart_str_appends(&query, " (");

    zval *col;
    int first = 1;
    ZEND_HASH_FOREACH_VAL(Z_ARRVAL_P(columns), col) {
        if (!first) {
            smart_str_appends(&query, ", ");
        }
        first = 0;
        convert_to_string(col);
        smart_str_appends(&query, Z_STRVAL_P(col));
    } ZEND_HASH_FOREACH_END();

    smart_str_appends(&query, ") VALUES");
    smart_str_0(&query);

    char *result = estrdup(ZSTR_VAL(query.s));
    smart_str_free(&query);
    return result;
}

/* Helper: Set column data from PHP zval */
static int set_column_value(clickhouse_column *col, size_t row, zval *value) {
    clickhouse_type_info *type = col->type;

    /* Handle Nullable */
    if (type->type_id == CH_TYPE_NULLABLE) {
        if (Z_TYPE_P(value) == IS_NULL) {
            col->nulls[row] = 1;
            return 0;
        }
        col->nulls[row] = 0;
        type = type->nested;
    }

    switch (type->type_id) {
        case CH_TYPE_INT8: {
            int8_t *data = (int8_t *)col->data;
            data[row] = (int8_t)zval_get_long(value);
            break;
        }
        case CH_TYPE_INT16: {
            int16_t *data = (int16_t *)col->data;
            data[row] = (int16_t)zval_get_long(value);
            break;
        }
        case CH_TYPE_INT32: {
            int32_t *data = (int32_t *)col->data;
            data[row] = (int32_t)zval_get_long(value);
            break;
        }
        case CH_TYPE_INT64:
        case CH_TYPE_INTERVAL_NANOSECOND:
        case CH_TYPE_INTERVAL_MICROSECOND:
        case CH_TYPE_INTERVAL_MILLISECOND:
        case CH_TYPE_INTERVAL_SECOND:
        case CH_TYPE_INTERVAL_MINUTE:
        case CH_TYPE_INTERVAL_HOUR:
        case CH_TYPE_INTERVAL_DAY:
        case CH_TYPE_INTERVAL_WEEK:
        case CH_TYPE_INTERVAL_MONTH:
        case CH_TYPE_INTERVAL_QUARTER:
        case CH_TYPE_INTERVAL_YEAR: {
            int64_t *data = (int64_t *)col->data;
            data[row] = (int64_t)zval_get_long(value);
            break;
        }
        case CH_TYPE_UINT8: {
            uint8_t *data = (uint8_t *)col->data;
            data[row] = (uint8_t)zval_get_long(value);
            break;
        }
        case CH_TYPE_BOOL: {
            uint8_t *data = (uint8_t *)col->data;
            data[row] = zval_is_true(value) ? 1 : 0;
            break;
        }
        case CH_TYPE_UINT16: {
            uint16_t *data = (uint16_t *)col->data;
            data[row] = (uint16_t)zval_get_long(value);
            break;
        }
        case CH_TYPE_UINT32: {
            uint32_t *data = (uint32_t *)col->data;
            data[row] = (uint32_t)zval_get_long(value);
            break;
        }
        case CH_TYPE_UINT64: {
            uint64_t *data = (uint64_t *)col->data;
            data[row] = (uint64_t)zval_get_long(value);
            break;
        }
        case CH_TYPE_FLOAT32: {
            float *data = (float *)col->data;
            data[row] = (float)zval_get_double(value);
            break;
        }
        case CH_TYPE_FLOAT64: {
            double *data = (double *)col->data;
            data[row] = zval_get_double(value);
            break;
        }
        case CH_TYPE_BFLOAT16: {
            /* BFloat16: convert double to 16-bit brain float */
            uint16_t *data = (uint16_t *)col->data;
            float f = (float)zval_get_double(value);
            uint32_t as_uint32;
            memcpy(&as_uint32, &f, sizeof(float));
            data[row] = (uint16_t)(as_uint32 >> 16);
            break;
        }
        case CH_TYPE_STRING: {
            char **strings = (char **)col->data;
            zend_string *str = zval_get_string(value);
            strings[row] = strdup(ZSTR_VAL(str));
            zend_string_release(str);
            break;
        }
        case CH_TYPE_FIXED_STRING: {
            size_t fixed_len = type->fixed_size;
            char *data = (char *)col->data + (row * fixed_len);
            memset(data, 0, fixed_len);
            zend_string *str = zval_get_string(value);
            size_t copy_len = ZSTR_LEN(str) < fixed_len ? ZSTR_LEN(str) : fixed_len;
            memcpy(data, ZSTR_VAL(str), copy_len);
            zend_string_release(str);
            break;
        }
        case CH_TYPE_DATE: {
            uint16_t *data = (uint16_t *)col->data;
            data[row] = (uint16_t)zval_get_long(value);
            break;
        }
        case CH_TYPE_DATE32: {
            int32_t *data = (int32_t *)col->data;
            data[row] = (int32_t)zval_get_long(value);
            break;
        }
        case CH_TYPE_DATETIME: {
            uint32_t *data = (uint32_t *)col->data;
            data[row] = (uint32_t)zval_get_long(value);
            break;
        }
        case CH_TYPE_DATETIME64: {
            int64_t *data = (int64_t *)col->data;
            /* Accept either integer ticks or float seconds */
            if (Z_TYPE_P(value) == IS_DOUBLE) {
                double val = Z_DVAL_P(value);
                size_t precision = type->fixed_size;
                double scale = 1.0;
                for (size_t i = 0; i < precision; i++) scale *= 10.0;
                data[row] = (int64_t)(val * scale);
            } else {
                data[row] = (int64_t)zval_get_long(value);
            }
            break;
        }
        case CH_TYPE_UUID: {
            /* Accept UUID string like "550e8400-e29b-41d4-a716-446655440000" */
            uint8_t *data = (uint8_t *)col->data + (row * 16);
            memset(data, 0, 16);
            zend_string *str = zval_get_string(value);
            const char *s = ZSTR_VAL(str);
            /* Parse UUID string - format: 8-4-4-4-12 */
            unsigned int bytes[16];
            int parsed = sscanf(s, "%2x%2x%2x%2x-%2x%2x-%2x%2x-%2x%2x-%2x%2x%2x%2x%2x%2x",
                &bytes[0], &bytes[1], &bytes[2], &bytes[3],
                &bytes[4], &bytes[5], &bytes[6], &bytes[7],
                &bytes[8], &bytes[9], &bytes[10], &bytes[11],
                &bytes[12], &bytes[13], &bytes[14], &bytes[15]);
            if (parsed == 16) {
                /* Store in ClickHouse format (each 8-byte half reversed) */
                data[7] = bytes[0]; data[6] = bytes[1]; data[5] = bytes[2]; data[4] = bytes[3];
                data[3] = bytes[4]; data[2] = bytes[5]; data[1] = bytes[6]; data[0] = bytes[7];
                data[15] = bytes[8]; data[14] = bytes[9]; data[13] = bytes[10]; data[12] = bytes[11];
                data[11] = bytes[12]; data[10] = bytes[13]; data[9] = bytes[14]; data[8] = bytes[15];
            }
            zend_string_release(str);
            break;
        }
        case CH_TYPE_IPV4: {
            /* Accept IPv4 string like "192.168.1.1" */
            uint8_t *data = (uint8_t *)col->data + (row * 4);
            memset(data, 0, 4);
            zend_string *str = zval_get_string(value);
            unsigned int a, b, c, d;
            if (sscanf(ZSTR_VAL(str), "%u.%u.%u.%u", &a, &b, &c, &d) == 4) {
                /* Store in little-endian format */
                data[0] = d; data[1] = c; data[2] = b; data[3] = a;
            }
            zend_string_release(str);
            break;
        }
        case CH_TYPE_IPV6: {
            /* Accept IPv6 string */
            uint8_t *data = (uint8_t *)col->data + (row * 16);
            memset(data, 0, 16);
            zend_string *str = zval_get_string(value);
            unsigned int bytes[16];
            /* Parse expanded IPv6 format */
            if (sscanf(ZSTR_VAL(str), "%2x%2x:%2x%2x:%2x%2x:%2x%2x:%2x%2x:%2x%2x:%2x%2x:%2x%2x",
                &bytes[0], &bytes[1], &bytes[2], &bytes[3],
                &bytes[4], &bytes[5], &bytes[6], &bytes[7],
                &bytes[8], &bytes[9], &bytes[10], &bytes[11],
                &bytes[12], &bytes[13], &bytes[14], &bytes[15]) == 16) {
                for (int i = 0; i < 16; i++) data[i] = bytes[i];
            }
            zend_string_release(str);
            break;
        }
        case CH_TYPE_DECIMAL32: {
            int32_t *data = (int32_t *)col->data;
            size_t scale = col->type ? col->type->fixed_size : 0;
            double multiplier = 1.0;
            for (size_t i = 0; i < scale; i++) multiplier *= 10.0;
            if (Z_TYPE_P(value) == IS_DOUBLE) {
                data[row] = (int32_t)(Z_DVAL_P(value) * multiplier);
            } else if (Z_TYPE_P(value) == IS_STRING) {
                data[row] = (int32_t)(zval_get_double(value) * multiplier);
            } else {
                data[row] = (int32_t)(zval_get_long(value) * (int64_t)multiplier);
            }
            break;
        }
        case CH_TYPE_DECIMAL64: {
            int64_t *data = (int64_t *)col->data;
            size_t scale = col->type ? col->type->fixed_size : 0;
            double multiplier = 1.0;
            for (size_t i = 0; i < scale; i++) multiplier *= 10.0;
            if (Z_TYPE_P(value) == IS_DOUBLE) {
                data[row] = (int64_t)(Z_DVAL_P(value) * multiplier);
            } else if (Z_TYPE_P(value) == IS_STRING) {
                data[row] = (int64_t)(zval_get_double(value) * multiplier);
            } else {
                data[row] = (int64_t)(zval_get_long(value) * (int64_t)multiplier);
            }
            break;
        }
        case CH_TYPE_ENUM8: {
            int8_t *data = (int8_t *)col->data;
            data[row] = (int8_t)zval_get_long(value);
            break;
        }
        case CH_TYPE_ENUM16: {
            int16_t *data = (int16_t *)col->data;
            data[row] = (int16_t)zval_get_long(value);
            break;
        }
        default:
            return -1;
    }
    return 0;
}

/* {{{ proto void ClickHouse\Client::insert(string $table, array $columns, array $rows) */
PHP_METHOD(ClickHouse_Client, insert) {
    char *table;
    size_t table_len;
    zval *columns;
    zval *rows;

    ZEND_PARSE_PARAMETERS_START(3, 3)
        Z_PARAM_STRING(table, table_len)
        Z_PARAM_ARRAY(columns)
        Z_PARAM_ARRAY(rows)
    ZEND_PARSE_PARAMETERS_END();

    clickhouse_client_object *intern = Z_CLICKHOUSE_CLIENT_P(ZEND_THIS);

    if (!intern->conn) {
        zend_throw_exception(clickhouse_exception_ce, "Not connected", 0);
        return;
    }

    /* Check read-only mode */
    if (intern->readonly) {
        zend_throw_exception(clickhouse_exception_ce, "Insert operations not allowed in read-only mode", 0);
        return;
    }

    /* Serialize rows to JSONEachRow and stream via FORMAT insert */
    smart_str payload = {0};
    size_t col_count = zend_hash_num_elements(Z_ARRVAL_P(columns));

    zval *row;
    ZEND_HASH_FOREACH_VAL(Z_ARRVAL_P(rows), row) {
        if (Z_TYPE_P(row) != IS_ARRAY) {
            continue;
        }

        zval row_obj;
        array_init(&row_obj);

        size_t idx = 0;
        zval *col_zv;
        ZEND_HASH_FOREACH_VAL(Z_ARRVAL_P(columns), col_zv) {
            zend_string *col_name = zval_get_string(col_zv);
            zval *cell = zend_hash_index_find(Z_ARRVAL_P(row), idx);
            if (cell) {
                Z_TRY_ADDREF_P(cell);
                add_assoc_zval_ex(&row_obj, ZSTR_VAL(col_name), ZSTR_LEN(col_name), cell);
            } else {
                add_assoc_null_ex(&row_obj, ZSTR_VAL(col_name), ZSTR_LEN(col_name));
            }
            zend_string_release(col_name);
            idx++;
        } ZEND_HASH_FOREACH_END();

        smart_str json_row = {0};
        php_json_encode(&json_row, &row_obj, PHP_JSON_UNESCAPED_UNICODE | PHP_JSON_UNESCAPED_SLASHES);
        smart_str_appendc(&json_row, '\n');
        smart_str_append_smart_str(&payload, &json_row);
        smart_str_free(&json_row);
        zval_ptr_dtor(&row_obj);
    } ZEND_HASH_FOREACH_END();

    smart_str_0(&payload);

    int status = clickhouse_connection_insert_format_data(intern->conn, table, "JSONEachRow",
                                                          payload.s ? ZSTR_VAL(payload.s) : "",
                                                          payload.s ? ZSTR_LEN(payload.s) : 0);
    smart_str_free(&payload);

    if (status != 0) {
        const char *error = clickhouse_connection_get_error(intern->conn);
        zend_throw_exception(clickhouse_exception_ce, error ? error : "Insert failed", 0);
        return;
    }
}


/* {{{ proto bool ClickHouse\Client::ping() */
PHP_METHOD(ClickHouse_Client, ping) {
    ZEND_PARSE_PARAMETERS_NONE();

    clickhouse_client_object *intern = Z_CLICKHOUSE_CLIENT_P(ZEND_THIS);

    if (!intern->conn) {
        RETURN_FALSE;
    }

    int result = clickhouse_connection_ping(intern->conn);
    RETURN_BOOL(result == 0);
}
/* }}} */

/* {{{ proto void ClickHouse\Client::close() */
PHP_METHOD(ClickHouse_Client, close) {
    ZEND_PARSE_PARAMETERS_NONE();

    clickhouse_client_object *intern = Z_CLICKHOUSE_CLIENT_P(ZEND_THIS);

    if (intern->conn) {
        clickhouse_connection_close(intern->conn);
        clickhouse_connection_free(intern->conn);
        intern->conn = NULL;
    }
}
/* }}} */

/* {{{ proto bool ClickHouse\Client::isConnected() */
PHP_METHOD(ClickHouse_Client, isConnected) {
    ZEND_PARSE_PARAMETERS_NONE();

    clickhouse_client_object *intern = Z_CLICKHOUSE_CLIENT_P(ZEND_THIS);
    RETURN_BOOL(intern->conn != NULL && intern->conn->socket_fd >= 0);
}
/* }}} */

/* {{{ proto void ClickHouse\Client::insertFromString(string $table, string $data [, string $format])
   Insert raw formatted data (CSV, TSV, JSONEachRow, etc.) from a string */
PHP_METHOD(ClickHouse_Client, insertFromString) {
    char *table, *data, *format = "CSV";
    size_t table_len, data_len, format_len = 3;

    ZEND_PARSE_PARAMETERS_START(2, 3)
        Z_PARAM_STRING(table, table_len)
        Z_PARAM_STRING(data, data_len)
        Z_PARAM_OPTIONAL
        Z_PARAM_STRING(format, format_len)
    ZEND_PARSE_PARAMETERS_END();

    clickhouse_client_object *intern = Z_CLICKHOUSE_CLIENT_P(ZEND_THIS);

    if (!intern->conn) {
        zend_throw_exception(clickhouse_exception_ce, "Not connected", 0);
        return;
    }

    int status = clickhouse_connection_insert_format_data(intern->conn, table, format, data, data_len);

    if (status != 0) {
        const char *error = clickhouse_connection_get_error(intern->conn);
        zend_throw_exception(clickhouse_exception_ce, error ? error : "Insert failed", 0);
        return;
    }
}
/* }}} */

/* {{{ proto void ClickHouse\Client::insertFromFile(string $table, string $filepath [, string $format])
   Insert raw formatted data (CSV, TSV, JSONEachRow, etc.) from a file */
PHP_METHOD(ClickHouse_Client, insertFromFile) {
    char *table, *filepath, *format = "CSV";
    size_t table_len, filepath_len, format_len = 3;

    ZEND_PARSE_PARAMETERS_START(2, 3)
        Z_PARAM_STRING(table, table_len)
        Z_PARAM_STRING(filepath, filepath_len)
        Z_PARAM_OPTIONAL
        Z_PARAM_STRING(format, format_len)
    ZEND_PARSE_PARAMETERS_END();

    clickhouse_client_object *intern = Z_CLICKHOUSE_CLIENT_P(ZEND_THIS);

    if (!intern->conn) {
        zend_throw_exception(clickhouse_exception_ce, "Not connected", 0);
        return;
    }

    /* Read file contents */
    php_stream *stream = php_stream_open_wrapper(filepath, "rb", REPORT_ERRORS, NULL);
    if (!stream) {
        zend_throw_exception(clickhouse_exception_ce, "Failed to open file", 0);
        return;
    }

    zend_string *contents = php_stream_copy_to_mem(stream, PHP_STREAM_COPY_ALL, 0);
    php_stream_close(stream);

    if (!contents || ZSTR_LEN(contents) == 0) {
        if (contents) {
            zend_string_release(contents);
        }
        zend_throw_exception(clickhouse_exception_ce, "File is empty or unreadable", 0);
        return;
    }

    int status = clickhouse_connection_insert_format_data(intern->conn, table, format, ZSTR_VAL(contents), ZSTR_LEN(contents));
    zend_string_release(contents);

    if (status != 0) {
        const char *error = clickhouse_connection_get_error(intern->conn);
        zend_throw_exception(clickhouse_exception_ce, error ? error : "Insert failed", 0);
        return;
    }
}
/* }}} */

/* {{{ proto array ClickHouse\Client::getServerInfo() */
PHP_METHOD(ClickHouse_Client, getServerInfo) {
    ZEND_PARSE_PARAMETERS_NONE();

    clickhouse_client_object *intern = Z_CLICKHOUSE_CLIENT_P(ZEND_THIS);

    if (!intern->conn || !intern->conn->server_info) {
        zend_throw_exception(clickhouse_exception_ce, "Not connected", 0);
        return;
    }

    clickhouse_server_info *info = intern->conn->server_info;

    array_init(return_value);
    if (info->name) {
        add_assoc_string(return_value, "name", info->name);
    }
    add_assoc_long(return_value, "version_major", (zend_long)info->version_major);
    add_assoc_long(return_value, "version_minor", (zend_long)info->version_minor);
    add_assoc_long(return_value, "version_patch", (zend_long)info->version_patch);
    add_assoc_long(return_value, "revision", (zend_long)info->revision);
    if (info->timezone) {
        add_assoc_string(return_value, "timezone", info->timezone);
    }
    if (info->display_name) {
        add_assoc_string(return_value, "display_name", info->display_name);
    }
}
/* }}} */

/* {{{ proto array ClickHouse\Client::getDatabases()
   Returns list of database names */
PHP_METHOD(ClickHouse_Client, getDatabases) {
    ZEND_PARSE_PARAMETERS_NONE();

    clickhouse_client_object *intern = Z_CLICKHOUSE_CLIENT_P(ZEND_THIS);

    if (!intern->conn) {
        zend_throw_exception(clickhouse_exception_ce, "Not connected", 0);
        return;
    }

    clickhouse_result *result = NULL;
    int status = clickhouse_connection_execute_query(intern->conn,
        "SELECT name FROM system.databases ORDER BY name", &result);

    if (status != 0) {
        const char *error = clickhouse_connection_get_error(intern->conn);
        if (result) {
            clickhouse_result_free(result);
        }
        zend_throw_exception(clickhouse_exception_ce, error ? error : "Failed to get databases", 0);
        return;
    }

    /* Extract first column as flat array of strings */
    array_init(return_value);

    if (result) {
        for (size_t b = 0; b < result->block_count; b++) {
            clickhouse_block *block = result->blocks[b];
            if (block->column_count < 1) continue;

            clickhouse_column *col = block->columns[0];
            for (size_t row = 0; row < block->row_count; row++) {
                zval cell;
                column_value_to_zval(col, row, &cell);
                if (Z_TYPE(cell) == IS_STRING) {
                    add_next_index_zval(return_value, &cell);
                } else {
                    zval_ptr_dtor(&cell);
                }
            }
        }
        clickhouse_result_free(result);
    }
}
/* }}} */

/* {{{ proto array ClickHouse\Client::getTables(?string $database = null)
   Returns list of table names in the specified or current database */
PHP_METHOD(ClickHouse_Client, getTables) {
    char *database = NULL;
    size_t database_len = 0;

    ZEND_PARSE_PARAMETERS_START(0, 1)
        Z_PARAM_OPTIONAL
        Z_PARAM_STRING_OR_NULL(database, database_len)
    ZEND_PARSE_PARAMETERS_END();

    clickhouse_client_object *intern = Z_CLICKHOUSE_CLIENT_P(ZEND_THIS);

    if (!intern->conn) {
        zend_throw_exception(clickhouse_exception_ce, "Not connected", 0);
        return;
    }

    /* Use provided database or fall back to saved_database */
    const char *db_to_use = database ? database : intern->saved_database;
    if (!db_to_use || *db_to_use == '\0') {
        db_to_use = "default";
    }

    /* Build query with proper escaping */
    char query[512];
    snprintf(query, sizeof(query),
        "SELECT name FROM system.tables WHERE database = '%s' ORDER BY name",
        db_to_use);

    clickhouse_result *result = NULL;
    int status = clickhouse_connection_execute_query(intern->conn, query, &result);

    if (status != 0) {
        const char *error = clickhouse_connection_get_error(intern->conn);
        if (result) {
            clickhouse_result_free(result);
        }
        zend_throw_exception(clickhouse_exception_ce, error ? error : "Failed to get tables", 0);
        return;
    }

    /* Extract first column as flat array of strings */
    array_init(return_value);

    if (result) {
        for (size_t b = 0; b < result->block_count; b++) {
            clickhouse_block *block = result->blocks[b];
            if (block->column_count < 1) continue;

            clickhouse_column *col = block->columns[0];
            for (size_t row = 0; row < block->row_count; row++) {
                zval cell;
                column_value_to_zval(col, row, &cell);
                if (Z_TYPE(cell) == IS_STRING) {
                    add_next_index_zval(return_value, &cell);
                } else {
                    zval_ptr_dtor(&cell);
                }
            }
        }
        clickhouse_result_free(result);
    }
}
/* }}} */

/* {{{ proto array ClickHouse\Client::describeTable(string $table, ?string $database = null)
   Returns table schema information */
PHP_METHOD(ClickHouse_Client, describeTable) {
    char *table;
    size_t table_len;
    char *database = NULL;
    size_t database_len = 0;

    ZEND_PARSE_PARAMETERS_START(1, 2)
        Z_PARAM_STRING(table, table_len)
        Z_PARAM_OPTIONAL
        Z_PARAM_STRING_OR_NULL(database, database_len)
    ZEND_PARSE_PARAMETERS_END();

    clickhouse_client_object *intern = Z_CLICKHOUSE_CLIENT_P(ZEND_THIS);

    if (!intern->conn) {
        zend_throw_exception(clickhouse_exception_ce, "Not connected", 0);
        return;
    }

    /* Use provided database or fall back to saved_database */
    const char *db_to_use = database ? database : intern->saved_database;

    /* Build query - DESCRIBE TABLE db.table or just DESCRIBE TABLE table */
    char query[512];
    if (db_to_use && *db_to_use != '\0') {
        snprintf(query, sizeof(query), "DESCRIBE TABLE `%s`.`%s`", db_to_use, table);
    } else {
        snprintf(query, sizeof(query), "DESCRIBE TABLE `%s`", table);
    }

    clickhouse_result *result = NULL;
    int status = clickhouse_connection_execute_query(intern->conn, query, &result);

    if (status != 0) {
        const char *error = clickhouse_connection_get_error(intern->conn);
        if (result) {
            clickhouse_result_free(result);
        }
        zend_throw_exception(clickhouse_exception_ce, error ? error : "Failed to describe table", 0);
        return;
    }

    array_init(return_value);

    /* DESCRIBE TABLE returns: name, type, default_type, default_expression, comment, codec_expression, ttl_expression */
    const char *col_names[] = {"name", "type", "default_type", "default_expression", "comment", "codec_expression", "ttl_expression"};
    size_t num_col_names = sizeof(col_names) / sizeof(col_names[0]);

    if (result) {
        for (size_t block_idx = 0; block_idx < result->block_count; block_idx++) {
            clickhouse_block *block = result->blocks[block_idx];
            if (!block || block->row_count == 0) continue;

            size_t col_count = block->column_count;
            size_t num_cols = col_count < num_col_names ? col_count : num_col_names;

            for (size_t row = 0; row < block->row_count; row++) {
                zval row_arr;
                array_init(&row_arr);

                for (size_t c = 0; c < num_cols; c++) {
                    zval value;
                    column_value_to_zval(block->columns[c], row, &value);
                    if (Z_TYPE(value) == IS_STRING) {
                        add_assoc_zval(&row_arr, col_names[c], &value);
                    } else {
                        add_assoc_string(&row_arr, col_names[c], "");
                        zval_ptr_dtor(&value);
                    }
                }

                add_next_index_zval(return_value, &row_arr);
            }
        }
        clickhouse_result_free(result);
    }
}
/* }}} */

/* {{{ proto void ClickHouse\Client::setCompression(int $method) */
PHP_METHOD(ClickHouse_Client, setCompression) {
    zend_long method;

    ZEND_PARSE_PARAMETERS_START(1, 1)
        Z_PARAM_LONG(method)
    ZEND_PARSE_PARAMETERS_END();

    clickhouse_client_object *intern = Z_CLICKHOUSE_CLIENT_P(ZEND_THIS);

    if (method < 0 || method > 2) {
        zend_throw_exception(clickhouse_exception_ce,
            "Invalid compression method. Use Client::COMPRESS_NONE, COMPRESS_LZ4, or COMPRESS_ZSTD", 0);
        return;
    }

    /* Check if compression method is supported */
    if (!clickhouse_compression_supported((uint8_t)method)) {
        zend_throw_exception(clickhouse_exception_ce,
            "Compression method not supported (library not compiled in)", 0);
        return;
    }

    intern->compression = (uint8_t)method;
}
/* }}} */

/* {{{ proto int ClickHouse\Client::getCompression() */
PHP_METHOD(ClickHouse_Client, getCompression) {
    ZEND_PARSE_PARAMETERS_NONE();

    clickhouse_client_object *intern = Z_CLICKHOUSE_CLIENT_P(ZEND_THIS);
    RETURN_LONG(intern->compression);
}
/* }}} */

/* {{{ proto void ClickHouse\Client::setProgressCallback(?callable $callback)
   Set callback for progress updates during query execution.
   Callback receives: (int $rows, int $bytes, int $total_rows, int $written_rows, int $written_bytes) */
PHP_METHOD(ClickHouse_Client, setProgressCallback) {
    zend_fcall_info fci;
    zend_fcall_info_cache fcc;
    zend_bool callback_is_null = 0;

    ZEND_PARSE_PARAMETERS_START(1, 1)
        Z_PARAM_FUNC_EX(fci, fcc, 1, 0)
    ZEND_PARSE_PARAMETERS_END();

    clickhouse_client_object *intern = Z_CLICKHOUSE_CLIENT_P(ZEND_THIS);

    /* Clear existing callback */
    if (!Z_ISUNDEF(intern->progress_callback)) {
        zval_ptr_dtor(&intern->progress_callback);
        ZVAL_UNDEF(&intern->progress_callback);
    }

    /* Set new callback if provided */
    if (ZEND_FCI_INITIALIZED(fci)) {
        ZVAL_COPY(&intern->progress_callback, &fci.function_name);
    }
}
/* }}} */

/* {{{ proto void ClickHouse\Client::setProfileCallback(?callable $callback)
   Set callback for profile info events during query execution.
   Callback receives: (int $rows, int $blocks, int $bytes, bool $applied_limit, int $rows_before_limit, bool $calculated_rows_before_limit) */
PHP_METHOD(ClickHouse_Client, setProfileCallback) {
    zend_fcall_info fci;
    zend_fcall_info_cache fcc;
    zend_bool callback_is_null = 0;

    ZEND_PARSE_PARAMETERS_START(1, 1)
        Z_PARAM_FUNC_EX(fci, fcc, 1, 0)
    ZEND_PARSE_PARAMETERS_END();

    clickhouse_client_object *intern = Z_CLICKHOUSE_CLIENT_P(ZEND_THIS);

    /* Clear existing callback */
    if (!Z_ISUNDEF(intern->profile_callback)) {
        zval_ptr_dtor(&intern->profile_callback);
        ZVAL_UNDEF(&intern->profile_callback);
    }

    /* Set new callback if provided */
    if (ZEND_FCI_INITIALIZED(fci)) {
        ZVAL_COPY(&intern->profile_callback, &fci.function_name);
    }
}
/* }}} */

/* {{{ proto void ClickHouse\Client::setLogCallback(?callable $callback)
   Set callback for server log messages during query execution.
   Callback receives: (int $timestamp, int $microseconds, int $thread_id, int $level, string $source, string $text) */
PHP_METHOD(ClickHouse_Client, setLogCallback) {
    zend_fcall_info fci;
    zend_fcall_info_cache fcc;
    zend_bool callback_is_null = 0;

    ZEND_PARSE_PARAMETERS_START(1, 1)
        Z_PARAM_FUNC_EX(fci, fcc, 1, 0)
    ZEND_PARSE_PARAMETERS_END();

    clickhouse_client_object *intern = Z_CLICKHOUSE_CLIENT_P(ZEND_THIS);

    /* Clear existing callback */
    if (!Z_ISUNDEF(intern->log_callback)) {
        zval_ptr_dtor(&intern->log_callback);
        ZVAL_UNDEF(&intern->log_callback);
    }

    /* Set new callback if provided */
    if (ZEND_FCI_INITIALIZED(fci)) {
        ZVAL_COPY(&intern->log_callback, &fci.function_name);
    }
}
/* }}} */

/* {{{ proto void ClickHouse\Client::setReadOnly(bool $readonly)
   Enable or disable read-only mode. When enabled, prevents execution of write operations. */
PHP_METHOD(ClickHouse_Client, setReadOnly) {
    zend_bool readonly;

    ZEND_PARSE_PARAMETERS_START(1, 1)
        Z_PARAM_BOOL(readonly)
    ZEND_PARSE_PARAMETERS_END();

    clickhouse_client_object *intern = Z_CLICKHOUSE_CLIENT_P(ZEND_THIS);
    intern->readonly = readonly;
}
/* }}} */

/* {{{ proto bool ClickHouse\Client::isReadOnly()
   Check if read-only mode is enabled. */
PHP_METHOD(ClickHouse_Client, isReadOnly) {
    ZEND_PARSE_PARAMETERS_NONE();

    clickhouse_client_object *intern = Z_CLICKHOUSE_CLIENT_P(ZEND_THIS);
    RETURN_BOOL(intern->readonly);
}
/* }}} */

/* {{{ proto bool ClickHouse\Client::beginTransaction()
   Begin a transaction (EXPERIMENTAL - requires Atomic database engine) */
PHP_METHOD(ClickHouse_Client, beginTransaction) {
    ZEND_PARSE_PARAMETERS_NONE();

    clickhouse_client_object *intern = Z_CLICKHOUSE_CLIENT_P(ZEND_THIS);

    if (!intern->conn) {
        zend_throw_exception(clickhouse_exception_ce, "Not connected", 0);
        RETURN_FALSE;
    }

    if (intern->in_transaction) {
        zend_throw_exception(clickhouse_exception_ce, "Transaction already active", 0);
        RETURN_FALSE;
    }

    /* ClickHouse transactions use sessions - ensure we have a session ID */
    if (!intern->session_id) {
        /* Generate a session ID for transaction isolation */
        char session_buf[64];
        snprintf(session_buf, sizeof(session_buf), "tx_%ld_%d", (long)time(NULL), rand());
        intern->session_id = estrdup(session_buf);
    }

    /* ClickHouse BEGIN TRANSACTION syntax */
    clickhouse_result *result = NULL;
    clickhouse_query_options *opts = clickhouse_query_options_create();
    if (!opts) {
        zend_throw_exception(clickhouse_exception_ce, "Failed to create query options", 0);
        RETURN_FALSE;
    }

    opts->session_id = intern->session_id;
    opts->compression = intern->compression;

    int status = clickhouse_connection_execute_query_ext(intern->conn, "BEGIN TRANSACTION", opts, &result);

    opts->session_id = NULL;
    clickhouse_query_options_free(opts);

    if (status != 0) {
        const char *error = clickhouse_connection_get_error(intern->conn);
        if (result) clickhouse_result_free(result);

        char err_msg[512];
        snprintf(err_msg, sizeof(err_msg),
                "Failed to begin transaction: %s. "
                "Note: Transactions require ClickHouse 21.11+ with Atomic database engine "
                "and compatible table engines (e.g., ReplicatedMergeTree). "
                "This feature is EXPERIMENTAL.",
                error ? error : "Unknown error");
        zend_throw_exception(clickhouse_exception_ce, err_msg, 0);
        RETURN_FALSE;
    }

    if (result) clickhouse_result_free(result);

    intern->in_transaction = 1;
    RETURN_TRUE;
}
/* }}} */

/* {{{ proto bool ClickHouse\Client::commit()
   Commit the current transaction (EXPERIMENTAL) */
PHP_METHOD(ClickHouse_Client, commit) {
    ZEND_PARSE_PARAMETERS_NONE();

    clickhouse_client_object *intern = Z_CLICKHOUSE_CLIENT_P(ZEND_THIS);

    if (!intern->conn) {
        zend_throw_exception(clickhouse_exception_ce, "Not connected", 0);
        RETURN_FALSE;
    }

    if (!intern->in_transaction) {
        zend_throw_exception(clickhouse_exception_ce, "No active transaction", 0);
        RETURN_FALSE;
    }

    /* ClickHouse COMMIT syntax */
    clickhouse_result *result = NULL;
    clickhouse_query_options *opts = clickhouse_query_options_create();
    if (!opts) {
        zend_throw_exception(clickhouse_exception_ce, "Failed to create query options", 0);
        RETURN_FALSE;
    }

    opts->session_id = intern->session_id;
    opts->compression = intern->compression;

    int status = clickhouse_connection_execute_query_ext(intern->conn, "COMMIT", opts, &result);

    opts->session_id = NULL;
    clickhouse_query_options_free(opts);

    if (status != 0) {
        const char *error = clickhouse_connection_get_error(intern->conn);
        if (result) clickhouse_result_free(result);

        char err_msg[256];
        snprintf(err_msg, sizeof(err_msg),
                "Failed to commit transaction: %s",
                error ? error : "Unknown error");
        zend_throw_exception(clickhouse_exception_ce, err_msg, 0);
        RETURN_FALSE;
    }

    if (result) clickhouse_result_free(result);

    intern->in_transaction = 0;
    RETURN_TRUE;
}
/* }}} */

/* {{{ proto bool ClickHouse\Client::rollback()
   Rollback the current transaction (EXPERIMENTAL) */
PHP_METHOD(ClickHouse_Client, rollback) {
    ZEND_PARSE_PARAMETERS_NONE();

    clickhouse_client_object *intern = Z_CLICKHOUSE_CLIENT_P(ZEND_THIS);

    if (!intern->conn) {
        zend_throw_exception(clickhouse_exception_ce, "Not connected", 0);
        RETURN_FALSE;
    }

    if (!intern->in_transaction) {
        zend_throw_exception(clickhouse_exception_ce, "No active transaction", 0);
        RETURN_FALSE;
    }

    /* ClickHouse ROLLBACK syntax */
    clickhouse_result *result = NULL;
    clickhouse_query_options *opts = clickhouse_query_options_create();
    if (!opts) {
        zend_throw_exception(clickhouse_exception_ce, "Failed to create query options", 0);
        RETURN_FALSE;
    }

    opts->session_id = intern->session_id;
    opts->compression = intern->compression;

    int status = clickhouse_connection_execute_query_ext(intern->conn, "ROLLBACK", opts, &result);

    opts->session_id = NULL;
    clickhouse_query_options_free(opts);

    if (status != 0) {
        const char *error = clickhouse_connection_get_error(intern->conn);
        if (result) clickhouse_result_free(result);

        char err_msg[256];
        snprintf(err_msg, sizeof(err_msg),
                "Failed to rollback transaction: %s",
                error ? error : "Unknown error");
        zend_throw_exception(clickhouse_exception_ce, err_msg, 0);
        RETURN_FALSE;
    }

    if (result) clickhouse_result_free(result);

    intern->in_transaction = 0;
    RETURN_TRUE;
}
/* }}} */

/* {{{ proto bool ClickHouse\Client::inTransaction()
   Check if currently in a transaction */
PHP_METHOD(ClickHouse_Client, inTransaction) {
    ZEND_PARSE_PARAMETERS_NONE();

    clickhouse_client_object *intern = Z_CLICKHOUSE_CLIENT_P(ZEND_THIS);
    RETURN_BOOL(intern->in_transaction);
}
/* }}} */

/* {{{ proto void ClickHouse\Client::setTimeout(int $timeout_ms)
   Set query timeout in milliseconds. 0 means no timeout. */
PHP_METHOD(ClickHouse_Client, setTimeout) {
    zend_long timeout_ms;

    ZEND_PARSE_PARAMETERS_START(1, 1)
        Z_PARAM_LONG(timeout_ms)
    ZEND_PARSE_PARAMETERS_END();

    clickhouse_client_object *intern = Z_CLICKHOUSE_CLIENT_P(ZEND_THIS);

    if (timeout_ms < 0) {
        zend_throw_exception(clickhouse_exception_ce,
            "Timeout must be non-negative (0 for no timeout)", 0);
        return;
    }

    intern->query_timeout_ms = timeout_ms;
}
/* }}} */

/* {{{ proto int ClickHouse\Client::getTimeout()
   Get current query timeout in milliseconds. */
PHP_METHOD(ClickHouse_Client, getTimeout) {
    ZEND_PARSE_PARAMETERS_NONE();

    clickhouse_client_object *intern = Z_CLICKHOUSE_CLIENT_P(ZEND_THIS);
    RETURN_LONG(intern->query_timeout_ms);
}
/* }}} */

/* {{{ proto void ClickHouse\Client::setSSL(bool $enabled) */
PHP_METHOD(ClickHouse_Client, setSSL) {
    zend_bool enabled;

    ZEND_PARSE_PARAMETERS_START(1, 1)
        Z_PARAM_BOOL(enabled)
    ZEND_PARSE_PARAMETERS_END();

    clickhouse_client_object *intern = Z_CLICKHOUSE_CLIENT_P(ZEND_THIS);

    if (!intern->conn) {
        zend_throw_exception(clickhouse_exception_ce, "Not connected", 0);
        return;
    }

#ifdef HAVE_OPENSSL
    clickhouse_connection_set_ssl_enabled(intern->conn, enabled ? 1 : 0);
#else
    if (enabled) {
        zend_throw_exception(clickhouse_exception_ce,
            "SSL/TLS support not available (extension not compiled with OpenSSL)", 0);
        return;
    }
#endif
}
/* }}} */

/* {{{ proto void ClickHouse\Client::setSSLVerify(bool $verify_peer, bool $verify_host = true) */
PHP_METHOD(ClickHouse_Client, setSSLVerify) {
    zend_bool verify_peer;
    zend_bool verify_host = 1;

    ZEND_PARSE_PARAMETERS_START(1, 2)
        Z_PARAM_BOOL(verify_peer)
        Z_PARAM_OPTIONAL
        Z_PARAM_BOOL(verify_host)
    ZEND_PARSE_PARAMETERS_END();

    clickhouse_client_object *intern = Z_CLICKHOUSE_CLIENT_P(ZEND_THIS);

    if (!intern->conn) {
        zend_throw_exception(clickhouse_exception_ce, "Not connected", 0);
        return;
    }

#ifdef HAVE_OPENSSL
    clickhouse_connection_set_ssl_verify(intern->conn, verify_peer ? 1 : 0, verify_host ? 1 : 0);
#else
    zend_throw_exception(clickhouse_exception_ce,
        "SSL/TLS support not available (extension not compiled with OpenSSL)", 0);
#endif
}
/* }}} */

/* {{{ proto void ClickHouse\Client::setSSLCA(string $ca_path) */
PHP_METHOD(ClickHouse_Client, setSSLCA) {
    char *ca_path;
    size_t ca_path_len;

    ZEND_PARSE_PARAMETERS_START(1, 1)
        Z_PARAM_STRING(ca_path, ca_path_len)
    ZEND_PARSE_PARAMETERS_END();

    clickhouse_client_object *intern = Z_CLICKHOUSE_CLIENT_P(ZEND_THIS);

    if (!intern->conn) {
        zend_throw_exception(clickhouse_exception_ce, "Not connected", 0);
        return;
    }

#ifdef HAVE_OPENSSL
    clickhouse_connection_set_ssl_ca_cert(intern->conn, ca_path);
#else
    zend_throw_exception(clickhouse_exception_ce,
        "SSL/TLS support not available (extension not compiled with OpenSSL)", 0);
#endif
}
/* }}} */

/* {{{ proto void ClickHouse\Client::setSSLCert(string $cert_path, string $key_path) */
PHP_METHOD(ClickHouse_Client, setSSLCert) {
    char *cert_path, *key_path;
    size_t cert_path_len, key_path_len;

    ZEND_PARSE_PARAMETERS_START(2, 2)
        Z_PARAM_STRING(cert_path, cert_path_len)
        Z_PARAM_STRING(key_path, key_path_len)
    ZEND_PARSE_PARAMETERS_END();

    clickhouse_client_object *intern = Z_CLICKHOUSE_CLIENT_P(ZEND_THIS);

    if (!intern->conn) {
        zend_throw_exception(clickhouse_exception_ce, "Not connected", 0);
        return;
    }

#ifdef HAVE_OPENSSL
    clickhouse_connection_set_ssl_client_cert(intern->conn, cert_path, key_path);
#else
    zend_throw_exception(clickhouse_exception_ce,
        "SSL/TLS support not available (extension not compiled with OpenSSL)", 0);
#endif
}
/* }}} */

/* {{{ proto static bool ClickHouse\Client::sslAvailable() */
PHP_METHOD(ClickHouse_Client, sslAvailable) {
    ZEND_PARSE_PARAMETERS_NONE();

    RETURN_BOOL(clickhouse_ssl_available());
}
/* }}} */

/* {{{ proto bool ClickHouse\Client::isSSLConnected() */
PHP_METHOD(ClickHouse_Client, isSSLConnected) {
    ZEND_PARSE_PARAMETERS_NONE();

    clickhouse_client_object *intern = Z_CLICKHOUSE_CLIENT_P(ZEND_THIS);

    if (!intern->conn) {
        RETURN_FALSE;
    }

#ifdef HAVE_OPENSSL
    RETURN_BOOL(intern->conn->ssl != NULL);
#else
    RETURN_FALSE;
#endif
}
/* }}} */

/* {{{ proto array ClickHouse\Client::queryWithMeta(string $sql)
   Returns: ['data' => array, 'totals' => array|null, 'extremes' => array|null, 'progress' => array, 'profile' => array] */
PHP_METHOD(ClickHouse_Client, queryWithMeta) {
    char *sql;
    size_t sql_len;

    ZEND_PARSE_PARAMETERS_START(1, 1)
        Z_PARAM_STRING(sql, sql_len)
    ZEND_PARSE_PARAMETERS_END();

    clickhouse_client_object *intern = Z_CLICKHOUSE_CLIENT_P(ZEND_THIS);

    if (!intern->conn) {
        zend_throw_exception(clickhouse_exception_ce, "Not connected", 0);
        return;
    }

    clickhouse_result *result = NULL;
    int status;

    /* Apply query timeout if set */
    if (intern->query_timeout_ms > 0) {
        clickhouse_connection_set_query_timeout_ms(intern->conn, (int)intern->query_timeout_ms);
    }

    clickhouse_query_options *opts = clickhouse_query_options_create();
    if (opts) {
        opts->compression = intern->compression;
        if (intern->session_id) {
            opts->session_id = intern->session_id;
        }
        if (intern->default_query_id) {
            opts->query_id = intern->default_query_id;
        }
        /* Copy query settings from client to options */
        if (intern->query_settings) {
            clickhouse_setting *setting = intern->query_settings->head;
            while (setting) {
                clickhouse_query_options_set_setting(opts, setting->name, setting->value);
                setting = setting->next;
            }
        }
        status = clickhouse_connection_execute_query_ext(intern->conn, sql, opts, &result);
        /* Don't free session_id/query_id as they're borrowed from intern */
        opts->session_id = NULL;
        opts->query_id = NULL;
        clickhouse_query_options_free(opts);
    } else {
        status = clickhouse_connection_execute_query(intern->conn, sql, &result);
    }

    /* Reset timeout to default if it was set */
    if (intern->query_timeout_ms > 0) {
        clickhouse_connection_set_query_timeout_ms(intern->conn, 0);
    }

    if (status != 0) {
        const char *error = clickhouse_connection_get_error(intern->conn);
        if (result) {
            clickhouse_result_free(result);
        }
        zend_throw_exception(clickhouse_exception_ce, error ? error : "Query failed", 0);
        return;
    }

    /* Store query ID from result for tracking */
    if (intern->last_query_id) {
        efree(intern->last_query_id);
        intern->last_query_id = NULL;
    }
    if (result->query_id) {
        intern->last_query_id = estrdup(result->query_id);
    }

    array_init(return_value);

    /* Data rows */
    zval data_array;
    result_to_php_array(result, &data_array);
    add_assoc_zval(return_value, "data", &data_array);

    /* Totals */
    if (result->totals) {
        zval totals_array;
        block_to_php_array(result->totals, &totals_array);
        add_assoc_zval(return_value, "totals", &totals_array);
    } else {
        add_assoc_null(return_value, "totals");
    }

    /* Extremes */
    if (result->extremes) {
        zval extremes_array;
        block_to_php_array(result->extremes, &extremes_array);
        add_assoc_zval(return_value, "extremes", &extremes_array);
    } else {
        add_assoc_null(return_value, "extremes");
    }

    /* Progress info */
    zval progress_array;
    array_init(&progress_array);
    add_assoc_long(&progress_array, "rows", result->progress.rows);
    add_assoc_long(&progress_array, "bytes", result->progress.bytes);
    add_assoc_long(&progress_array, "total_rows", result->progress.total_rows);
    add_assoc_long(&progress_array, "written_rows", result->progress.written_rows);
    add_assoc_long(&progress_array, "written_bytes", result->progress.written_bytes);
    add_assoc_zval(return_value, "progress", &progress_array);

    /* Profile info */
    zval profile_array;
    array_init(&profile_array);
    add_assoc_long(&profile_array, "rows", result->profile.rows);
    add_assoc_long(&profile_array, "blocks", result->profile.blocks);
    add_assoc_long(&profile_array, "bytes", result->profile.bytes);
    add_assoc_bool(&profile_array, "applied_limit", result->profile.applied_limit);
    add_assoc_long(&profile_array, "rows_before_limit", result->profile.rows_before_limit);
    add_assoc_bool(&profile_array, "calculated_rows_before_limit", result->profile.calculated_rows_before_limit);
    add_assoc_zval(return_value, "profile", &profile_array);

    clickhouse_result_free(result);
}
/* }}} */

/* {{{ proto bool ClickHouse\Client::cancel()
   Cancel the currently running query */
PHP_METHOD(ClickHouse_Client, cancel) {
    ZEND_PARSE_PARAMETERS_NONE();

    clickhouse_client_object *intern = Z_CLICKHOUSE_CLIENT_P(ZEND_THIS);

    if (!intern->conn) {
        zend_throw_exception(clickhouse_exception_ce, "Not connected", 0);
        return;
    }

    int result = clickhouse_connection_cancel(intern->conn);
    RETURN_BOOL(result == 0);
}
/* }}} */

/* {{{ proto ClickHouse\Client ClickHouse\Client::fromDSN(string $dsn)
   Create a client from a DSN string.
   Format: clickhouse://[user[:password]@]host[:port][/database][?options]
   Options: ssl=1, compression=lz4|zstd|none, connect_timeout=N, read_timeout=N */
PHP_METHOD(ClickHouse_Client, fromDSN) {
    char *dsn;
    size_t dsn_len;

    ZEND_PARSE_PARAMETERS_START(1, 1)
        Z_PARAM_STRING(dsn, dsn_len)
    ZEND_PARSE_PARAMETERS_END();

    /* Parse the URL */
    php_url *url = php_url_parse_ex(dsn, dsn_len);
    if (!url) {
        zend_throw_exception(clickhouse_exception_ce, "Invalid DSN format", 0);
        return;
    }

    /* Validate scheme */
    if (!url->scheme || (strcmp(ZSTR_VAL(url->scheme), "clickhouse") != 0 &&
                          strcmp(ZSTR_VAL(url->scheme), "ch") != 0)) {
        php_url_free(url);
        zend_throw_exception(clickhouse_exception_ce,
            "DSN must start with clickhouse:// or ch://", 0);
        return;
    }

    /* Extract components */
    const char *host = url->host ? ZSTR_VAL(url->host) : "localhost";
    zend_long port = url->port ? url->port : 9000;
    const char *user = url->user ? ZSTR_VAL(url->user) : "default";
    const char *password = url->pass ? ZSTR_VAL(url->pass) : "";
    const char *database = "default";

    /* Extract database from path (skip leading /) */
    if (url->path && ZSTR_LEN(url->path) > 1) {
        database = ZSTR_VAL(url->path) + 1;
    }

    /* Create new Client object */
    object_init_ex(return_value, clickhouse_client_ce);
    clickhouse_client_object *intern = Z_CLICKHOUSE_CLIENT_P(return_value);

    /* Create connection */
    intern->conn = clickhouse_connection_create(host, (uint16_t)port, user, password, database);
    if (!intern->conn) {
        php_url_free(url);
        zend_throw_exception(clickhouse_exception_ce, "Failed to create connection", 0);
        return;
    }

    /* Save connection parameters for potential reconnection */
    intern->saved_host = estrdup(host);
    intern->saved_port = (uint16_t)port;
    intern->saved_user = estrdup(user);
    intern->saved_password = estrdup(password);
    intern->saved_database = estrdup(database);

    /* Parse query string options */
    if (url->query && ZSTR_LEN(url->query) > 0) {
        char *query_str = estrdup(ZSTR_VAL(url->query));
        char *token, *saveptr;

        token = strtok_r(query_str, "&", &saveptr);
        while (token) {
            char *eq = strchr(token, '=');
            if (eq) {
                *eq = '\0';
                char *key = token;
                char *value = eq + 1;

                if (strcmp(key, "ssl") == 0) {
                    if (strcmp(value, "1") == 0 || strcmp(value, "true") == 0) {
                        clickhouse_connection_set_ssl_enabled(intern->conn, 1);
                    }
                } else if (strcmp(key, "compression") == 0) {
                    if (strcmp(value, "lz4") == 0) {
                        intern->compression = 1;  /* CH_COMPRESS_LZ4 */
                    } else if (strcmp(value, "zstd") == 0) {
                        intern->compression = 2;  /* CH_COMPRESS_ZSTD */
                    } else if (strcmp(value, "none") == 0) {
                        intern->compression = 0;
                    }
                } else if (strcmp(key, "connect_timeout") == 0) {
                    clickhouse_connection_set_connect_timeout(intern->conn, atoi(value));
                } else if (strcmp(key, "read_timeout") == 0) {
                    clickhouse_connection_set_read_timeout(intern->conn, atoi(value));
                } else if (strcmp(key, "write_timeout") == 0) {
                    clickhouse_connection_set_write_timeout(intern->conn, atoi(value));
                }
            }
            token = strtok_r(NULL, "&", &saveptr);
        }
        efree(query_str);
    }

    php_url_free(url);

    /* Connect */
    if (clickhouse_connection_connect(intern->conn) != 0) {
        const char *error = clickhouse_connection_get_error(intern->conn);
        zend_throw_exception(clickhouse_exception_ce,
            error ? error : "Failed to connect", 0);
        return;
    }

    CLICKHOUSE_G(num_links)++;
}
/* }}} */

/* {{{ proto void ClickHouse\Client::setSession(?string $session_id)
   Set the session ID for stateful queries */
PHP_METHOD(ClickHouse_Client, setSession) {
    char *session_id = NULL;
    size_t session_id_len = 0;

    ZEND_PARSE_PARAMETERS_START(1, 1)
        Z_PARAM_STRING_OR_NULL(session_id, session_id_len)
    ZEND_PARSE_PARAMETERS_END();

    clickhouse_client_object *intern = Z_CLICKHOUSE_CLIENT_P(ZEND_THIS);

    /* Free existing session ID */
    if (intern->session_id) {
        efree(intern->session_id);
        intern->session_id = NULL;
    }

    /* Set new session ID */
    if (session_id && session_id_len > 0) {
        intern->session_id = estrndup(session_id, session_id_len);
    }
}
/* }}} */

/* {{{ proto ?string ClickHouse\Client::getSession()
   Get the current session ID */
PHP_METHOD(ClickHouse_Client, getSession) {
    ZEND_PARSE_PARAMETERS_NONE();

    clickhouse_client_object *intern = Z_CLICKHOUSE_CLIENT_P(ZEND_THIS);

    if (intern->session_id) {
        RETURN_STRING(intern->session_id);
    }
    RETURN_NULL();
}
/* }}} */

/* {{{ proto bool ClickHouse\Client::reconnect()
   Manually reconnect a dropped connection */
PHP_METHOD(ClickHouse_Client, reconnect) {
    ZEND_PARSE_PARAMETERS_NONE();

    clickhouse_client_object *intern = Z_CLICKHOUSE_CLIENT_P(ZEND_THIS);

    /* Check if we have saved connection parameters */
    if (!intern->saved_host || !intern->saved_user || !intern->saved_password || !intern->saved_database) {
        zend_throw_exception(clickhouse_exception_ce, "No saved connection parameters for reconnection", 0);
        RETURN_FALSE;
    }

    /* Close existing connection if any */
    if (intern->conn) {
        clickhouse_connection_close(intern->conn);
        clickhouse_connection_free(intern->conn);
        intern->conn = NULL;
    }

    /* Create new connection with saved parameters */
    intern->conn = clickhouse_connection_create(
        intern->saved_host,
        intern->saved_port,
        intern->saved_user,
        intern->saved_password,
        intern->saved_database
    );

    if (!intern->conn) {
        RETURN_FALSE;
    }

    int result = clickhouse_connection_connect(intern->conn);
    if (result != 0) {
        clickhouse_connection_free(intern->conn);
        intern->conn = NULL;
        RETURN_FALSE;
    }

    RETURN_TRUE;
}
/* }}} */

/* {{{ proto void ClickHouse\Client::setAutoReconnect(bool $enabled)
   Enable or disable automatic reconnection on query failure */
PHP_METHOD(ClickHouse_Client, setAutoReconnect) {
    zend_bool enabled;

    ZEND_PARSE_PARAMETERS_START(1, 1)
        Z_PARAM_BOOL(enabled)
    ZEND_PARSE_PARAMETERS_END();

    clickhouse_client_object *intern = Z_CLICKHOUSE_CLIENT_P(ZEND_THIS);
    intern->auto_reconnect = enabled;
}
/* }}} */

/* {{{ proto bool ClickHouse\Client::getAutoReconnect()
   Get the auto-reconnect setting */
PHP_METHOD(ClickHouse_Client, getAutoReconnect) {
    ZEND_PARSE_PARAMETERS_NONE();

    clickhouse_client_object *intern = Z_CLICKHOUSE_CLIENT_P(ZEND_THIS);
    RETURN_BOOL(intern->auto_reconnect);
}
/* }}} */

/* {{{ proto void ClickHouse\Client::setMaxRetryAttempts(int $max_attempts)
   Set maximum retry attempts for reconnection (0 = use default) */
PHP_METHOD(ClickHouse_Client, setMaxRetryAttempts) {
    zend_long max_attempts;

    ZEND_PARSE_PARAMETERS_START(1, 1)
        Z_PARAM_LONG(max_attempts)
    ZEND_PARSE_PARAMETERS_END();

    clickhouse_client_object *intern = Z_CLICKHOUSE_CLIENT_P(ZEND_THIS);

    if (max_attempts < 0) {
        zend_throw_exception(clickhouse_exception_ce, "Max retry attempts must be >= 0", 0);
        return;
    }

    intern->max_retry_attempts = max_attempts;
}
/* }}} */

/* {{{ proto int ClickHouse\Client::getMaxRetryAttempts()
   Get maximum retry attempts setting */
PHP_METHOD(ClickHouse_Client, getMaxRetryAttempts) {
    ZEND_PARSE_PARAMETERS_NONE();

    clickhouse_client_object *intern = Z_CLICKHOUSE_CLIENT_P(ZEND_THIS);
    RETURN_LONG(intern->max_retry_attempts);
}
/* }}} */

/* {{{ proto void ClickHouse\Client::setRetryDelay(float $base_delay, float $max_delay)
   Set retry delay configuration (base delay and max delay in seconds) */
PHP_METHOD(ClickHouse_Client, setRetryDelay) {
    double base_delay, max_delay;

    ZEND_PARSE_PARAMETERS_START(2, 2)
        Z_PARAM_DOUBLE(base_delay)
        Z_PARAM_DOUBLE(max_delay)
    ZEND_PARSE_PARAMETERS_END();

    clickhouse_client_object *intern = Z_CLICKHOUSE_CLIENT_P(ZEND_THIS);

    if (base_delay < 0 || max_delay < 0) {
        zend_throw_exception(clickhouse_exception_ce, "Retry delays must be >= 0", 0);
        return;
    }

    if (base_delay > max_delay) {
        zend_throw_exception(clickhouse_exception_ce, "Base delay cannot exceed max delay", 0);
        return;
    }

    intern->retry_base_delay = base_delay;
    intern->retry_max_delay = max_delay;
}
/* }}} */

/* {{{ proto array ClickHouse\Client::getRetryDelay()
   Get retry delay configuration */
PHP_METHOD(ClickHouse_Client, getRetryDelay) {
    ZEND_PARSE_PARAMETERS_NONE();

    clickhouse_client_object *intern = Z_CLICKHOUSE_CLIENT_P(ZEND_THIS);

    array_init(return_value);
    add_assoc_double(return_value, "base_delay", intern->retry_base_delay);
    add_assoc_double(return_value, "max_delay", intern->retry_max_delay);
}
/* }}} */

/* {{{ proto void ClickHouse\Client::setRetryJitter(bool $enabled)
   Enable or disable jitter in retry delays */
PHP_METHOD(ClickHouse_Client, setRetryJitter) {
    zend_bool enabled;

    ZEND_PARSE_PARAMETERS_START(1, 1)
        Z_PARAM_BOOL(enabled)
    ZEND_PARSE_PARAMETERS_END();

    clickhouse_client_object *intern = Z_CLICKHOUSE_CLIENT_P(ZEND_THIS);
    intern->retry_jitter = enabled;
}
/* }}} */

/* {{{ proto bool ClickHouse\Client::getRetryJitter()
   Get retry jitter setting */
PHP_METHOD(ClickHouse_Client, getRetryJitter) {
    ZEND_PARSE_PARAMETERS_NONE();

    clickhouse_client_object *intern = Z_CLICKHOUSE_CLIENT_P(ZEND_THIS);
    RETURN_BOOL(intern->retry_jitter);
}
/* }}} */

/* {{{ proto int ClickHouse\Client::getTotalRetryAttempts()
   Get total number of retry attempts made (for metrics) */
PHP_METHOD(ClickHouse_Client, getTotalRetryAttempts) {
    ZEND_PARSE_PARAMETERS_NONE();

    clickhouse_client_object *intern = Z_CLICKHOUSE_CLIENT_P(ZEND_THIS);
    RETURN_LONG(intern->total_retry_attempts);
}
/* }}} */

/* {{{ proto void ClickHouse\Client::resetRetryMetrics()
   Reset retry attempt counter */
PHP_METHOD(ClickHouse_Client, resetRetryMetrics) {
    ZEND_PARSE_PARAMETERS_NONE();

    clickhouse_client_object *intern = Z_CLICKHOUSE_CLIENT_P(ZEND_THIS);
    intern->total_retry_attempts = 0;
}
/* }}} */

/* {{{ proto void ClickHouse\Client::setQueryId(?string $query_id)
   Set the default query ID prefix */
PHP_METHOD(ClickHouse_Client, setQueryId) {
    char *query_id = NULL;
    size_t query_id_len = 0;

    ZEND_PARSE_PARAMETERS_START(1, 1)
        Z_PARAM_STRING_OR_NULL(query_id, query_id_len)
    ZEND_PARSE_PARAMETERS_END();

    clickhouse_client_object *intern = Z_CLICKHOUSE_CLIENT_P(ZEND_THIS);

    /* Free existing query ID */
    if (intern->default_query_id) {
        efree(intern->default_query_id);
        intern->default_query_id = NULL;
    }

    /* Set new query ID */
    if (query_id && query_id_len > 0) {
        intern->default_query_id = estrndup(query_id, query_id_len);
    }
}
/* }}} */

/* {{{ proto ?string ClickHouse\Client::getQueryId()
   Get the current default query ID prefix */
PHP_METHOD(ClickHouse_Client, getQueryId) {
    ZEND_PARSE_PARAMETERS_NONE();

    clickhouse_client_object *intern = Z_CLICKHOUSE_CLIENT_P(ZEND_THIS);

    if (intern->default_query_id) {
        RETURN_STRING(intern->default_query_id);
    }
    RETURN_NULL();
}
/* }}} */

/* {{{ proto ?string ClickHouse\Client::getLastQueryId()
   Get the query ID from the last executed query */
PHP_METHOD(ClickHouse_Client, getLastQueryId) {
    ZEND_PARSE_PARAMETERS_NONE();

    clickhouse_client_object *intern = Z_CLICKHOUSE_CLIENT_P(ZEND_THIS);

    if (intern->last_query_id) {
        RETURN_STRING(intern->last_query_id);
    }
    RETURN_NULL();
}
/* }}} */

/* {{{ proto void ClickHouse\Client::enableMetrics()
   Enable query metrics collection */
PHP_METHOD(ClickHouse_Client, enableMetrics) {
    ZEND_PARSE_PARAMETERS_NONE();

    clickhouse_client_object *intern = Z_CLICKHOUSE_CLIENT_P(ZEND_THIS);
    intern->metrics_enabled = 1;
}
/* }}} */

/* {{{ proto void ClickHouse\Client::disableMetrics()
   Disable query metrics collection */
PHP_METHOD(ClickHouse_Client, disableMetrics) {
    ZEND_PARSE_PARAMETERS_NONE();

    clickhouse_client_object *intern = Z_CLICKHOUSE_CLIENT_P(ZEND_THIS);
    intern->metrics_enabled = 0;
}
/* }}} */

/* {{{ proto array ClickHouse\Client::getMetrics()
   Get collected query metrics */
PHP_METHOD(ClickHouse_Client, getMetrics) {
    ZEND_PARSE_PARAMETERS_NONE();

    clickhouse_client_object *intern = Z_CLICKHOUSE_CLIENT_P(ZEND_THIS);

    array_init(return_value);
    add_assoc_bool(return_value, "enabled", intern->metrics_enabled);
    add_assoc_long(return_value, "queries_executed", intern->queries_executed);
    add_assoc_long(return_value, "queries_failed", intern->queries_failed);
    add_assoc_double(return_value, "total_query_time", intern->total_query_time);
    add_assoc_long(return_value, "total_rows_read", intern->total_rows_read);
    add_assoc_long(return_value, "total_bytes_read", intern->total_bytes_read);
    add_assoc_long(return_value, "slow_queries", intern->slow_queries);
    add_assoc_double(return_value, "slow_query_threshold", intern->slow_query_threshold);

    /* Calculate average query time if we have executed queries */
    if (intern->queries_executed > 0) {
        add_assoc_double(return_value, "avg_query_time", intern->total_query_time / intern->queries_executed);
    } else {
        add_assoc_double(return_value, "avg_query_time", 0.0);
    }
}
/* }}} */

/* {{{ proto void ClickHouse\Client::resetMetrics()
   Reset all query metrics counters */
PHP_METHOD(ClickHouse_Client, resetMetrics) {
    ZEND_PARSE_PARAMETERS_NONE();

    clickhouse_client_object *intern = Z_CLICKHOUSE_CLIENT_P(ZEND_THIS);

    intern->queries_executed = 0;
    intern->queries_failed = 0;
    intern->total_query_time = 0.0;
    intern->total_rows_read = 0;
    intern->total_bytes_read = 0;
    intern->slow_queries = 0;
}
/* }}} */

/* {{{ proto void ClickHouse\Client::setSlowQueryThreshold(float $seconds)
   Set the threshold in seconds for slow query detection (0 to disable) */
PHP_METHOD(ClickHouse_Client, setSlowQueryThreshold) {
    double threshold;

    ZEND_PARSE_PARAMETERS_START(1, 1)
        Z_PARAM_DOUBLE(threshold)
    ZEND_PARSE_PARAMETERS_END();

    clickhouse_client_object *intern = Z_CLICKHOUSE_CLIENT_P(ZEND_THIS);

    if (threshold < 0) {
        zend_throw_exception(clickhouse_exception_ce, "Slow query threshold must be >= 0", 0);
        return;
    }

    intern->slow_query_threshold = threshold;
}
/* }}} */

/* {{{ proto void ClickHouse\Client::setQuerySetting(string $name, string $value)
   Set a query setting to apply to all subsequent queries */
PHP_METHOD(ClickHouse_Client, setQuerySetting) {
    char *name, *value;
    size_t name_len, value_len;

    ZEND_PARSE_PARAMETERS_START(2, 2)
        Z_PARAM_STRING(name, name_len)
        Z_PARAM_STRING(value, value_len)
    ZEND_PARSE_PARAMETERS_END();

    clickhouse_client_object *intern = Z_CLICKHOUSE_CLIENT_P(ZEND_THIS);

    /* Create settings collection if it doesn't exist */
    if (!intern->query_settings) {
        intern->query_settings = clickhouse_settings_create();
        if (!intern->query_settings) {
            zend_throw_exception(clickhouse_exception_ce, "Failed to create settings collection", 0);
            return;
        }
    }

    /* Add or update the setting - use flags=1 for "custom" (user-defined) setting */
    if (clickhouse_settings_add(intern->query_settings, name, value, 1) != 0) {
        zend_throw_exception(clickhouse_exception_ce, "Failed to add query setting", 0);
        return;
    }
}
/* }}} */

/* {{{ proto void ClickHouse\Client::clearQuerySettings()
   Clear all query settings */
PHP_METHOD(ClickHouse_Client, clearQuerySettings) {
    ZEND_PARSE_PARAMETERS_NONE();

    clickhouse_client_object *intern = Z_CLICKHOUSE_CLIENT_P(ZEND_THIS);

    if (intern->query_settings) {
        clickhouse_settings_free(intern->query_settings);
        intern->query_settings = NULL;
    }
}
/* }}} */

/* {{{ proto array ClickHouse\Client::queryWithTable(string $sql, string $table_name, array $columns, array $rows)
   Execute query with an external table. Columns: ['col_name' => 'Type', ...], Rows: [[val1, val2], ...] */
PHP_METHOD(ClickHouse_Client, queryWithTable) {
    char *sql, *table_name;
    size_t sql_len, table_name_len;
    zval *columns, *rows;

    ZEND_PARSE_PARAMETERS_START(4, 4)
        Z_PARAM_STRING(sql, sql_len)
        Z_PARAM_STRING(table_name, table_name_len)
        Z_PARAM_ARRAY(columns)
        Z_PARAM_ARRAY(rows)
    ZEND_PARSE_PARAMETERS_END();

    clickhouse_client_object *intern = Z_CLICKHOUSE_CLIENT_P(ZEND_THIS);

    if (!intern->conn) {
        zend_throw_exception(clickhouse_exception_ce, "Not connected", 0);
        return;
    }

    /* Count columns and rows */
    size_t col_count = zend_hash_num_elements(Z_ARRVAL_P(columns));
    size_t row_count = zend_hash_num_elements(Z_ARRVAL_P(rows));

    if (col_count == 0) {
        zend_throw_exception(clickhouse_exception_ce, "External table must have at least one column", 0);
        return;
    }

    /* Create external table */
    clickhouse_external_table *ext_table = clickhouse_external_table_create(table_name);
    if (!ext_table) {
        zend_throw_exception(clickhouse_exception_ce, "Failed to create external table", 0);
        return;
    }
    ext_table->row_count = row_count;

    /* Add columns from PHP array */
    zend_string *col_name;
    zval *col_type;
    ZEND_HASH_FOREACH_STR_KEY_VAL(Z_ARRVAL_P(columns), col_name, col_type) {
        if (!col_name || Z_TYPE_P(col_type) != IS_STRING) {
            clickhouse_external_table_free(ext_table);
            zend_throw_exception(clickhouse_exception_ce, "Column definition must be ['name' => 'Type']", 0);
            return;
        }
        if (clickhouse_external_table_add_column(ext_table, ZSTR_VAL(col_name), Z_STRVAL_P(col_type)) != 0) {
            clickhouse_external_table_free(ext_table);
            zend_throw_exception(clickhouse_exception_ce, "Failed to add column to external table", 0);
            return;
        }
    } ZEND_HASH_FOREACH_END();

    /* Allocate column data arrays (all as strings for simplicity) */
    for (size_t i = 0; i < ext_table->column_count; i++) {
        ext_table->columns[i].data = ecalloc(row_count, sizeof(char *));
        ext_table->columns[i].row_count = row_count;
    }

    /* Fill column data from rows */
    size_t row_idx = 0;
    zval *row_val;
    ZEND_HASH_FOREACH_VAL(Z_ARRVAL_P(rows), row_val) {
        if (Z_TYPE_P(row_val) != IS_ARRAY) {
            /* Free allocated data */
            for (size_t i = 0; i < ext_table->column_count; i++) {
                char **data = (char **)ext_table->columns[i].data;
                for (size_t j = 0; j < row_idx; j++) {
                    if (data[j]) efree(data[j]);
                }
                efree(data);
                ext_table->columns[i].data = NULL;
            }
            clickhouse_external_table_free(ext_table);
            zend_throw_exception(clickhouse_exception_ce, "Each row must be an array", 0);
            return;
        }

        /* Get values for each column */
        size_t col_idx = 0;
        zval *cell;
        ZEND_HASH_FOREACH_VAL(Z_ARRVAL_P(row_val), cell) {
            if (col_idx >= ext_table->column_count) break;

            char **col_data = (char **)ext_table->columns[col_idx].data;
            zend_string *str_val = zval_get_string(cell);
            col_data[row_idx] = estrndup(ZSTR_VAL(str_val), ZSTR_LEN(str_val));
            zend_string_release(str_val);

            col_idx++;
        } ZEND_HASH_FOREACH_END();

        row_idx++;
        if (row_idx >= row_count) break;
    } ZEND_HASH_FOREACH_END();

    /* Create query options with external table */
    clickhouse_query_options *opts = clickhouse_query_options_create();
    if (!opts) {
        for (size_t i = 0; i < ext_table->column_count; i++) {
            char **data = (char **)ext_table->columns[i].data;
            for (size_t j = 0; j < row_count; j++) {
                if (data[j]) efree(data[j]);
            }
            efree(data);
            ext_table->columns[i].data = NULL;
        }
        clickhouse_external_table_free(ext_table);
        zend_throw_exception(clickhouse_exception_ce, "Failed to create query options", 0);
        return;
    }

    opts->compression = intern->compression;
    if (intern->session_id) {
        opts->session_id = intern->session_id;
    }
    if (intern->default_query_id) {
        opts->query_id = intern->default_query_id;
    }
    /* Copy query settings from client to options */
    if (intern->query_settings) {
        clickhouse_setting *setting = intern->query_settings->head;
        while (setting) {
            clickhouse_query_options_set_setting(opts, setting->name, setting->value);
            setting = setting->next;
        }
    }

    /* Create external tables collection */
    opts->external_tables = clickhouse_external_tables_create();
    if (!opts->external_tables) {
        opts->session_id = NULL;
        opts->query_id = NULL;
        clickhouse_query_options_free(opts);
        for (size_t i = 0; i < ext_table->column_count; i++) {
            char **data = (char **)ext_table->columns[i].data;
            for (size_t j = 0; j < row_count; j++) {
                if (data[j]) efree(data[j]);
            }
            efree(data);
            ext_table->columns[i].data = NULL;
        }
        clickhouse_external_table_free(ext_table);
        zend_throw_exception(clickhouse_exception_ce, "Failed to create external tables", 0);
        return;
    }

    clickhouse_external_tables_add(opts->external_tables, ext_table);

    /* Apply query timeout if set */
    if (intern->query_timeout_ms > 0) {
        clickhouse_connection_set_query_timeout_ms(intern->conn, (int)intern->query_timeout_ms);
    }

    /* Execute query */
    clickhouse_result *result = NULL;
    int status = clickhouse_connection_execute_query_ext(intern->conn, sql, opts, &result);

    /* Reset timeout to default if it was set */
    if (intern->query_timeout_ms > 0) {
        clickhouse_connection_set_query_timeout_ms(intern->conn, 0);
    }

    /* Free column data (strings allocated by PHP) */
    for (size_t i = 0; i < ext_table->column_count; i++) {
        char **data = (char **)ext_table->columns[i].data;
        if (data) {
            for (size_t j = 0; j < row_count; j++) {
                if (data[j]) efree(data[j]);
            }
            efree(data);
        }
        ext_table->columns[i].data = NULL;
    }

    /* Clean up - don't free borrowed session_id/query_id */
    opts->session_id = NULL;
    opts->query_id = NULL;
    clickhouse_query_options_free(opts);

    if (status != 0) {
        const char *error = clickhouse_connection_get_error(intern->conn);
        if (result) {
            clickhouse_result_free(result);
        }
        zend_throw_exception(clickhouse_exception_ce, error ? error : "Query with external table failed", 0);
        return;
    }

    /* Store query ID from result for tracking */
    if (intern->last_query_id) {
        efree(intern->last_query_id);
        intern->last_query_id = NULL;
    }
    if (result && result->query_id) {
        intern->last_query_id = estrdup(result->query_id);
    }

    result_to_php_array(result, return_value);
    clickhouse_result_free(result);
}
/* }}} */

/* {{{ proto ClickHouse\Statement ClickHouse\Client::prepare(string $sql) */
PHP_METHOD(ClickHouse_Client, prepare) {
    char *sql;
    size_t sql_len;

    ZEND_PARSE_PARAMETERS_START(1, 1)
        Z_PARAM_STRING(sql, sql_len)
    ZEND_PARSE_PARAMETERS_END();

    clickhouse_client_object *client = Z_CLICKHOUSE_CLIENT_P(ZEND_THIS);

    if (!client->conn) {
        zend_throw_exception(clickhouse_exception_ce, "Not connected", 0);
        return;
    }

    /* Create statement object */
    object_init_ex(return_value, clickhouse_statement_ce);
    clickhouse_statement_object *stmt = Z_CLICKHOUSE_STATEMENT_P(return_value);

    /* Store reference to client */
    ZVAL_COPY(&stmt->client_zv, ZEND_THIS);
    stmt->conn = client->conn;

    /* Store query */
    stmt->query = estrndup(sql, sql_len);

    /* Create query options */
    stmt->opts = clickhouse_query_options_create();
    if (!stmt->opts) {
        zend_throw_exception(clickhouse_exception_ce, "Failed to create query options", 0);
        return;
    }
}
/* }}} */

/* ========================= Statement class methods ========================= */

/* {{{ proto ClickHouse\Statement ClickHouse\Statement::bind(string $name, mixed $value, string $type = "String") */
PHP_METHOD(ClickHouse_Statement, bind) {
    char *name;
    size_t name_len;
    zval *value;
    char *type = "String";
    size_t type_len = sizeof("String") - 1;

    ZEND_PARSE_PARAMETERS_START(2, 3)
        Z_PARAM_STRING(name, name_len)
        Z_PARAM_ZVAL(value)
        Z_PARAM_OPTIONAL
        Z_PARAM_STRING(type, type_len)
    ZEND_PARSE_PARAMETERS_END();

    clickhouse_statement_object *stmt = Z_CLICKHOUSE_STATEMENT_P(ZEND_THIS);

    if (!stmt->opts) {
        zend_throw_exception(clickhouse_exception_ce, "Statement not initialized", 0);
        return;
    }

    /* Convert value to string */
    zend_string *str_val = zval_get_string(value);
    if (!str_val) {
        zend_throw_exception(clickhouse_exception_ce, "Failed to convert value to string", 0);
        return;
    }

    /* Add parameter */
    if (clickhouse_query_options_set_param(stmt->opts, name, ZSTR_VAL(str_val), type) != 0) {
        zend_string_release(str_val);
        zend_throw_exception(clickhouse_exception_ce, "Failed to bind parameter", 0);
        return;
    }

    zend_string_release(str_val);

    /* Return $this for chaining */
    RETURN_ZVAL(ZEND_THIS, 1, 0);
}
/* }}} */

/* {{{ proto ClickHouse\Statement ClickHouse\Statement::setOption(string $name, string $value) */
PHP_METHOD(ClickHouse_Statement, setOption) {
    char *name;
    size_t name_len;
    char *value;
    size_t value_len;

    ZEND_PARSE_PARAMETERS_START(2, 2)
        Z_PARAM_STRING(name, name_len)
        Z_PARAM_STRING(value, value_len)
    ZEND_PARSE_PARAMETERS_END();

    clickhouse_statement_object *stmt = Z_CLICKHOUSE_STATEMENT_P(ZEND_THIS);

    if (!stmt->opts) {
        zend_throw_exception(clickhouse_exception_ce, "Statement not initialized", 0);
        return;
    }

    if (clickhouse_query_options_set_setting(stmt->opts, name, value) != 0) {
        zend_throw_exception(clickhouse_exception_ce, "Failed to set option", 0);
        return;
    }

    /* Return $this for chaining */
    RETURN_ZVAL(ZEND_THIS, 1, 0);
}
/* }}} */

/* Helper: Substitute parameters in query string using PHP-level replacement */
static char *substitute_params(const char *query, clickhouse_params *params) {
    if (!params || params->count == 0 || !query) {
        return estrdup(query);
    }

    smart_str result = {0};
    const char *p = query;
    size_t position = 0;  /* Track position in original query */

    while (*p) {
        /* Only support {param} and {param:Type} ClickHouse-native syntax */
        if (*p == '{') {
            const char *start = p + 1;
            const char *colon = NULL;
            const char *end = NULL;
            size_t param_start_pos = position;  /* Remember where parameter started */

            /* {param} or {param:Type} style */
            for (const char *c = start; *c && *c != '}'; c++) {
                if (*c == ':') colon = c;
            }
            if (*start) {
                for (end = start; *end && *end != '}'; end++);
            }
            if (!end || *end != '}') {
                /* Not a valid {param} pattern */
                smart_str_appendc(&result, *p);
                p++;
                position++;
                continue;
            }

            /* Extract parameter name (before colon if present, otherwise full name) */
            size_t name_len = colon ? (colon - start) : (end - start);
            char *name = estrndup(start, name_len);

            /* Look up parameter */
            clickhouse_param *param = params->head;
            const char *value = NULL;
            const char *type = NULL;
            int found = 0;
            while (param) {
                if (strcmp(param->name, name) == 0) {
                    value = param->value;
                    type = param->type;
                    found = 1;
                    break;
                }
                param = param->next;
            }

            if (!found) {
                /* Parameter not found - build helpful error */
                smart_str error_msg = {0};
                smart_str_appends(&error_msg, "Parameter '");
                smart_str_appends(&error_msg, name);
                smart_str_appends(&error_msg, "' not found in provided parameters.\n");

                /* List available parameters */
                if (params->count > 0) {
                    smart_str_appends(&error_msg, "  Available parameters: ");
                    param = params->head;
                    int first = 1;
                    while (param) {
                        if (!first) smart_str_appends(&error_msg, ", ");
                        smart_str_appends(&error_msg, "'");
                        smart_str_appends(&error_msg, param->name);
                        smart_str_appends(&error_msg, "'");
                        first = 0;
                        param = param->next;
                    }
                } else {
                    smart_str_appends(&error_msg, "  No parameters provided");
                }

                smart_str_0(&error_msg);
                throw_param_error(name, query, param_start_pos, ZSTR_VAL(error_msg.s));
                efree(name);
                smart_str_free(&error_msg);
                smart_str_free(&result);
                return NULL;
            }

            if (value) {
                /* Format value based on type */
                if (type && strncmp(type, "Array", 5) == 0) {
                    /* Array type - value is already formatted as [1,2,3] or ['a','b','c'] */
                    smart_str_appends(&result, value);
                } else if (type && (strncmp(type, "String", 6) == 0 ||
                             strncmp(type, "FixedString", 11) == 0 ||
                             strncmp(type, "Nullable", 8) == 0 ||
                             strncmp(type, "UUID", 4) == 0 ||
                             strncmp(type, "IPv4", 4) == 0 ||
                             strncmp(type, "IPv6", 4) == 0 ||
                             strncmp(type, "Date", 4) == 0 ||
                             strncmp(type, "DateTime", 8) == 0)) {
                    /* String-like types - add quotes and escape */
                    smart_str_appendc(&result, '\'');
                    for (const char *v = value; *v; v++) {
                        if (*v == '\'') smart_str_appendc(&result, '\'');
                        smart_str_appendc(&result, *v);
                    }
                    smart_str_appendc(&result, '\'');
                } else {
                    /* Numeric/other type (including Decimal) - use as-is */
                    smart_str_appends(&result, value);
                }
                position += (end - p) + 1;
                p = end + 1;
                efree(name);
                continue;
            }

            efree(name);
        }
        smart_str_appendc(&result, *p);
        p++;
        position++;
    }

    smart_str_0(&result);
    char *ret = estrdup(ZSTR_VAL(result.s));
    smart_str_free(&result);
    return ret;
}

/* {{{ proto array ClickHouse\Statement::execute() */
PHP_METHOD(ClickHouse_Statement, execute) {
    ZEND_PARSE_PARAMETERS_NONE();

    clickhouse_statement_object *stmt = Z_CLICKHOUSE_STATEMENT_P(ZEND_THIS);

    if (!stmt->conn || !stmt->query) {
        zend_throw_exception(clickhouse_exception_ce, "Statement not initialized", 0);
        return;
    }

    /* Substitute parameters in query */
    char *final_query = stmt->query;
    if (stmt->opts && stmt->opts->params && stmt->opts->params->count > 0) {
        final_query = substitute_params(stmt->query, stmt->opts->params);
        if (!final_query) {
            /* Error already thrown by substitute_params */
            return;
        }
    }

    clickhouse_result *result = NULL;
    int status = clickhouse_connection_execute_query(stmt->conn, final_query, &result);

    if (status != 0) {
        const char *error = clickhouse_connection_get_error(stmt->conn);
        if (result) {
            clickhouse_result_free(result);
        }
        /* Use enhanced error message with query context */
        throw_query_error(error, stmt->query, final_query);
        if (final_query != stmt->query) {
            efree(final_query);
        }
        return;
    }

    if (final_query != stmt->query) {
        efree(final_query);
    }

    result_to_php_array(result, return_value);
    clickhouse_result_free(result);
}
/* }}} */

/* {{{ proto array ClickHouse\Statement::fetchAll() - alias for execute() */
PHP_METHOD(ClickHouse_Statement, fetchAll) {
    ZEND_PARSE_PARAMETERS_NONE();

    clickhouse_statement_object *stmt = Z_CLICKHOUSE_STATEMENT_P(ZEND_THIS);

    if (!stmt->conn || !stmt->query) {
        zend_throw_exception(clickhouse_exception_ce, "Statement not initialized", 0);
        return;
    }

    /* Substitute parameters in query */
    char *final_query = stmt->query;
    if (stmt->opts && stmt->opts->params && stmt->opts->params->count > 0) {
        final_query = substitute_params(stmt->query, stmt->opts->params);
        if (!final_query) {
            /* Error already thrown by substitute_params */
            return;
        }
    }

    clickhouse_result *result = NULL;
    int status = clickhouse_connection_execute_query(stmt->conn, final_query, &result);

    if (status != 0) {
        const char *error = clickhouse_connection_get_error(stmt->conn);
        if (result) {
            clickhouse_result_free(result);
        }
        /* Use enhanced error message with query context */
        throw_query_error(error, stmt->query, final_query);
        if (final_query != stmt->query) {
            efree(final_query);
        }
        return;
    }

    if (final_query != stmt->query) {
        efree(final_query);
    }

    result_to_php_array(result, return_value);
    clickhouse_result_free(result);
}
/* }}} */

/* {{{ proto AsyncResult ClickHouse\Client::queryAsync(string $sql)
   Start async query and return AsyncResult object */
PHP_METHOD(ClickHouse_Client, queryAsync) {
    char *sql;
    size_t sql_len;

    ZEND_PARSE_PARAMETERS_START(1, 1)
        Z_PARAM_STRING(sql, sql_len)
    ZEND_PARSE_PARAMETERS_END();

    clickhouse_client_object *intern = Z_CLICKHOUSE_CLIENT_P(ZEND_THIS);

    if (!intern->conn || intern->conn->state != CONN_STATE_AUTHENTICATED) {
        zend_throw_exception(clickhouse_exception_ce, "Not connected", 0);
        return;
    }

    /* Apply query timeout if set */
    if (intern->query_timeout_ms > 0) {
        clickhouse_connection_set_query_timeout_ms(intern->conn, (int)intern->query_timeout_ms);
    }

    /* Start async query */
    clickhouse_async_query *async = NULL;
    if (clickhouse_connection_query_async(intern->conn, sql, NULL, &async) != 0) {
        const char *error = clickhouse_connection_get_error(intern->conn);
        /* Reset timeout to default if it was set */
        if (intern->query_timeout_ms > 0) {
            clickhouse_connection_set_query_timeout_ms(intern->conn, 0);
        }
        zend_throw_exception(clickhouse_exception_ce, error ? error : "Failed to start async query", 0);
        return;
    }

    /* Create AsyncResult object */
    object_init_ex(return_value, clickhouse_asyncresult_ce);
    clickhouse_asyncresult_object *result_obj = Z_CLICKHOUSE_ASYNCRESULT_P(return_value);
    result_obj->async = async;
    result_obj->conn = intern->conn;
    ZVAL_COPY(&result_obj->client_zv, ZEND_THIS);
}
/* }}} */

/* {{{ proto bool ClickHouse\AsyncResult::poll(int $timeout_ms = 0)
   Check if result is ready (non-blocking with optional timeout) */
PHP_METHOD(ClickHouse_AsyncResult, poll) {
    zend_long timeout_ms = 0;

    ZEND_PARSE_PARAMETERS_START(0, 1)
        Z_PARAM_OPTIONAL
        Z_PARAM_LONG(timeout_ms)
    ZEND_PARSE_PARAMETERS_END();

    clickhouse_asyncresult_object *intern = Z_CLICKHOUSE_ASYNCRESULT_P(ZEND_THIS);

    if (intern->has_result) {
        RETURN_TRUE;
    }

    if (!intern->async || !intern->conn) {
        zend_throw_exception(clickhouse_exception_ce, "Invalid async result", 0);
        return;
    }

    /* Check if data is available */
    int has_data = clickhouse_connection_has_data(intern->conn, (int)timeout_ms);
    RETURN_BOOL(has_data > 0);
}
/* }}} */

/* {{{ proto bool ClickHouse\AsyncResult::isReady()
   Check if result is ready (non-blocking) */
PHP_METHOD(ClickHouse_AsyncResult, isReady) {
    ZEND_PARSE_PARAMETERS_NONE();

    clickhouse_asyncresult_object *intern = Z_CLICKHOUSE_ASYNCRESULT_P(ZEND_THIS);

    if (intern->has_result) {
        RETURN_TRUE;
    }

    if (!intern->async || !intern->conn) {
        RETURN_FALSE;
    }

    int has_data = clickhouse_connection_has_data(intern->conn, 0);
    RETURN_BOOL(has_data > 0);
}
/* }}} */

/* {{{ proto array ClickHouse\AsyncResult::wait()
   Wait for result and return it */
PHP_METHOD(ClickHouse_AsyncResult, wait) {
    ZEND_PARSE_PARAMETERS_NONE();

    clickhouse_asyncresult_object *intern = Z_CLICKHOUSE_ASYNCRESULT_P(ZEND_THIS);

    if (intern->has_result) {
        ZVAL_COPY(return_value, &intern->cached_result);
        return;
    }

    if (!intern->async || !intern->conn) {
        zend_throw_exception(clickhouse_exception_ce, "Invalid async result", 0);
        return;
    }

    /* Poll until complete */
    while (1) {
        int status = clickhouse_async_poll(intern->conn, intern->async);
        if (status < 0) {
            const char *error = intern->async->error ? intern->async->error :
                               clickhouse_connection_get_error(intern->conn);
            zend_throw_exception(clickhouse_exception_ce, error ? error : "Async query failed", 0);
            return;
        }
        if (status > 0) {
            break; /* Complete */
        }
        /* Wait a bit before polling again */
        usleep(1000); /* 1ms */
    }

    /* Convert result to PHP array */
    if (intern->async->result) {
        result_to_php_array(intern->async->result, return_value);
        /* Cache the result */
        ZVAL_COPY(&intern->cached_result, return_value);
        intern->has_result = 1;
    } else {
        array_init(return_value);
    }
}
/* }}} */

/* {{{ proto array ClickHouse\AsyncResult::getResult()
   Get cached result (must call wait() or poll() first) */
PHP_METHOD(ClickHouse_AsyncResult, getResult) {
    ZEND_PARSE_PARAMETERS_NONE();

    clickhouse_asyncresult_object *intern = Z_CLICKHOUSE_ASYNCRESULT_P(ZEND_THIS);

    if (intern->has_result) {
        ZVAL_COPY(return_value, &intern->cached_result);
        return;
    }

    /* Not ready yet - call wait() internally */
    zim_ClickHouse_AsyncResult_wait(INTERNAL_FUNCTION_PARAM_PASSTHRU);
}
/* }}} */

/* {{{ proto bool ClickHouse\AsyncResult::cancel()
   Cancel a running async query */
PHP_METHOD(ClickHouse_AsyncResult, cancel) {
    ZEND_PARSE_PARAMETERS_NONE();

    clickhouse_asyncresult_object *intern = Z_CLICKHOUSE_ASYNCRESULT_P(ZEND_THIS);

    /* If already complete or has result, nothing to cancel */
    if (intern->has_result || !intern->async) {
        RETURN_FALSE;
    }

    /* If async is complete or errored, nothing to cancel */
    if (intern->async->state == ASYNC_STATE_COMPLETE || intern->async->state == ASYNC_STATE_ERROR) {
        RETURN_FALSE;
    }

    /* Send cancel to the server */
    if (clickhouse_connection_cancel(intern->conn) != 0) {
        zend_throw_exception(clickhouse_exception_ce,
            clickhouse_connection_get_error(intern->conn), 0);
        RETURN_FALSE;
    }

    /* Mark as error state */
    intern->async->state = ASYNC_STATE_ERROR;
    if (intern->async->error) {
        free(intern->async->error);
    }
    intern->async->error = strdup("Query cancelled");

    RETURN_TRUE;
}
/* }}} */

/* ========================= ResultIterator Methods ========================= */

/* Helper: Update iterator validity based on current position */
static void resultiterator_update_validity(clickhouse_resultiterator_object *intern) {
    if (intern->finished || !intern->result) {
        intern->valid = 0;
        return;
    }

    /* Skip empty blocks */
    while (intern->current_block < intern->result->block_count) {
        clickhouse_block *block = intern->result->blocks[intern->current_block];
        if (intern->current_row < block->row_count) {
            intern->valid = 1;
            return;
        }
        /* Move to next block */
        intern->current_block++;
        intern->current_row = 0;
    }

    /* Reached end of all blocks */
    intern->valid = 0;
    intern->finished = 1;
}

/* ========================= StreamingIterator Methods ========================= */

/* {{{ Helper function to update streaming iterator validity */
static void streamingiterator_update_validity(clickhouse_streamingiterator_object *intern) {
    if (!intern->sq || !intern->sq->current_block) {
        intern->valid = 0;
        return;
    }

    clickhouse_block *block = intern->sq->current_block;

    /* Check if current position is within current block */
    if (intern->current_row < block->row_count) {
        intern->valid = 1;
        return;
    }

    /* Try to fetch next block */
    int result = clickhouse_streaming_fetch_next_block(intern->sq);

    if (result == 1) {
        /* Block available, reset row position */
        intern->current_row = 0;
        intern->valid = 1;
        return;
    }

    if (result == -1) {
        /* Error fetching block */
        intern->valid = 0;
        const char *error_msg = (intern->sq && intern->sq->error) ? intern->sq->error : "Error fetching next block from server";
        zend_throw_exception(clickhouse_exception_ce, error_msg, 0);
        return;
    }

    /* result == 0: no more blocks, streaming complete */
    intern->valid = 0;
}
/* }}} */

/* {{{ proto void ClickHouse\StreamingIterator::rewind()
   Reset iterator to the beginning by re-executing query */
PHP_METHOD(ClickHouse_StreamingIterator, rewind) {
    ZEND_PARSE_PARAMETERS_NONE();

    clickhouse_streamingiterator_object *intern = Z_CLICKHOUSE_STREAMINGITERATOR_P(ZEND_THIS);

    if (!intern->query_sql) {
        zend_throw_exception(clickhouse_exception_ce, "Query SQL not stored", 0);
        return;
    }

    /* Get client object to access query options */
    clickhouse_client_object *client = Z_CLICKHOUSE_CLIENT_P(&intern->client_zv);

    /* If this is the first rewind (foreach starting), use existing query */
    if (intern->sq && !intern->started) {
        /* First rewind - query already created in queryStreaming(), just fetch first block */
        intern->current_row = 0;
        intern->current_key = 0;
        intern->total_rows = 0;

        int result = clickhouse_streaming_fetch_next_block(intern->sq);

        if (result == -1) {
            intern->valid = 0;
            zend_throw_exception(clickhouse_exception_ce, "Error fetching first block from server", 0);
            return;
        }

        if (result == 0) {
            /* No data available */
            intern->valid = 0;
            intern->started = 1;
            return;
        }

        /* Block fetched successfully */
        intern->started = 1;
        if (intern->sq->current_block && intern->sq->current_block->row_count > 0) {
            intern->valid = 1;
        } else {
            intern->valid = 0;
        }
        return;
    }

    /* Subsequent rewinds - need to re-execute query */
    /* Free old streaming query if exists */
    if (intern->sq) {
        clickhouse_streaming_query_free(intern->sq);
        intern->sq = NULL;
    }

    /* Reset iteration state */
    intern->current_row = 0;
    intern->current_key = 0;
    intern->total_rows = 0;
    intern->started = 0;
    intern->valid = 0;

    /* Re-create query options from client */
    clickhouse_query_options *opts = NULL;
    if (client->compression != CH_COMPRESS_NONE || client->session_id || client->default_query_id || client->query_settings) {
        opts = clickhouse_query_options_create();
        if (opts) {
            opts->compression = client->compression;
            if (client->session_id) {
                opts->session_id = client->session_id;
            }
            if (client->default_query_id) {
                opts->query_id = client->default_query_id;
            }
            /* Copy query settings from client to options */
            if (client->query_settings) {
                clickhouse_setting *setting = client->query_settings->head;
                while (setting) {
                    clickhouse_query_options_set_setting(opts, setting->name, setting->value);
                    setting = setting->next;
                }
            }
        }
    }

    /* Re-execute streaming query using client's connection */
    clickhouse_streaming_query *sq = NULL;
    int status;
    if (opts) {
        status = clickhouse_connection_query_streaming(client->conn, intern->query_sql, opts, &sq);
        /* Don't free session_id/query_id as they're borrowed from client */
        opts->session_id = NULL;
        opts->query_id = NULL;
        clickhouse_query_options_free(opts);
    } else {
        status = clickhouse_connection_query_streaming(client->conn, intern->query_sql, NULL, &sq);
    }

    /* Update stored connection reference */
    intern->conn = client->conn;

    if (status != 0) {
        const char *error = clickhouse_connection_get_error(intern->conn);
        if (sq) {
            clickhouse_streaming_query_free(sq);
        }
        zend_throw_exception(clickhouse_exception_ce, error ? error : "Failed to re-execute streaming query", 0);
        return;
    }

    /* Transfer ownership of new streaming query */
    intern->sq = sq;

    /* Fetch first block */
    int result = clickhouse_streaming_fetch_next_block(intern->sq);

    if (result == -1) {
        intern->valid = 0;
        zend_throw_exception(clickhouse_exception_ce, "Error fetching first block from server", 0);
        return;
    }

    if (result == 0) {
        /* No data available */
        intern->valid = 0;
        intern->started = 1;
        return;
    }

    /* Block fetched successfully - set valid directly */
    intern->started = 1;
    if (intern->sq->current_block && intern->sq->current_block->row_count > 0) {
        intern->valid = 1;
    } else {
        intern->valid = 0;
    }
}
/* }}} */

/* {{{ proto mixed ClickHouse\StreamingIterator::current()
   Return current row as associative array */
PHP_METHOD(ClickHouse_StreamingIterator, current) {
    ZEND_PARSE_PARAMETERS_NONE();

    clickhouse_streamingiterator_object *intern = Z_CLICKHOUSE_STREAMINGITERATOR_P(ZEND_THIS);

    if (!intern->valid || !intern->sq || !intern->sq->current_block) {
        RETURN_NULL();
    }

    clickhouse_block *block = intern->sq->current_block;

    array_init(return_value);
    for (size_t c = 0; c < block->column_count; c++) {
        clickhouse_column *col = block->columns[c];
        zval cell;
        column_value_to_zval(col, intern->current_row, &cell);
        add_assoc_zval(return_value, col->name, &cell);
    }
}
/* }}} */

/* {{{ proto int ClickHouse\StreamingIterator::key()
   Return current row index */
PHP_METHOD(ClickHouse_StreamingIterator, key) {
    ZEND_PARSE_PARAMETERS_NONE();

    clickhouse_streamingiterator_object *intern = Z_CLICKHOUSE_STREAMINGITERATOR_P(ZEND_THIS);

    RETURN_LONG(intern->current_key);
}
/* }}} */

/* {{{ proto void ClickHouse\StreamingIterator::next()
   Move to next row, fetching next block if needed */
PHP_METHOD(ClickHouse_StreamingIterator, next) {
    ZEND_PARSE_PARAMETERS_NONE();

    clickhouse_streamingiterator_object *intern = Z_CLICKHOUSE_STREAMINGITERATOR_P(ZEND_THIS);

    if (!intern->sq || !intern->valid) {
        intern->valid = 0;
        return;
    }

    /* Move to next row */
    intern->current_row++;
    intern->current_key++;
    intern->total_rows++;

    /* Update validity, which will fetch next block if needed */
    streamingiterator_update_validity(intern);
}
/* }}} */

/* {{{ proto bool ClickHouse\StreamingIterator::valid()
   Check if current position is valid */
PHP_METHOD(ClickHouse_StreamingIterator, valid) {
    ZEND_PARSE_PARAMETERS_NONE();

    clickhouse_streamingiterator_object *intern = Z_CLICKHOUSE_STREAMINGITERATOR_P(ZEND_THIS);

    RETURN_BOOL(intern->valid);
}
/* }}} */

/* {{{ proto int ClickHouse\StreamingIterator::count()
   Return total rows fetched so far (not total available) */
PHP_METHOD(ClickHouse_StreamingIterator, count) {
    ZEND_PARSE_PARAMETERS_NONE();

    clickhouse_streamingiterator_object *intern = Z_CLICKHOUSE_STREAMINGITERATOR_P(ZEND_THIS);

    /* For streaming queries, we can only report rows fetched so far
     * Include current row if we're on a valid position */
    zend_long count = intern->total_rows;
    if (intern->valid) {
        count++;  /* Current row hasn't been counted by next() yet */
    }
    RETURN_LONG(count);
}
/* }}} */

/* {{{ proto void ClickHouse\ResultIterator::rewind()
   Reset iterator to the beginning */
PHP_METHOD(ClickHouse_ResultIterator, rewind) {
    ZEND_PARSE_PARAMETERS_NONE();

    clickhouse_resultiterator_object *intern = Z_CLICKHOUSE_RESULTITERATOR_P(ZEND_THIS);

    intern->current_block = 0;
    intern->current_row = 0;
    intern->current_key = 0;
    intern->finished = 0;

    resultiterator_update_validity(intern);
}
/* }}} */

/* {{{ proto mixed ClickHouse\ResultIterator::current()
   Return current row as associative array */
PHP_METHOD(ClickHouse_ResultIterator, current) {
    ZEND_PARSE_PARAMETERS_NONE();

    clickhouse_resultiterator_object *intern = Z_CLICKHOUSE_RESULTITERATOR_P(ZEND_THIS);

    if (!intern->valid || !intern->result) {
        RETURN_NULL();
    }

    clickhouse_block *block = intern->result->blocks[intern->current_block];

    array_init(return_value);
    for (size_t c = 0; c < block->column_count; c++) {
        clickhouse_column *col = block->columns[c];
        zval cell;
        column_value_to_zval(col, intern->current_row, &cell);
        add_assoc_zval(return_value, col->name, &cell);
    }
}
/* }}} */

/* {{{ proto int ClickHouse\ResultIterator::key()
   Return current row index */
PHP_METHOD(ClickHouse_ResultIterator, key) {
    ZEND_PARSE_PARAMETERS_NONE();

    clickhouse_resultiterator_object *intern = Z_CLICKHOUSE_RESULTITERATOR_P(ZEND_THIS);

    RETURN_LONG(intern->current_key);
}
/* }}} */

/* {{{ proto void ClickHouse\ResultIterator::next()
   Move to next row */
PHP_METHOD(ClickHouse_ResultIterator, next) {
    ZEND_PARSE_PARAMETERS_NONE();

    clickhouse_resultiterator_object *intern = Z_CLICKHOUSE_RESULTITERATOR_P(ZEND_THIS);

    if (!intern->result || intern->finished) {
        intern->valid = 0;
        return;
    }

    /* Move to next row */
    intern->current_row++;
    intern->current_key++;

    /* Check if we need to move to next block */
    if (intern->current_block < intern->result->block_count) {
        clickhouse_block *block = intern->result->blocks[intern->current_block];
        if (intern->current_row >= block->row_count) {
            intern->current_block++;
            intern->current_row = 0;
        }
    }

    resultiterator_update_validity(intern);
}
/* }}} */

/* {{{ proto bool ClickHouse\ResultIterator::valid()
   Check if current position is valid */
PHP_METHOD(ClickHouse_ResultIterator, valid) {
    ZEND_PARSE_PARAMETERS_NONE();

    clickhouse_resultiterator_object *intern = Z_CLICKHOUSE_RESULTITERATOR_P(ZEND_THIS);

    RETURN_BOOL(intern->valid);
}
/* }}} */

/* {{{ proto int ClickHouse\ResultIterator::count()
   Return total number of rows across all blocks */
PHP_METHOD(ClickHouse_ResultIterator, count) {
    ZEND_PARSE_PARAMETERS_NONE();

    clickhouse_resultiterator_object *intern = Z_CLICKHOUSE_RESULTITERATOR_P(ZEND_THIS);

    if (!intern->result) {
        RETURN_LONG(0);
    }

    zend_long total = 0;
    for (size_t b = 0; b < intern->result->block_count; b++) {
        total += intern->result->blocks[b]->row_count;
    }

    RETURN_LONG(total);
}
/* }}} */

/* {{{ proto ResultIterator ClickHouse\Client::queryIterator(string $sql)
   Execute query and return a ResultIterator for streaming results */
PHP_METHOD(ClickHouse_Client, queryIterator) {
    char *sql;
    size_t sql_len;

    ZEND_PARSE_PARAMETERS_START(1, 1)
        Z_PARAM_STRING(sql, sql_len)
    ZEND_PARSE_PARAMETERS_END();

    clickhouse_client_object *intern = Z_CLICKHOUSE_CLIENT_P(ZEND_THIS);

    if (!intern->conn || intern->conn->state != CONN_STATE_AUTHENTICATED) {
        zend_throw_exception(clickhouse_exception_ce, "Not connected", 0);
        return;
    }

    /* Execute query using existing mechanism */
    clickhouse_query_options *opts = NULL;
    if (intern->compression != CH_COMPRESS_NONE || intern->session_id || intern->default_query_id || intern->query_settings) {
        opts = clickhouse_query_options_create();
        if (opts) {
            opts->compression = intern->compression;
            if (intern->session_id) {
                opts->session_id = intern->session_id;
            }
            if (intern->default_query_id) {
                opts->query_id = intern->default_query_id;
            }
            /* Copy query settings from client to options */
            if (intern->query_settings) {
                clickhouse_setting *setting = intern->query_settings->head;
                while (setting) {
                    clickhouse_query_options_set_setting(opts, setting->name, setting->value);
                    setting = setting->next;
                }
            }
        }
    }

    clickhouse_result *result = NULL;
    int status;
    if (opts) {
        status = clickhouse_connection_execute_query_ext(intern->conn, sql, opts, &result);
        /* Don't free session_id/query_id as they're borrowed from intern */
        opts->session_id = NULL;
        opts->query_id = NULL;
        clickhouse_query_options_free(opts);
    } else {
        status = clickhouse_connection_execute_query(intern->conn, sql, &result);
    }

    if (status != 0) {
        const char *error = clickhouse_connection_get_error(intern->conn);
        if (result) {
            clickhouse_result_free(result);
        }
        zend_throw_exception(clickhouse_exception_ce, error ? error : "Query failed", 0);
        return;
    }

    /* Store query ID from result for tracking */
    if (intern->last_query_id) {
        efree(intern->last_query_id);
        intern->last_query_id = NULL;
    }
    if (result && result->query_id) {
        intern->last_query_id = estrdup(result->query_id);
    }

    /* Create ResultIterator object */
    object_init_ex(return_value, clickhouse_resultiterator_ce);
    clickhouse_resultiterator_object *iter_obj = Z_CLICKHOUSE_RESULTITERATOR_P(return_value);

    iter_obj->result = result;  /* Transfer ownership */
    iter_obj->conn = intern->conn;
    iter_obj->current_block = 0;
    iter_obj->current_row = 0;
    iter_obj->current_key = 0;
    iter_obj->finished = 0;
    ZVAL_COPY(&iter_obj->client_zv, ZEND_THIS);

    /* Set initial validity */
    resultiterator_update_validity(iter_obj);
}
/* }}} */

/* {{{ proto StreamingIterator ClickHouse\Client::queryStreaming(string $sql)
   Execute query and return a StreamingIterator for true streaming results */
PHP_METHOD(ClickHouse_Client, queryStreaming) {
    char *sql;
    size_t sql_len;

    ZEND_PARSE_PARAMETERS_START(1, 1)
        Z_PARAM_STRING(sql, sql_len)
    ZEND_PARSE_PARAMETERS_END();

    clickhouse_client_object *intern = Z_CLICKHOUSE_CLIENT_P(ZEND_THIS);

    if (!intern->conn || intern->conn->state != CONN_STATE_AUTHENTICATED) {
        zend_throw_exception(clickhouse_exception_ce, "Not connected", 0);
        return;
    }

    /* Create query options */
    clickhouse_query_options *opts = NULL;
    if (intern->compression != CH_COMPRESS_NONE || intern->session_id || intern->default_query_id || intern->query_settings) {
        opts = clickhouse_query_options_create();
        if (opts) {
            opts->compression = intern->compression;
            if (intern->session_id) {
                opts->session_id = intern->session_id;
            }
            if (intern->default_query_id) {
                opts->query_id = intern->default_query_id;
            }
            /* Copy query settings from client to options */
            if (intern->query_settings) {
                clickhouse_setting *setting = intern->query_settings->head;
                while (setting) {
                    clickhouse_query_options_set_setting(opts, setting->name, setting->value);
                    setting = setting->next;
                }
            }
        }
    }

    /* Initialize streaming query */
    clickhouse_streaming_query *sq = NULL;
    int status;
    if (opts) {
        status = clickhouse_connection_query_streaming(intern->conn, sql, opts, &sq);
        /* Don't free session_id/query_id as they're borrowed from intern */
        opts->session_id = NULL;
        opts->query_id = NULL;
        clickhouse_query_options_free(opts);
    } else {
        status = clickhouse_connection_query_streaming(intern->conn, sql, NULL, &sq);
    }

    if (status != 0) {
        const char *error = clickhouse_connection_get_error(intern->conn);
        if (sq) {
            clickhouse_streaming_query_free(sq);
        }
        zend_throw_exception(clickhouse_exception_ce, error ? error : "Streaming query failed", 0);
        return;
    }

    /* Store query ID for tracking */
    if (intern->last_query_id) {
        efree(intern->last_query_id);
        intern->last_query_id = NULL;
    }
    if (sq && sq->query_id) {
        intern->last_query_id = estrdup(sq->query_id);
    }

    /* Create StreamingIterator object */
    object_init_ex(return_value, clickhouse_streamingiterator_ce);
    clickhouse_streamingiterator_object *iter_obj = Z_CLICKHOUSE_STREAMINGITERATOR_P(return_value);

    iter_obj->sq = sq;  /* Transfer ownership */
    iter_obj->conn = intern->conn;
    iter_obj->query_sql = estrndup(sql, sql_len);  /* Store SQL for rewind */
    iter_obj->current_row = 0;
    iter_obj->current_key = 0;
    iter_obj->total_rows = 0;
    iter_obj->valid = 0;
    iter_obj->started = 0;
    ZVAL_COPY(&iter_obj->client_zv, ZEND_THIS);
}
/* }}} */

/* {{{ arginfo */
ZEND_BEGIN_ARG_INFO_EX(arginfo_clickhouse_client_construct, 0, 0, 1)
    ZEND_ARG_TYPE_INFO(0, host, IS_STRING, 0)
    ZEND_ARG_TYPE_INFO(0, port, IS_LONG, 0)
    ZEND_ARG_TYPE_INFO(0, user, IS_STRING, 0)
    ZEND_ARG_TYPE_INFO(0, password, IS_STRING, 0)
    ZEND_ARG_TYPE_INFO(0, database, IS_STRING, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_clickhouse_client_query, 0, 1, IS_ARRAY, 0)
    ZEND_ARG_TYPE_INFO(0, sql, IS_STRING, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_clickhouse_client_execute, 0, 1, IS_VOID, 0)
    ZEND_ARG_TYPE_INFO(0, sql, IS_STRING, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_clickhouse_client_executebatch, 0, 1, IS_ARRAY, 0)
    ZEND_ARG_TYPE_INFO(0, queries, IS_ARRAY, 0)
    ZEND_ARG_TYPE_INFO(0, options, IS_ARRAY, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_clickhouse_client_insert, 0, 3, IS_VOID, 0)
    ZEND_ARG_TYPE_INFO(0, table, IS_STRING, 0)
    ZEND_ARG_TYPE_INFO(0, columns, IS_ARRAY, 0)
    ZEND_ARG_TYPE_INFO(0, rows, IS_ARRAY, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_clickhouse_client_ping, 0, 0, _IS_BOOL, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_clickhouse_client_close, 0, 0, IS_VOID, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_clickhouse_client_isconnected, 0, 0, _IS_BOOL, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_clickhouse_client_getserverinfo, 0, 0, IS_ARRAY, 0)
ZEND_END_ARG_INFO()

/* Schema introspection arginfo */
ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_clickhouse_client_getdatabases, 0, 0, IS_ARRAY, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_clickhouse_client_gettables, 0, 0, IS_ARRAY, 0)
    ZEND_ARG_TYPE_INFO(0, database, IS_STRING, 1)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_clickhouse_client_describetable, 0, 1, IS_ARRAY, 0)
    ZEND_ARG_TYPE_INFO(0, table, IS_STRING, 0)
    ZEND_ARG_TYPE_INFO(0, database, IS_STRING, 1)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_clickhouse_client_setcompression, 0, 1, IS_VOID, 0)
    ZEND_ARG_TYPE_INFO(0, method, IS_LONG, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_clickhouse_client_getcompression, 0, 0, IS_LONG, 0)
ZEND_END_ARG_INFO()

/* Progress callback arginfo */
ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_clickhouse_client_setprogresscallback, 0, 1, IS_VOID, 0)
    ZEND_ARG_TYPE_INFO(0, callback, IS_CALLABLE, 1)
ZEND_END_ARG_INFO()

/* Profile callback arginfo */
ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_clickhouse_client_setprofilecallback, 0, 1, IS_VOID, 0)
    ZEND_ARG_TYPE_INFO(0, callback, IS_CALLABLE, 1)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_clickhouse_client_setlogcallback, 0, 1, IS_VOID, 0)
    ZEND_ARG_TYPE_INFO(0, callback, IS_CALLABLE, 1)
ZEND_END_ARG_INFO()

/* Read-only mode arginfo */
ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_clickhouse_client_setreadonly, 0, 1, IS_VOID, 0)
    ZEND_ARG_TYPE_INFO(0, readonly, _IS_BOOL, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_clickhouse_client_isreadonly, 0, 0, _IS_BOOL, 0)
ZEND_END_ARG_INFO()

/* Insert from string/file arginfo */
ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_clickhouse_client_insertfromstring, 0, 2, IS_VOID, 0)
    ZEND_ARG_TYPE_INFO(0, table, IS_STRING, 0)
    ZEND_ARG_TYPE_INFO(0, data, IS_STRING, 0)
    ZEND_ARG_TYPE_INFO(0, format, IS_STRING, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_clickhouse_client_insertfromfile, 0, 2, IS_VOID, 0)
    ZEND_ARG_TYPE_INFO(0, table, IS_STRING, 0)
    ZEND_ARG_TYPE_INFO(0, filepath, IS_STRING, 0)
    ZEND_ARG_TYPE_INFO(0, format, IS_STRING, 0)
ZEND_END_ARG_INFO()

/* Timeout arginfo */
ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_clickhouse_client_settimeout, 0, 1, IS_VOID, 0)
    ZEND_ARG_TYPE_INFO(0, timeout_ms, IS_LONG, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_clickhouse_client_gettimeout, 0, 0, IS_LONG, 0)
ZEND_END_ARG_INFO()

/* SSL arginfo */
ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_clickhouse_client_setssl, 0, 1, IS_VOID, 0)
    ZEND_ARG_TYPE_INFO(0, enabled, _IS_BOOL, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_clickhouse_client_setsslverify, 0, 1, IS_VOID, 0)
    ZEND_ARG_TYPE_INFO(0, verify_peer, _IS_BOOL, 0)
    ZEND_ARG_TYPE_INFO(0, verify_host, _IS_BOOL, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_clickhouse_client_setsslca, 0, 1, IS_VOID, 0)
    ZEND_ARG_TYPE_INFO(0, ca_path, IS_STRING, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_clickhouse_client_setsslcert, 0, 2, IS_VOID, 0)
    ZEND_ARG_TYPE_INFO(0, cert_path, IS_STRING, 0)
    ZEND_ARG_TYPE_INFO(0, key_path, IS_STRING, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_clickhouse_client_sslavailable, 0, 0, _IS_BOOL, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_clickhouse_client_issslconnected, 0, 0, _IS_BOOL, 0)
ZEND_END_ARG_INFO()

/* New feature arginfo */
ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_clickhouse_client_querywithmeta, 0, 1, IS_ARRAY, 0)
    ZEND_ARG_TYPE_INFO(0, sql, IS_STRING, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_clickhouse_client_cancel, 0, 0, _IS_BOOL, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_OBJ_INFO_EX(arginfo_clickhouse_client_fromdsn, 0, 1, ClickHouse\\Client, 0)
    ZEND_ARG_TYPE_INFO(0, dsn, IS_STRING, 0)
ZEND_END_ARG_INFO()

/* Session and Query ID arginfo */
ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_clickhouse_client_setsession, 0, 1, IS_VOID, 0)
    ZEND_ARG_TYPE_INFO(0, session_id, IS_STRING, 1)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_clickhouse_client_getsession, 0, 0, IS_STRING, 1)
ZEND_END_ARG_INFO()

/* Reconnection arginfo */
ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_clickhouse_client_reconnect, 0, 0, _IS_BOOL, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_clickhouse_client_setautoreconnect, 0, 1, IS_VOID, 0)
    ZEND_ARG_TYPE_INFO(0, enabled, _IS_BOOL, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_clickhouse_client_getautoreconnect, 0, 0, _IS_BOOL, 0)
ZEND_END_ARG_INFO()

/* Retry configuration arginfo */
ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_clickhouse_client_setmaxretryattempts, 0, 1, IS_VOID, 0)
    ZEND_ARG_TYPE_INFO(0, max_attempts, IS_LONG, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_clickhouse_client_getmaxretryattempts, 0, 0, IS_LONG, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_clickhouse_client_setretrydelay, 0, 2, IS_VOID, 0)
    ZEND_ARG_TYPE_INFO(0, base_delay, IS_DOUBLE, 0)
    ZEND_ARG_TYPE_INFO(0, max_delay, IS_DOUBLE, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_clickhouse_client_getretrydelay, 0, 0, IS_ARRAY, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_clickhouse_client_setretryjitter, 0, 1, IS_VOID, 0)
    ZEND_ARG_TYPE_INFO(0, enabled, _IS_BOOL, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_clickhouse_client_getretryjitter, 0, 0, _IS_BOOL, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_clickhouse_client_gettotalretryattempts, 0, 0, IS_LONG, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_clickhouse_client_resetretrymetrics, 0, 0, IS_VOID, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_clickhouse_client_setqueryid, 0, 1, IS_VOID, 0)
    ZEND_ARG_TYPE_INFO(0, query_id, IS_STRING, 1)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_clickhouse_client_getqueryid, 0, 0, IS_STRING, 1)
ZEND_END_ARG_INFO()

/* Query tracking arginfo */
ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_clickhouse_client_getlastqueryid, 0, 0, IS_STRING, 1)
ZEND_END_ARG_INFO()

/* Metrics arginfo */
ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_clickhouse_client_enablemetrics, 0, 0, IS_VOID, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_clickhouse_client_disablemetrics, 0, 0, IS_VOID, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_clickhouse_client_getmetrics, 0, 0, IS_ARRAY, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_clickhouse_client_resetmetrics, 0, 0, IS_VOID, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_clickhouse_client_setslowquerythreshold, 0, 1, IS_VOID, 0)
    ZEND_ARG_TYPE_INFO(0, threshold, IS_DOUBLE, 0)
ZEND_END_ARG_INFO()

/* Transaction arginfo (EXPERIMENTAL) */
ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_clickhouse_client_begintransaction, 0, 0, _IS_BOOL, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_clickhouse_client_commit, 0, 0, _IS_BOOL, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_clickhouse_client_rollback, 0, 0, _IS_BOOL, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_clickhouse_client_intransaction, 0, 0, _IS_BOOL, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_clickhouse_client_setquerysetting, 0, 2, IS_VOID, 0)
    ZEND_ARG_TYPE_INFO(0, name, IS_STRING, 0)
    ZEND_ARG_TYPE_INFO(0, value, IS_STRING, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_clickhouse_client_clearquerysettings, 0, 0, IS_VOID, 0)
ZEND_END_ARG_INFO()

/* External table query */
ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_clickhouse_client_querywithtable, 0, 4, IS_ARRAY, 0)
    ZEND_ARG_TYPE_INFO(0, sql, IS_STRING, 0)
    ZEND_ARG_TYPE_INFO(0, table_name, IS_STRING, 0)
    ZEND_ARG_TYPE_INFO(0, columns, IS_ARRAY, 0)
    ZEND_ARG_TYPE_INFO(0, rows, IS_ARRAY, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_OBJ_INFO_EX(arginfo_clickhouse_client_prepare, 0, 1, ClickHouse\\Statement, 0)
    ZEND_ARG_TYPE_INFO(0, sql, IS_STRING, 0)
ZEND_END_ARG_INFO()

/* Statement arginfo */
ZEND_BEGIN_ARG_WITH_RETURN_OBJ_INFO_EX(arginfo_clickhouse_statement_bind, 0, 2, ClickHouse\\Statement, 0)
    ZEND_ARG_TYPE_INFO(0, name, IS_STRING, 0)
    ZEND_ARG_TYPE_INFO(0, value, IS_MIXED, 0)
    ZEND_ARG_TYPE_INFO(0, type, IS_STRING, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_OBJ_INFO_EX(arginfo_clickhouse_statement_setoption, 0, 2, ClickHouse\\Statement, 0)
    ZEND_ARG_TYPE_INFO(0, name, IS_STRING, 0)
    ZEND_ARG_TYPE_INFO(0, value, IS_STRING, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_clickhouse_statement_execute, 0, 0, IS_ARRAY, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_clickhouse_statement_fetchall, 0, 0, IS_ARRAY, 0)
ZEND_END_ARG_INFO()

/* AsyncResult arginfo */
ZEND_BEGIN_ARG_WITH_RETURN_OBJ_INFO_EX(arginfo_clickhouse_client_queryasync, 0, 1, ClickHouse\\AsyncResult, 0)
    ZEND_ARG_TYPE_INFO(0, sql, IS_STRING, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_clickhouse_asyncresult_poll, 0, 0, _IS_BOOL, 0)
    ZEND_ARG_TYPE_INFO(0, timeout_ms, IS_LONG, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_clickhouse_asyncresult_wait, 0, 0, IS_ARRAY, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_clickhouse_asyncresult_getresult, 0, 0, IS_ARRAY, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_clickhouse_asyncresult_isready, 0, 0, _IS_BOOL, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_clickhouse_asyncresult_cancel, 0, 0, _IS_BOOL, 0)
ZEND_END_ARG_INFO()
/* }}} */

/* ResultIterator arginfo */
ZEND_BEGIN_ARG_WITH_RETURN_OBJ_INFO_EX(arginfo_clickhouse_client_queryiterator, 0, 1, ClickHouse\\ResultIterator, 0)
    ZEND_ARG_TYPE_INFO(0, sql, IS_STRING, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_clickhouse_resultiterator_rewind, 0, 0, IS_VOID, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_clickhouse_resultiterator_current, 0, 0, IS_MIXED, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_clickhouse_resultiterator_key, 0, 0, IS_LONG, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_clickhouse_resultiterator_next, 0, 0, IS_VOID, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_clickhouse_resultiterator_valid, 0, 0, _IS_BOOL, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_clickhouse_resultiterator_count, 0, 0, IS_LONG, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_OBJ_INFO_EX(arginfo_clickhouse_client_querystreaming, 0, 1, ClickHouse\\StreamingIterator, 0)
    ZEND_ARG_TYPE_INFO(0, sql, IS_STRING, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_clickhouse_streamingiterator_rewind, 0, 0, IS_VOID, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_clickhouse_streamingiterator_current, 0, 0, IS_MIXED, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_clickhouse_streamingiterator_key, 0, 0, IS_LONG, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_clickhouse_streamingiterator_next, 0, 0, IS_VOID, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_clickhouse_streamingiterator_valid, 0, 0, _IS_BOOL, 0)
ZEND_END_ARG_INFO()

ZEND_BEGIN_ARG_WITH_RETURN_TYPE_INFO_EX(arginfo_clickhouse_streamingiterator_count, 0, 0, IS_LONG, 0)
ZEND_END_ARG_INFO()

/* {{{ Client methods */
static const zend_function_entry clickhouse_client_methods[] = {
    PHP_ME(ClickHouse_Client, __construct, arginfo_clickhouse_client_construct, ZEND_ACC_PUBLIC)
    PHP_ME(ClickHouse_Client, query, arginfo_clickhouse_client_query, ZEND_ACC_PUBLIC)
    PHP_ME(ClickHouse_Client, execute, arginfo_clickhouse_client_execute, ZEND_ACC_PUBLIC)
    PHP_ME(ClickHouse_Client, executeBatch, arginfo_clickhouse_client_executebatch, ZEND_ACC_PUBLIC)
    PHP_ME(ClickHouse_Client, insert, arginfo_clickhouse_client_insert, ZEND_ACC_PUBLIC)
    PHP_ME(ClickHouse_Client, ping, arginfo_clickhouse_client_ping, ZEND_ACC_PUBLIC)
    PHP_ME(ClickHouse_Client, close, arginfo_clickhouse_client_close, ZEND_ACC_PUBLIC)
    PHP_ME(ClickHouse_Client, isConnected, arginfo_clickhouse_client_isconnected, ZEND_ACC_PUBLIC)
    PHP_ME(ClickHouse_Client, getServerInfo, arginfo_clickhouse_client_getserverinfo, ZEND_ACC_PUBLIC)
    /* Schema introspection methods */
    PHP_ME(ClickHouse_Client, getDatabases, arginfo_clickhouse_client_getdatabases, ZEND_ACC_PUBLIC)
    PHP_ME(ClickHouse_Client, getTables, arginfo_clickhouse_client_gettables, ZEND_ACC_PUBLIC)
    PHP_ME(ClickHouse_Client, describeTable, arginfo_clickhouse_client_describetable, ZEND_ACC_PUBLIC)
    PHP_ME(ClickHouse_Client, setCompression, arginfo_clickhouse_client_setcompression, ZEND_ACC_PUBLIC)
    PHP_ME(ClickHouse_Client, getCompression, arginfo_clickhouse_client_getcompression, ZEND_ACC_PUBLIC)
    /* Progress callback method */
    PHP_ME(ClickHouse_Client, setProgressCallback, arginfo_clickhouse_client_setprogresscallback, ZEND_ACC_PUBLIC)
    /* Profile callback method */
    PHP_ME(ClickHouse_Client, setProfileCallback, arginfo_clickhouse_client_setprofilecallback, ZEND_ACC_PUBLIC)
    PHP_ME(ClickHouse_Client, setLogCallback, arginfo_clickhouse_client_setlogcallback, ZEND_ACC_PUBLIC)
    /* Read-only mode methods */
    PHP_ME(ClickHouse_Client, setReadOnly, arginfo_clickhouse_client_setreadonly, ZEND_ACC_PUBLIC)
    PHP_ME(ClickHouse_Client, isReadOnly, arginfo_clickhouse_client_isreadonly, ZEND_ACC_PUBLIC)
    /* Insert from string/file methods */
    PHP_ME(ClickHouse_Client, insertFromString, arginfo_clickhouse_client_insertfromstring, ZEND_ACC_PUBLIC)
    PHP_ME(ClickHouse_Client, insertFromFile, arginfo_clickhouse_client_insertfromfile, ZEND_ACC_PUBLIC)
    /* Timeout methods */
    PHP_ME(ClickHouse_Client, setTimeout, arginfo_clickhouse_client_settimeout, ZEND_ACC_PUBLIC)
    PHP_ME(ClickHouse_Client, getTimeout, arginfo_clickhouse_client_gettimeout, ZEND_ACC_PUBLIC)
    /* SSL methods */
    PHP_ME(ClickHouse_Client, setSSL, arginfo_clickhouse_client_setssl, ZEND_ACC_PUBLIC)
    PHP_ME(ClickHouse_Client, setSSLVerify, arginfo_clickhouse_client_setsslverify, ZEND_ACC_PUBLIC)
    PHP_ME(ClickHouse_Client, setSSLCA, arginfo_clickhouse_client_setsslca, ZEND_ACC_PUBLIC)
    PHP_ME(ClickHouse_Client, setSSLCert, arginfo_clickhouse_client_setsslcert, ZEND_ACC_PUBLIC)
    PHP_ME(ClickHouse_Client, sslAvailable, arginfo_clickhouse_client_sslavailable, ZEND_ACC_PUBLIC | ZEND_ACC_STATIC)
    PHP_ME(ClickHouse_Client, isSSLConnected, arginfo_clickhouse_client_issslconnected, ZEND_ACC_PUBLIC)
    /* New feature methods */
    PHP_ME(ClickHouse_Client, queryWithMeta, arginfo_clickhouse_client_querywithmeta, ZEND_ACC_PUBLIC)
    PHP_ME(ClickHouse_Client, cancel, arginfo_clickhouse_client_cancel, ZEND_ACC_PUBLIC)
    PHP_ME(ClickHouse_Client, fromDSN, arginfo_clickhouse_client_fromdsn, ZEND_ACC_PUBLIC | ZEND_ACC_STATIC)
    /* Session and Query ID methods */
    PHP_ME(ClickHouse_Client, setSession, arginfo_clickhouse_client_setsession, ZEND_ACC_PUBLIC)
    PHP_ME(ClickHouse_Client, getSession, arginfo_clickhouse_client_getsession, ZEND_ACC_PUBLIC)
    /* Reconnection methods */
    PHP_ME(ClickHouse_Client, reconnect, arginfo_clickhouse_client_reconnect, ZEND_ACC_PUBLIC)
    PHP_ME(ClickHouse_Client, setAutoReconnect, arginfo_clickhouse_client_setautoreconnect, ZEND_ACC_PUBLIC)
    PHP_ME(ClickHouse_Client, getAutoReconnect, arginfo_clickhouse_client_getautoreconnect, ZEND_ACC_PUBLIC)
    /* Retry configuration methods */
    PHP_ME(ClickHouse_Client, setMaxRetryAttempts, arginfo_clickhouse_client_setmaxretryattempts, ZEND_ACC_PUBLIC)
    PHP_ME(ClickHouse_Client, getMaxRetryAttempts, arginfo_clickhouse_client_getmaxretryattempts, ZEND_ACC_PUBLIC)
    PHP_ME(ClickHouse_Client, setRetryDelay, arginfo_clickhouse_client_setretrydelay, ZEND_ACC_PUBLIC)
    PHP_ME(ClickHouse_Client, getRetryDelay, arginfo_clickhouse_client_getretrydelay, ZEND_ACC_PUBLIC)
    PHP_ME(ClickHouse_Client, setRetryJitter, arginfo_clickhouse_client_setretryjitter, ZEND_ACC_PUBLIC)
    PHP_ME(ClickHouse_Client, getRetryJitter, arginfo_clickhouse_client_getretryjitter, ZEND_ACC_PUBLIC)
    PHP_ME(ClickHouse_Client, getTotalRetryAttempts, arginfo_clickhouse_client_gettotalretryattempts, ZEND_ACC_PUBLIC)
    PHP_ME(ClickHouse_Client, resetRetryMetrics, arginfo_clickhouse_client_resetretrymetrics, ZEND_ACC_PUBLIC)
    PHP_ME(ClickHouse_Client, setQueryId, arginfo_clickhouse_client_setqueryid, ZEND_ACC_PUBLIC)
    PHP_ME(ClickHouse_Client, getQueryId, arginfo_clickhouse_client_getqueryid, ZEND_ACC_PUBLIC)
    /* Query tracking methods */
    PHP_ME(ClickHouse_Client, getLastQueryId, arginfo_clickhouse_client_getlastqueryid, ZEND_ACC_PUBLIC)
    /* Metrics methods */
    PHP_ME(ClickHouse_Client, enableMetrics, arginfo_clickhouse_client_enablemetrics, ZEND_ACC_PUBLIC)
    PHP_ME(ClickHouse_Client, disableMetrics, arginfo_clickhouse_client_disablemetrics, ZEND_ACC_PUBLIC)
    PHP_ME(ClickHouse_Client, getMetrics, arginfo_clickhouse_client_getmetrics, ZEND_ACC_PUBLIC)
    PHP_ME(ClickHouse_Client, resetMetrics, arginfo_clickhouse_client_resetmetrics, ZEND_ACC_PUBLIC)
    PHP_ME(ClickHouse_Client, setSlowQueryThreshold, arginfo_clickhouse_client_setslowquerythreshold, ZEND_ACC_PUBLIC)
    /* Transaction methods (EXPERIMENTAL) */
    PHP_ME(ClickHouse_Client, beginTransaction, arginfo_clickhouse_client_begintransaction, ZEND_ACC_PUBLIC)
    PHP_ME(ClickHouse_Client, commit, arginfo_clickhouse_client_commit, ZEND_ACC_PUBLIC)
    PHP_ME(ClickHouse_Client, rollback, arginfo_clickhouse_client_rollback, ZEND_ACC_PUBLIC)
    PHP_ME(ClickHouse_Client, inTransaction, arginfo_clickhouse_client_intransaction, ZEND_ACC_PUBLIC)
    PHP_ME(ClickHouse_Client, setQuerySetting, arginfo_clickhouse_client_setquerysetting, ZEND_ACC_PUBLIC)
    PHP_ME(ClickHouse_Client, clearQuerySettings, arginfo_clickhouse_client_clearquerysettings, ZEND_ACC_PUBLIC)
    PHP_ME(ClickHouse_Client, queryWithTable, arginfo_clickhouse_client_querywithtable, ZEND_ACC_PUBLIC)
    PHP_ME(ClickHouse_Client, prepare, arginfo_clickhouse_client_prepare, ZEND_ACC_PUBLIC)
    PHP_ME(ClickHouse_Client, queryAsync, arginfo_clickhouse_client_queryasync, ZEND_ACC_PUBLIC)
    PHP_ME(ClickHouse_Client, queryIterator, arginfo_clickhouse_client_queryiterator, ZEND_ACC_PUBLIC)
    PHP_ME(ClickHouse_Client, queryStreaming, arginfo_clickhouse_client_querystreaming, ZEND_ACC_PUBLIC)
    PHP_FE_END
};
/* }}} */

/* {{{ Statement methods */
static const zend_function_entry clickhouse_statement_methods[] = {
    PHP_ME(ClickHouse_Statement, bind, arginfo_clickhouse_statement_bind, ZEND_ACC_PUBLIC)
    PHP_ME(ClickHouse_Statement, setOption, arginfo_clickhouse_statement_setoption, ZEND_ACC_PUBLIC)
    PHP_ME(ClickHouse_Statement, execute, arginfo_clickhouse_statement_execute, ZEND_ACC_PUBLIC)
    PHP_ME(ClickHouse_Statement, fetchAll, arginfo_clickhouse_statement_fetchall, ZEND_ACC_PUBLIC)
    PHP_FE_END
};
/* }}} */

/* {{{ AsyncResult methods */
static const zend_function_entry clickhouse_asyncresult_methods[] = {
    PHP_ME(ClickHouse_AsyncResult, poll, arginfo_clickhouse_asyncresult_poll, ZEND_ACC_PUBLIC)
    PHP_ME(ClickHouse_AsyncResult, wait, arginfo_clickhouse_asyncresult_wait, ZEND_ACC_PUBLIC)
    PHP_ME(ClickHouse_AsyncResult, getResult, arginfo_clickhouse_asyncresult_getresult, ZEND_ACC_PUBLIC)
    PHP_ME(ClickHouse_AsyncResult, isReady, arginfo_clickhouse_asyncresult_isready, ZEND_ACC_PUBLIC)
    PHP_ME(ClickHouse_AsyncResult, cancel, arginfo_clickhouse_asyncresult_cancel, ZEND_ACC_PUBLIC)
    PHP_FE_END
};
/* }}} */

/* {{{ ResultIterator methods */
static const zend_function_entry clickhouse_resultiterator_methods[] = {
    PHP_ME(ClickHouse_ResultIterator, rewind, arginfo_clickhouse_resultiterator_rewind, ZEND_ACC_PUBLIC)
    PHP_ME(ClickHouse_ResultIterator, current, arginfo_clickhouse_resultiterator_current, ZEND_ACC_PUBLIC)
    PHP_ME(ClickHouse_ResultIterator, key, arginfo_clickhouse_resultiterator_key, ZEND_ACC_PUBLIC)
    PHP_ME(ClickHouse_ResultIterator, next, arginfo_clickhouse_resultiterator_next, ZEND_ACC_PUBLIC)
    PHP_ME(ClickHouse_ResultIterator, valid, arginfo_clickhouse_resultiterator_valid, ZEND_ACC_PUBLIC)
    PHP_ME(ClickHouse_ResultIterator, count, arginfo_clickhouse_resultiterator_count, ZEND_ACC_PUBLIC)
    PHP_FE_END
};
/* }}} */

/* {{{ StreamingIterator methods */
static const zend_function_entry clickhouse_streamingiterator_methods[] = {
    PHP_ME(ClickHouse_StreamingIterator, rewind, arginfo_clickhouse_streamingiterator_rewind, ZEND_ACC_PUBLIC)
    PHP_ME(ClickHouse_StreamingIterator, current, arginfo_clickhouse_streamingiterator_current, ZEND_ACC_PUBLIC)
    PHP_ME(ClickHouse_StreamingIterator, key, arginfo_clickhouse_streamingiterator_key, ZEND_ACC_PUBLIC)
    PHP_ME(ClickHouse_StreamingIterator, next, arginfo_clickhouse_streamingiterator_next, ZEND_ACC_PUBLIC)
    PHP_ME(ClickHouse_StreamingIterator, valid, arginfo_clickhouse_streamingiterator_valid, ZEND_ACC_PUBLIC)
    PHP_ME(ClickHouse_StreamingIterator, count, arginfo_clickhouse_streamingiterator_count, ZEND_ACC_PUBLIC)
    PHP_FE_END
};
/* }}} */

/* {{{ INI Settings */
PHP_INI_BEGIN()
    STD_PHP_INI_BOOLEAN("clickhouse.allow_persistent", "1", PHP_INI_SYSTEM,
        OnUpdateBool, allow_persistent, zend_clickhouse_globals, clickhouse_globals)
    STD_PHP_INI_ENTRY("clickhouse.max_persistent", "-1", PHP_INI_SYSTEM,
        OnUpdateLong, max_persistent, zend_clickhouse_globals, clickhouse_globals)
    STD_PHP_INI_ENTRY("clickhouse.max_links", "-1", PHP_INI_SYSTEM,
        OnUpdateLong, max_links, zend_clickhouse_globals, clickhouse_globals)
PHP_INI_END()
/* }}} */

/* {{{ Globals initialization */
static PHP_GINIT_FUNCTION(clickhouse) {
#if defined(COMPILE_DL_CLICKHOUSE) && defined(ZTS)
    ZEND_TSRMLS_CACHE_UPDATE();
#endif
    clickhouse_globals->allow_persistent = 1;
    clickhouse_globals->max_persistent = -1;
    clickhouse_globals->max_links = -1;
    clickhouse_globals->num_persistent = 0;
    clickhouse_globals->num_links = 0;
}
/* }}} */

/* {{{ Persistent connection destructor */
static ZEND_RSRC_DTOR_FUNC(clickhouse_plist_dtor) {
    if (res->ptr) {
        clickhouse_plist_entry *plist = (clickhouse_plist_entry *)res->ptr;

        /* Close all connections in the stack */
        while (zend_ptr_stack_num_elements(&plist->free_connections) > 0) {
            clickhouse_connection *conn = zend_ptr_stack_pop(&plist->free_connections);
            if (conn) {
                clickhouse_connection_close(conn);
                clickhouse_connection_free(conn);
            }
        }

        zend_ptr_stack_destroy(&plist->free_connections);
        free(plist);
    }
}
/* }}} */

/* {{{ PHP_MINIT_FUNCTION */
PHP_MINIT_FUNCTION(clickhouse) {
    zend_class_entry ce;

    REGISTER_INI_ENTRIES();

    /* Register persistent connection resource type */
    le_pclickhouse = zend_register_list_destructors_ex(
        NULL, clickhouse_plist_dtor, "ClickHouse persistent connection", module_number);

    /* Register ClickHouse\Exception */
    INIT_NS_CLASS_ENTRY(ce, "ClickHouse", "Exception", NULL);
    clickhouse_exception_ce = zend_register_internal_class_ex(&ce, zend_ce_exception);

    /* Register ClickHouse\Client */
    INIT_NS_CLASS_ENTRY(ce, "ClickHouse", "Client", clickhouse_client_methods);
    clickhouse_client_ce = zend_register_internal_class(&ce);
    clickhouse_client_ce->create_object = clickhouse_client_create;

    memcpy(&clickhouse_client_handlers, &std_object_handlers, sizeof(zend_object_handlers));
    clickhouse_client_handlers.offset = XtOffsetOf(clickhouse_client_object, std);
    clickhouse_client_handlers.free_obj = clickhouse_client_free;

    /* Register ClickHouse\Statement */
    INIT_NS_CLASS_ENTRY(ce, "ClickHouse", "Statement", clickhouse_statement_methods);
    clickhouse_statement_ce = zend_register_internal_class(&ce);
    clickhouse_statement_ce->create_object = clickhouse_statement_create;

    memcpy(&clickhouse_statement_handlers, &std_object_handlers, sizeof(zend_object_handlers));
    clickhouse_statement_handlers.offset = XtOffsetOf(clickhouse_statement_object, std);
    clickhouse_statement_handlers.free_obj = clickhouse_statement_free;

    /* Register ClickHouse\AsyncResult */
    INIT_NS_CLASS_ENTRY(ce, "ClickHouse", "AsyncResult", clickhouse_asyncresult_methods);
    clickhouse_asyncresult_ce = zend_register_internal_class(&ce);
    clickhouse_asyncresult_ce->create_object = clickhouse_asyncresult_create;

    memcpy(&clickhouse_asyncresult_handlers, &std_object_handlers, sizeof(zend_object_handlers));
    clickhouse_asyncresult_handlers.offset = XtOffsetOf(clickhouse_asyncresult_object, std);
    clickhouse_asyncresult_handlers.free_obj = clickhouse_asyncresult_free;

    /* Register ClickHouse\ResultIterator */
    INIT_NS_CLASS_ENTRY(ce, "ClickHouse", "ResultIterator", clickhouse_resultiterator_methods);
    clickhouse_resultiterator_ce = zend_register_internal_class(&ce);
    clickhouse_resultiterator_ce->create_object = clickhouse_resultiterator_create;
    /* Implement Iterator and Countable interfaces */
    zend_class_implements(clickhouse_resultiterator_ce, 2, zend_ce_iterator, zend_ce_countable);

    memcpy(&clickhouse_resultiterator_handlers, &std_object_handlers, sizeof(zend_object_handlers));
    clickhouse_resultiterator_handlers.offset = XtOffsetOf(clickhouse_resultiterator_object, std);
    clickhouse_resultiterator_handlers.free_obj = clickhouse_resultiterator_free;

    /* Register ClickHouse\StreamingIterator */
    INIT_NS_CLASS_ENTRY(ce, "ClickHouse", "StreamingIterator", clickhouse_streamingiterator_methods);
    clickhouse_streamingiterator_ce = zend_register_internal_class(&ce);
    clickhouse_streamingiterator_ce->create_object = clickhouse_streamingiterator_create;
    /* Implement Iterator and Countable interfaces */
    zend_class_implements(clickhouse_streamingiterator_ce, 2, zend_ce_iterator, zend_ce_countable);

    memcpy(&clickhouse_streamingiterator_handlers, &std_object_handlers, sizeof(zend_object_handlers));
    clickhouse_streamingiterator_handlers.offset = XtOffsetOf(clickhouse_streamingiterator_object, std);
    clickhouse_streamingiterator_handlers.free_obj = clickhouse_streamingiterator_free;

    /* Register compression constants on Client class */
    zend_declare_class_constant_long(clickhouse_client_ce, "COMPRESS_NONE", sizeof("COMPRESS_NONE")-1, CH_COMPRESS_NONE);
    zend_declare_class_constant_long(clickhouse_client_ce, "COMPRESS_LZ4", sizeof("COMPRESS_LZ4")-1, CH_COMPRESS_LZ4);
    zend_declare_class_constant_long(clickhouse_client_ce, "COMPRESS_ZSTD", sizeof("COMPRESS_ZSTD")-1, CH_COMPRESS_ZSTD);

    return SUCCESS;
}
/* }}} */

/* {{{ PHP_MSHUTDOWN_FUNCTION */
PHP_MSHUTDOWN_FUNCTION(clickhouse) {
    UNREGISTER_INI_ENTRIES();
    return SUCCESS;
}
/* }}} */

/* {{{ PHP_RINIT_FUNCTION */
PHP_RINIT_FUNCTION(clickhouse) {
#if defined(ZTS) && defined(COMPILE_DL_CLICKHOUSE)
    ZEND_TSRMLS_CACHE_UPDATE();
#endif
    return SUCCESS;
}
/* }}} */

/* {{{ PHP_RSHUTDOWN_FUNCTION */
PHP_RSHUTDOWN_FUNCTION(clickhouse) {
    return SUCCESS;
}
/* }}} */

/* {{{ PHP_MINFO_FUNCTION */
PHP_MINFO_FUNCTION(clickhouse) {
    char buf[32];

    php_info_print_table_start();
    php_info_print_table_header(2, "ClickHouse Native Driver", "enabled");
    php_info_print_table_row(2, "Version", PHP_CLICKHOUSE_VERSION);
    php_info_print_table_row(2, "Protocol Version", "54460");
    php_info_print_table_row(2, "Persistent Connections",
        CLICKHOUSE_G(allow_persistent) ? "enabled" : "disabled");
    snprintf(buf, sizeof(buf), ZEND_LONG_FMT, CLICKHOUSE_G(num_persistent));
    php_info_print_table_row(2, "Active Persistent Connections", buf);
    snprintf(buf, sizeof(buf), ZEND_LONG_FMT, CLICKHOUSE_G(num_links));
    php_info_print_table_row(2, "Active Links", buf);
    php_info_print_table_end();

    DISPLAY_INI_ENTRIES();
}
/* }}} */

/* {{{ clickhouse_module_entry */
zend_module_entry clickhouse_module_entry = {
    STANDARD_MODULE_HEADER_EX,
    NULL,                       /* ini_entry */
    NULL,                       /* deps */
    PHP_CLICKHOUSE_EXTNAME,
    NULL,                       /* Functions */
    PHP_MINIT(clickhouse),
    PHP_MSHUTDOWN(clickhouse),
    PHP_RINIT(clickhouse),
    PHP_RSHUTDOWN(clickhouse),
    PHP_MINFO(clickhouse),
    PHP_CLICKHOUSE_VERSION,
    PHP_MODULE_GLOBALS(clickhouse),
    PHP_GINIT(clickhouse),
    NULL,                       /* GSHUTDOWN */
    NULL,                       /* PRSHUTDOWN */
    STANDARD_MODULE_PROPERTIES_EX
};
/* }}} */

#ifdef COMPILE_DL_CLICKHOUSE
#ifdef ZTS
ZEND_TSRMLS_CACHE_DEFINE()
#endif
ZEND_GET_MODULE(clickhouse)
#endif
