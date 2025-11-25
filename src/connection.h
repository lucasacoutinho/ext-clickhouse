/*
  +----------------------------------------------------------------------+
  | ClickHouse Native Driver - Connection Handling                      |
  +----------------------------------------------------------------------+
*/

#ifndef CLICKHOUSE_CONNECTION_H
#define CLICKHOUSE_CONNECTION_H

#include <stdint.h>
#include <stdbool.h>
#include "buffer.h"
#include "protocol.h"

/* Connection state */
typedef enum {
    CONN_STATE_DISCONNECTED = 0,
    CONN_STATE_CONNECTED,
    CONN_STATE_AUTHENTICATED
} clickhouse_conn_state;

/* SSL options */
typedef struct {
    int enabled;
    int verify_peer;
    int verify_host;
    char *ca_cert;       /* Path to CA certificate */
    char *client_cert;   /* Path to client certificate */
    char *client_key;    /* Path to client key */
} clickhouse_ssl_options;

/* Connection structure */
typedef struct {
    int socket_fd;
    char *host;
    uint16_t port;
    char *user;
    char *password;
    char *database;
    clickhouse_conn_state state;
    clickhouse_server_info *server_info;
    clickhouse_buffer *read_buf;
    clickhouse_buffer *write_buf;
    char *last_error;
    int connect_timeout;   /* seconds */
    int read_timeout;      /* seconds */
    int write_timeout;     /* seconds */
    uint8_t compression;   /* Active compression method for current query */
    /* SSL support */
    clickhouse_ssl_options ssl_opts;
    void *ssl;             /* SSL* - opaque to avoid including OpenSSL headers */
    void *ssl_ctx;         /* SSL_CTX* */
} clickhouse_connection;

/* Connection lifecycle */
clickhouse_connection *clickhouse_connection_create(const char *host, uint16_t port,
                                                    const char *user, const char *password,
                                                    const char *database);
void clickhouse_connection_free(clickhouse_connection *conn);
int clickhouse_connection_connect(clickhouse_connection *conn);
void clickhouse_connection_close(clickhouse_connection *conn);

/* Connection operations */
int clickhouse_connection_ping(clickhouse_connection *conn);
int clickhouse_connection_send(clickhouse_connection *conn);
int clickhouse_connection_receive(clickhouse_connection *conn);
int clickhouse_connection_receive_more(clickhouse_connection *conn);
int clickhouse_connection_read_packet_type(clickhouse_connection *conn, uint64_t *packet_type);

/* Error handling */
const char *clickhouse_connection_get_error(const clickhouse_connection *conn);
void clickhouse_connection_set_error(clickhouse_connection *conn, const char *error);

/* Timeout settings */
void clickhouse_connection_set_connect_timeout(clickhouse_connection *conn, int seconds);
void clickhouse_connection_set_read_timeout(clickhouse_connection *conn, int seconds);
void clickhouse_connection_set_write_timeout(clickhouse_connection *conn, int seconds);

/* Query timeout setting (milliseconds) - sets both read and write timeouts */
void clickhouse_connection_set_query_timeout_ms(clickhouse_connection *conn, int timeout_ms);

/* SSL settings */
void clickhouse_connection_set_ssl_enabled(clickhouse_connection *conn, int enabled);
void clickhouse_connection_set_ssl_verify(clickhouse_connection *conn, int verify_peer, int verify_host);
void clickhouse_connection_set_ssl_ca_cert(clickhouse_connection *conn, const char *ca_cert);
void clickhouse_connection_set_ssl_client_cert(clickhouse_connection *conn, const char *cert, const char *key);
int clickhouse_ssl_available(void);

/* Query execution */
#include "column.h"

/* Progress callback type */
typedef void (*clickhouse_progress_callback)(clickhouse_progress *progress, void *user_data);

/* Log callback type */
typedef void (*clickhouse_log_callback)(clickhouse_log_entry *entry, void *user_data);

typedef struct {
    clickhouse_block **blocks;
    size_t block_count;
    size_t block_capacity;
    clickhouse_block *totals;           /* Totals block (if WITH TOTALS) */
    clickhouse_block *extremes;         /* Extremes block (if SETTINGS extremes=1) */
    clickhouse_exception *exception;
    clickhouse_progress progress;
    clickhouse_profile_info profile;
    char *query_id;                     /* Query ID for tracking */
} clickhouse_result;

/* Query options */
typedef struct {
    clickhouse_settings *settings;
    clickhouse_params *params;
    uint8_t stage;
    uint8_t compression;
    clickhouse_progress_callback progress_callback;
    void *progress_user_data;
    clickhouse_log_callback log_callback;
    void *log_user_data;
    char *query_id;                     /* Custom query ID */
    char *session_id;                   /* Session ID for stateful queries */
    uint8_t session_check;              /* Verify session exists */
    clickhouse_external_tables *external_tables;  /* External tables for query */
} clickhouse_query_options;

clickhouse_result *clickhouse_result_create(void);
void clickhouse_result_free(clickhouse_result *result);
int clickhouse_result_add_block(clickhouse_result *result, clickhouse_block *block);

/* Query option helpers */
clickhouse_query_options *clickhouse_query_options_create(void);
void clickhouse_query_options_free(clickhouse_query_options *opts);
int clickhouse_query_options_set_setting(clickhouse_query_options *opts, const char *name, const char *value);
int clickhouse_query_options_set_param(clickhouse_query_options *opts, const char *name, const char *value, const char *type);

/* Query execution */
int clickhouse_connection_execute_query(clickhouse_connection *conn, const char *query,
                                        clickhouse_result **result);
int clickhouse_connection_execute_query_ext(clickhouse_connection *conn, const char *query,
                                            clickhouse_query_options *options,
                                            clickhouse_result **result);
int clickhouse_connection_send_data(clickhouse_connection *conn, clickhouse_block *block);
int clickhouse_connection_send_data_named(clickhouse_connection *conn, clickhouse_block *block, const char *table_name);
int clickhouse_connection_send_empty_block(clickhouse_connection *conn);
int clickhouse_connection_send_external_tables(clickhouse_connection *conn, clickhouse_external_tables *tables);

/* Insert raw formatted data (CSV, TSV, JSONEachRow, etc.) */
int clickhouse_connection_insert_format_data(clickhouse_connection *conn, const char *table,
                                              const char *format, const void *data, size_t data_len);

/* Query cancellation */
int clickhouse_connection_cancel(clickhouse_connection *conn);

/* Async query support */
typedef enum {
    ASYNC_STATE_IDLE = 0,
    ASYNC_STATE_SENDING,
    ASYNC_STATE_WAITING,
    ASYNC_STATE_READING,
    ASYNC_STATE_COMPLETE,
    ASYNC_STATE_ERROR
} clickhouse_async_state;

typedef struct {
    clickhouse_async_state state;
    clickhouse_result *result;
    clickhouse_query_options *options;
    char *error;
} clickhouse_async_query;

clickhouse_async_query *clickhouse_async_query_create(void);
void clickhouse_async_query_free(clickhouse_async_query *async);

/* Start async query - returns immediately after sending */
int clickhouse_connection_query_async(clickhouse_connection *conn, const char *query,
                                       clickhouse_query_options *options,
                                       clickhouse_async_query **async_out);

/* Poll for results - non-blocking, returns 1 if complete, 0 if pending, -1 on error */
int clickhouse_async_poll(clickhouse_connection *conn, clickhouse_async_query *async);

/* Check if connection has data waiting to be read (non-blocking) */
int clickhouse_connection_has_data(clickhouse_connection *conn, int timeout_ms);

/* Streaming query support - true row-by-row streaming without buffering */
typedef enum {
    STREAM_STATE_INIT = 0,
    STREAM_STATE_SENT,
    STREAM_STATE_RECEIVING,
    STREAM_STATE_COMPLETE,
    STREAM_STATE_ERROR
} clickhouse_stream_state;

typedef struct {
    clickhouse_connection *conn;
    clickhouse_stream_state state;
    clickhouse_query_options *options;
    char *query_id;

    /* Current block being streamed */
    clickhouse_block *current_block;
    size_t current_row;

    /* Streaming state */
    int done;
    int first_receive;

    /* Metadata blocks */
    clickhouse_block *totals;
    clickhouse_block *extremes;

    /* Progress and profile tracking */
    clickhouse_progress progress;
    clickhouse_profile_info profile;

    /* Error handling */
    clickhouse_exception *exception;
    char *error;
} clickhouse_streaming_query;

/* Streaming query lifecycle */
clickhouse_streaming_query *clickhouse_streaming_query_create(void);
void clickhouse_streaming_query_free(clickhouse_streaming_query *sq);

/* Initialize and start streaming query - sends query and prepares for streaming */
int clickhouse_connection_query_streaming(clickhouse_connection *conn, const char *query,
                                          clickhouse_query_options *options,
                                          clickhouse_streaming_query **sq_out);

/* Fetch next block from server - returns 1 if block available, 0 if done, -1 on error */
int clickhouse_streaming_fetch_next_block(clickhouse_streaming_query *sq);

/* Get current row from current block - advances to next row */
int clickhouse_streaming_get_current_row(clickhouse_streaming_query *sq, void **row_data);

/* Check if streaming query is complete */
int clickhouse_streaming_is_done(clickhouse_streaming_query *sq);

#endif /* CLICKHOUSE_CONNECTION_H */
