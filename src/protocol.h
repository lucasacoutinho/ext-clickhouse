/*
  +----------------------------------------------------------------------+
  | ClickHouse Native Driver - Protocol Constants and Messages          |
  +----------------------------------------------------------------------+
*/

#ifndef CLICKHOUSE_PROTOCOL_H
#define CLICKHOUSE_PROTOCOL_H

#include <stdint.h>
#include "buffer.h"

/* ClickHouse protocol revision - using stable revision */
#define CLICKHOUSE_REVISION 54429
#define CLICKHOUSE_MIN_REVISION 54406

/* Client packet types */
#define CH_CLIENT_HELLO         0
#define CH_CLIENT_QUERY         1
#define CH_CLIENT_DATA          2
#define CH_CLIENT_CANCEL        3
#define CH_CLIENT_PING          4
#define CH_CLIENT_TABLE_STATUS  5

/* Server packet types */
#define CH_SERVER_HELLO         0
#define CH_SERVER_DATA          1
#define CH_SERVER_EXCEPTION     2
#define CH_SERVER_PROGRESS      3
#define CH_SERVER_PONG          4
#define CH_SERVER_END_OF_STREAM 5
#define CH_SERVER_PROFILE_INFO  6
#define CH_SERVER_TOTALS        7
#define CH_SERVER_EXTREMES      8
#define CH_SERVER_TABLE_STATUS  9
#define CH_SERVER_LOG           10
#define CH_SERVER_TABLE_COLUMNS 11

/* Query stages */
#define CH_STAGE_FETCH_COLUMNS      0
#define CH_STAGE_WITH_MERGEABLE     1
#define CH_STAGE_COMPLETE           2

/* Compression methods */
#define CH_COMPRESS_NONE    0
#define CH_COMPRESS_LZ4     1
#define CH_COMPRESS_ZSTD    2

/* Compression byte codes (in wire format) */
#define CH_COMPRESS_BYTE_NONE   0x02
#define CH_COMPRESS_BYTE_LZ4    0x82
#define CH_COMPRESS_BYTE_ZSTD   0x90

/* Compression header size: method(1) + compressed_size(4) + original_size(4) */
#define CH_COMPRESS_HEADER_SIZE 9

/* Protocol revisions for feature detection */
#define CH_REVISION_CLIENT_INFO            54032
#define CH_REVISION_QUOTA_KEY_IN_CLIENT    54060
#define CH_REVISION_VERSION_PATCH          54401
#define CH_REVISION_SETTINGS_AS_STRINGS    54429
#define CH_REVISION_INTERSERVER_SECRET     54441
#define CH_REVISION_OPENTELEMETRY          54442
#define CH_REVISION_DISTRIBUTED_DEPTH      54448
#define CH_REVISION_INITIAL_QUERY_TIME     54449
#define CH_REVISION_PARALLEL_REPLICAS      54453
#define CH_REVISION_QUOTA_KEY              54458
#define CH_REVISION_PARAMETERS             54459

/* Query setting flags */
#define CH_SETTING_IMPORTANT   0x01
#define CH_SETTING_CUSTOM      0x02
#define CH_SETTING_OBSOLETE    0x04

/* Query setting entry */
typedef struct clickhouse_setting {
    char *name;
    char *value;
    uint64_t flags;
    struct clickhouse_setting *next;
} clickhouse_setting;

/* Query settings collection */
typedef struct {
    clickhouse_setting *head;
    size_t count;
} clickhouse_settings;

/* Query parameters (for prepared statements) */
typedef struct clickhouse_param {
    char *name;
    char *value;
    char *type;  /* ClickHouse type name */
    struct clickhouse_param *next;
} clickhouse_param;

typedef struct {
    clickhouse_param *head;
    size_t count;
} clickhouse_params;

/* Client info */
typedef struct {
    uint8_t query_kind;
    char *initial_user;
    char *initial_query_id;
    char *initial_address;
    uint8_t interface_type;  /* 1 = TCP, 2 = HTTP */
    char *os_user;
    char *client_hostname;
    char *client_name;
    uint64_t client_version_major;
    uint64_t client_version_minor;
    uint64_t client_revision;
    char *quota_key;
} clickhouse_client_info;

/* Server info (from hello response) */
typedef struct {
    char *name;
    uint64_t version_major;
    uint64_t version_minor;
    uint64_t revision;
    char *timezone;
    char *display_name;
    uint64_t version_patch;
} clickhouse_server_info;

/* Progress info */
typedef struct {
    uint64_t rows;
    uint64_t bytes;
    uint64_t total_rows;
    uint64_t written_rows;
    uint64_t written_bytes;
} clickhouse_progress;

/* Profile info */
typedef struct {
    uint64_t rows;
    uint64_t blocks;
    uint64_t bytes;
    uint8_t applied_limit;
    uint64_t rows_before_limit;
    uint8_t calculated_rows_before_limit;
} clickhouse_profile_info;

/* Exception info */
typedef struct {
    int32_t code;
    char *name;
    char *message;
    char *stack_trace;
    uint8_t has_nested;
    struct clickhouse_exception *nested;
} clickhouse_exception;

/* Client info functions */
clickhouse_client_info *clickhouse_client_info_create(void);
void clickhouse_client_info_free(clickhouse_client_info *info);
int clickhouse_client_info_write(clickhouse_buffer *buf, clickhouse_client_info *info, uint64_t server_revision);

/* Server info functions */
clickhouse_server_info *clickhouse_server_info_create(void);
void clickhouse_server_info_free(clickhouse_server_info *info);
int clickhouse_server_info_read(clickhouse_buffer *buf, clickhouse_server_info *info);

/* Hello packet */
int clickhouse_write_hello(clickhouse_buffer *buf, const char *database, const char *user, const char *password);
int clickhouse_read_hello(clickhouse_buffer *buf, clickhouse_server_info *info);

/* Query packet */
int clickhouse_write_query(clickhouse_buffer *buf, const char *query_id, clickhouse_client_info *client_info,
                           const char *query, uint8_t stage, uint8_t compression, uint64_t server_revision);

/* Ping/Pong */
int clickhouse_write_ping(clickhouse_buffer *buf);

/* Exception handling */
clickhouse_exception *clickhouse_exception_read(clickhouse_buffer *buf);
void clickhouse_exception_free(clickhouse_exception *ex);

/* Progress handling */
int clickhouse_progress_read(clickhouse_buffer *buf, clickhouse_progress *progress);

/* Profile info handling */
int clickhouse_profile_info_read(clickhouse_buffer *buf, clickhouse_profile_info *info);

/* Query settings */
clickhouse_settings *clickhouse_settings_create(void);
void clickhouse_settings_free(clickhouse_settings *settings);
int clickhouse_settings_add(clickhouse_settings *settings, const char *name, const char *value, uint64_t flags);
int clickhouse_settings_write(clickhouse_buffer *buf, clickhouse_settings *settings);

/* Query parameters */
clickhouse_params *clickhouse_params_create(void);
void clickhouse_params_free(clickhouse_params *params);
int clickhouse_params_add(clickhouse_params *params, const char *name, const char *value, const char *type);
int clickhouse_params_write(clickhouse_buffer *buf, clickhouse_params *params);

/* External table column definition */
typedef struct {
    char *name;
    char *type;          /* ClickHouse type name (e.g., "UInt32", "String") */
    void *data;          /* Column data */
    size_t row_count;
} clickhouse_external_column;

/* External table definition */
typedef struct clickhouse_external_table {
    char *name;                              /* Table name used in query */
    clickhouse_external_column *columns;     /* Array of columns */
    size_t column_count;
    size_t row_count;
    struct clickhouse_external_table *next;
} clickhouse_external_table;

/* External tables collection */
typedef struct {
    clickhouse_external_table *head;
    size_t count;
} clickhouse_external_tables;

/* External tables functions */
clickhouse_external_tables *clickhouse_external_tables_create(void);
void clickhouse_external_tables_free(clickhouse_external_tables *tables);
clickhouse_external_table *clickhouse_external_table_create(const char *name);
void clickhouse_external_table_free(clickhouse_external_table *table);
int clickhouse_external_table_add_column(clickhouse_external_table *table, const char *name, const char *type);
int clickhouse_external_tables_add(clickhouse_external_tables *tables, clickhouse_external_table *table);

/* Extended query with settings and parameters */
int clickhouse_write_query_ext(clickhouse_buffer *buf, const char *query_id,
                               clickhouse_client_info *client_info, const char *query,
                               clickhouse_settings *settings, clickhouse_params *params,
                               uint8_t stage, uint8_t compression, uint64_t server_revision);

/* Cancel query */
int clickhouse_write_cancel(clickhouse_buffer *buf);

/* Compression functions */
typedef struct {
    uint8_t *data;
    size_t size;
    size_t original_size;
    uint8_t method;
} clickhouse_compressed_block;

clickhouse_compressed_block *clickhouse_compress_lz4(const uint8_t *data, size_t len);
clickhouse_compressed_block *clickhouse_compress_zstd(const uint8_t *data, size_t len);
uint8_t *clickhouse_decompress(const uint8_t *data, size_t compressed_size,
                               size_t original_size, uint8_t method, size_t *out_size);
void clickhouse_compressed_block_free(clickhouse_compressed_block *block);
int clickhouse_compression_supported(uint8_t method);
int clickhouse_is_compressed_block(clickhouse_buffer *buf);
int clickhouse_read_compressed_block(clickhouse_buffer *buf, clickhouse_buffer **out_buf);

#endif /* CLICKHOUSE_PROTOCOL_H */
