/*
  +----------------------------------------------------------------------+
  | ClickHouse Native Driver - Protocol Implementation                  |
  +----------------------------------------------------------------------+
*/

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include "protocol.h"
#include "cityhash.h"
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <stdio.h>

#ifdef HAVE_LZ4
#include <lz4.h>
#endif

#ifdef HAVE_ZSTD
#include <zstd.h>
#endif

/* Compute CityHash128 checksum for compressed block */
static void compute_checksum(const uint8_t *data, size_t len, uint8_t *out) {
    cityhash128_t hash = cityhash128((const char *)data, len);
    memcpy(out, &hash.low, 8);
    memcpy(out + 8, &hash.high, 8);
}

/* Verify CityHash128 checksum */
static int verify_checksum(const uint8_t *expected, const uint8_t *data, size_t len) {
    cityhash128_t hash = cityhash128((const char *)data, len);
    uint64_t expected_low, expected_high;
    memcpy(&expected_low, expected, 8);
    memcpy(&expected_high, expected + 8, 8);
    return (hash.low == expected_low && hash.high == expected_high);
}

#define CLIENT_NAME "php-clickhouse-native"
#define CLIENT_VERSION_MAJOR 0
#define CLIENT_VERSION_MINOR 1

/* Client info functions */

clickhouse_client_info *clickhouse_client_info_create(void) {
    clickhouse_client_info *info = calloc(1, sizeof(clickhouse_client_info));
    if (!info) {
        return NULL;
    }

    info->query_kind = 1; /* Initial query */
    info->interface_type = 1; /* TCP */

    /* Set defaults */
    info->initial_user = strdup("");
    info->initial_query_id = strdup("");
    info->initial_address = strdup("0.0.0.0:0");

    char hostname[256];
    if (gethostname(hostname, sizeof(hostname)) == 0) {
        info->client_hostname = strdup(hostname);
    } else {
        info->client_hostname = strdup("unknown");
    }

    info->os_user = strdup("");
    info->client_name = strdup(CLIENT_NAME);
    info->client_version_major = CLIENT_VERSION_MAJOR;
    info->client_version_minor = CLIENT_VERSION_MINOR;
    info->client_revision = CLICKHOUSE_REVISION;
    info->quota_key = strdup("");

    return info;
}

void clickhouse_client_info_free(clickhouse_client_info *info) {
    if (info) {
        free(info->initial_user);
        free(info->initial_query_id);
        free(info->initial_address);
        free(info->os_user);
        free(info->client_hostname);
        free(info->client_name);
        free(info->quota_key);
        free(info);
    }
}

int clickhouse_client_info_write(clickhouse_buffer *buf, clickhouse_client_info *info, uint64_t server_revision) {
    /* Query kind */
    if (clickhouse_buffer_write_uint8(buf, info->query_kind) != 0) return -1;

    /* Initial user */
    if (clickhouse_buffer_write_string(buf, info->initial_user, strlen(info->initial_user)) != 0) return -1;

    /* Initial query ID */
    if (clickhouse_buffer_write_string(buf, info->initial_query_id, strlen(info->initial_query_id)) != 0) return -1;

    /* Initial address */
    if (clickhouse_buffer_write_string(buf, info->initial_address, strlen(info->initial_address)) != 0) return -1;

    /* Initial query start time (revision >= 54449) */
    if (server_revision >= CH_REVISION_INITIAL_QUERY_TIME) {
        int64_t start_time = 0;
        if (clickhouse_buffer_write_bytes(buf, (uint8_t *)&start_time, sizeof(start_time)) != 0) return -1;
    }

    /* Interface type (TCP = 1) */
    if (clickhouse_buffer_write_uint8(buf, info->interface_type) != 0) return -1;

    /* OS user */
    if (clickhouse_buffer_write_string(buf, info->os_user, strlen(info->os_user)) != 0) return -1;

    /* Client hostname */
    if (clickhouse_buffer_write_string(buf, info->client_hostname, strlen(info->client_hostname)) != 0) return -1;

    /* Client name */
    if (clickhouse_buffer_write_string(buf, info->client_name, strlen(info->client_name)) != 0) return -1;

    /* Client version */
    if (clickhouse_buffer_write_varint(buf, info->client_version_major) != 0) return -1;
    if (clickhouse_buffer_write_varint(buf, info->client_version_minor) != 0) return -1;
    if (clickhouse_buffer_write_varint(buf, info->client_revision) != 0) return -1;

    /* Quota key in client info (revision >= 54060) */
    if (server_revision >= CH_REVISION_QUOTA_KEY_IN_CLIENT) {
        if (clickhouse_buffer_write_string(buf, info->quota_key, strlen(info->quota_key)) != 0) return -1;
    }

    /* Distributed depth (revision >= 54448) */
    if (server_revision >= CH_REVISION_DISTRIBUTED_DEPTH) {
        if (clickhouse_buffer_write_varint(buf, 0) != 0) return -1;
    }

    /* Version patch (revision >= 54401) */
    if (server_revision >= CH_REVISION_VERSION_PATCH) {
        if (clickhouse_buffer_write_varint(buf, 0) != 0) return -1;  /* version patch = 0 */
    }

    /* OpenTelemetry (revision >= 54442) */
    if (server_revision >= CH_REVISION_OPENTELEMETRY) {
        if (clickhouse_buffer_write_uint8(buf, 0) != 0) return -1;  /* No OpenTelemetry */
    }

    /* Parallel replicas (revision >= 54453) */
    if (server_revision >= CH_REVISION_PARALLEL_REPLICAS) {
        if (clickhouse_buffer_write_varint(buf, 0) != 0) return -1;  /* collaborate_with_initiator */
        if (clickhouse_buffer_write_varint(buf, 0) != 0) return -1;  /* count_participating_replicas */
        if (clickhouse_buffer_write_varint(buf, 0) != 0) return -1;  /* number_of_current_replica */
    }

    return 0;
}

/* Server info functions */

clickhouse_server_info *clickhouse_server_info_create(void) {
    return calloc(1, sizeof(clickhouse_server_info));
}

void clickhouse_server_info_free(clickhouse_server_info *info) {
    if (info) {
        free(info->name);
        free(info->timezone);
        free(info->display_name);
        free(info);
    }
}

int clickhouse_server_info_read(clickhouse_buffer *buf, clickhouse_server_info *info) {
    size_t len;

    if (clickhouse_buffer_read_string(buf, &info->name, &len) != 0) return -1;
    if (clickhouse_buffer_read_varint(buf, &info->version_major) != 0) return -1;
    if (clickhouse_buffer_read_varint(buf, &info->version_minor) != 0) return -1;
    if (clickhouse_buffer_read_varint(buf, &info->revision) != 0) return -1;

    /* Timezone (revision >= 54058) */
    if (info->revision >= 54058) {
        if (clickhouse_buffer_read_string(buf, &info->timezone, &len) != 0) return -1;
    }

    /* Display name (revision >= 54372) */
    if (info->revision >= 54372) {
        if (clickhouse_buffer_read_string(buf, &info->display_name, &len) != 0) return -1;
    }

    /* Version patch (revision >= 54401) */
    if (info->revision >= 54401) {
        if (clickhouse_buffer_read_varint(buf, &info->version_patch) != 0) return -1;
    }

    return 0;
}

/* Hello packet */

int clickhouse_write_hello(clickhouse_buffer *buf, const char *database, const char *user, const char *password) {
    /* Packet type */
    if (clickhouse_buffer_write_varint(buf, CH_CLIENT_HELLO) != 0) return -1;

    /* Client name */
    if (clickhouse_buffer_write_string(buf, CLIENT_NAME, strlen(CLIENT_NAME)) != 0) return -1;

    /* Client version */
    if (clickhouse_buffer_write_varint(buf, CLIENT_VERSION_MAJOR) != 0) return -1;
    if (clickhouse_buffer_write_varint(buf, CLIENT_VERSION_MINOR) != 0) return -1;
    if (clickhouse_buffer_write_varint(buf, CLICKHOUSE_REVISION) != 0) return -1;

    /* Database */
    if (clickhouse_buffer_write_string(buf, database, strlen(database)) != 0) return -1;

    /* User */
    if (clickhouse_buffer_write_string(buf, user, strlen(user)) != 0) return -1;

    /* Password */
    if (clickhouse_buffer_write_string(buf, password, strlen(password)) != 0) return -1;

    return 0;
}

int clickhouse_read_hello(clickhouse_buffer *buf, clickhouse_server_info *info) {
    uint64_t packet_type;
    if (clickhouse_buffer_read_varint(buf, &packet_type) != 0) return -1;

    if (packet_type == CH_SERVER_EXCEPTION) {
        /* Server returned an exception */
        return -2;
    }

    if (packet_type != CH_SERVER_HELLO) {
        return -1;
    }

    return clickhouse_server_info_read(buf, info);
}

/* Query packet */

int clickhouse_write_query(clickhouse_buffer *buf, const char *query_id, clickhouse_client_info *client_info,
                           const char *query, uint8_t stage, uint8_t compression, uint64_t server_revision) {
    /* Packet type */
    if (clickhouse_buffer_write_varint(buf, CH_CLIENT_QUERY) != 0) return -1;

    /* Query ID */
    if (clickhouse_buffer_write_string(buf, query_id, strlen(query_id)) != 0) return -1;

    /* Client info (revision >= 54032) */
    if (server_revision >= CH_REVISION_CLIENT_INFO) {
        if (clickhouse_client_info_write(buf, client_info, server_revision) != 0) return -1;
    }

    /* Settings (empty string = no settings) */
    if (clickhouse_buffer_write_string(buf, "", 0) != 0) return -1;

    /* Interserver secret (revision >= 54441) - empty for client connections */
    if (server_revision >= CH_REVISION_INTERSERVER_SECRET) {
        if (clickhouse_buffer_write_string(buf, "", 0) != 0) return -1;
    }

    /* Query stage */
    if (clickhouse_buffer_write_varint(buf, stage) != 0) return -1;

    /* Compression - this is a boolean flag (0 = no compression, 1 = compression enabled)
     * The actual compression method (LZ4/ZSTD) is determined by the method byte in each block */
    if (clickhouse_buffer_write_varint(buf, compression != CH_COMPRESS_NONE ? 1 : 0) != 0) return -1;

    /* Query text */
    if (clickhouse_buffer_write_string(buf, query, strlen(query)) != 0) return -1;

    return 0;
}

/* Ping/Pong */

int clickhouse_write_ping(clickhouse_buffer *buf) {
    return clickhouse_buffer_write_varint(buf, CH_CLIENT_PING);
}

/* Exception handling */

clickhouse_exception *clickhouse_exception_read(clickhouse_buffer *buf) {
    clickhouse_exception *ex = calloc(1, sizeof(clickhouse_exception));
    if (!ex) return NULL;

    size_t len;

    if (clickhouse_buffer_read_int32(buf, &ex->code) != 0) goto error;
    if (clickhouse_buffer_read_string(buf, &ex->name, &len) != 0) goto error;
    if (clickhouse_buffer_read_string(buf, &ex->message, &len) != 0) goto error;
    if (clickhouse_buffer_read_string(buf, &ex->stack_trace, &len) != 0) goto error;
    if (clickhouse_buffer_read_uint8(buf, &ex->has_nested) != 0) goto error;

    if (ex->has_nested) {
        ex->nested = (struct clickhouse_exception *)clickhouse_exception_read(buf);
    }

    return ex;

error:
    clickhouse_exception_free(ex);
    return NULL;
}

void clickhouse_exception_free(clickhouse_exception *ex) {
    if (ex) {
        free(ex->name);
        free(ex->message);
        free(ex->stack_trace);
        if (ex->nested) {
            clickhouse_exception_free((clickhouse_exception *)ex->nested);
        }
        free(ex);
    }
}

/* Progress handling */

int clickhouse_progress_read(clickhouse_buffer *buf, clickhouse_progress *progress) {
    if (clickhouse_buffer_read_varint(buf, &progress->rows) != 0) return -1;
    if (clickhouse_buffer_read_varint(buf, &progress->bytes) != 0) return -1;
    if (clickhouse_buffer_read_varint(buf, &progress->total_rows) != 0) return -1;
    if (clickhouse_buffer_read_varint(buf, &progress->written_rows) != 0) return -1;
    if (clickhouse_buffer_read_varint(buf, &progress->written_bytes) != 0) return -1;
    return 0;
}

/* Profile info handling */

int clickhouse_profile_info_read(clickhouse_buffer *buf, clickhouse_profile_info *info) {
    if (clickhouse_buffer_read_varint(buf, &info->rows) != 0) return -1;
    if (clickhouse_buffer_read_varint(buf, &info->blocks) != 0) return -1;
    if (clickhouse_buffer_read_varint(buf, &info->bytes) != 0) return -1;
    if (clickhouse_buffer_read_uint8(buf, &info->applied_limit) != 0) return -1;
    if (clickhouse_buffer_read_varint(buf, &info->rows_before_limit) != 0) return -1;
    if (clickhouse_buffer_read_uint8(buf, &info->calculated_rows_before_limit) != 0) return -1;
    return 0;
}

/* Query settings */

clickhouse_settings *clickhouse_settings_create(void) {
    clickhouse_settings *settings = calloc(1, sizeof(clickhouse_settings));
    return settings;
}

void clickhouse_settings_free(clickhouse_settings *settings) {
    if (settings) {
        clickhouse_setting *current = settings->head;
        while (current) {
            clickhouse_setting *next = current->next;
            free(current->name);
            free(current->value);
            free(current);
            current = next;
        }
        free(settings);
    }
}

int clickhouse_settings_add(clickhouse_settings *settings, const char *name, const char *value, uint64_t flags) {
    clickhouse_setting *setting = calloc(1, sizeof(clickhouse_setting));
    if (!setting) return -1;

    setting->name = strdup(name);
    setting->value = strdup(value);
    setting->flags = flags;

    if (!setting->name || !setting->value) {
        free(setting->name);
        free(setting->value);
        free(setting);
        return -1;
    }

    /* Add to front of list */
    setting->next = settings->head;
    settings->head = setting;
    settings->count++;

    return 0;
}

int clickhouse_settings_write(clickhouse_buffer *buf, clickhouse_settings *settings) {
    if (!settings) {
        /* Empty settings - just write empty string */
        return clickhouse_buffer_write_string(buf, "", 0);
    }

    clickhouse_setting *current = settings->head;
    while (current) {
        /* Write setting name */
        if (clickhouse_buffer_write_string(buf, current->name, strlen(current->name)) != 0) return -1;
        /* Write flags (1 = custom setting) */
        if (clickhouse_buffer_write_varint(buf, current->flags) != 0) return -1;
        /* Write value as string */
        if (clickhouse_buffer_write_string(buf, current->value, strlen(current->value)) != 0) return -1;
        current = current->next;
    }

    /* Empty string to end settings */
    return clickhouse_buffer_write_string(buf, "", 0);
}

/* Query parameters */

clickhouse_params *clickhouse_params_create(void) {
    clickhouse_params *params = calloc(1, sizeof(clickhouse_params));
    return params;
}

void clickhouse_params_free(clickhouse_params *params) {
    if (params) {
        clickhouse_param *current = params->head;
        while (current) {
            clickhouse_param *next = current->next;
            free(current->name);
            free(current->value);
            free(current->type);
            free(current);
            current = next;
        }
        free(params);
    }
}

int clickhouse_params_add(clickhouse_params *params, const char *name, const char *value, const char *type) {
    clickhouse_param *param = calloc(1, sizeof(clickhouse_param));
    if (!param) return -1;

    param->name = strdup(name);
    param->value = strdup(value);
    param->type = type ? strdup(type) : strdup("String");

    if (!param->name || !param->value || !param->type) {
        free(param->name);
        free(param->value);
        free(param->type);
        free(param);
        return -1;
    }

    /* Add to front of list */
    param->next = params->head;
    params->head = param;
    params->count++;

    return 0;
}

int clickhouse_params_write(clickhouse_buffer *buf, clickhouse_params *params) {
    if (!params || params->count == 0) {
        return 0; /* No parameters to write */
    }

    clickhouse_param *current = params->head;
    while (current) {
        /* Write parameter name */
        if (clickhouse_buffer_write_string(buf, current->name, strlen(current->name)) != 0) return -1;
        /* Write parameter type */
        if (clickhouse_buffer_write_string(buf, current->type, strlen(current->type)) != 0) return -1;
        /* Write parameter value */
        if (clickhouse_buffer_write_string(buf, current->value, strlen(current->value)) != 0) return -1;
        current = current->next;
    }

    return 0;
}

/* Extended query with settings and parameters */

int clickhouse_write_query_ext(clickhouse_buffer *buf, const char *query_id,
                               clickhouse_client_info *client_info, const char *query,
                               clickhouse_settings *settings, clickhouse_params *params,
                               uint8_t stage, uint8_t compression, uint64_t server_revision) {
    /* Packet type */
    if (clickhouse_buffer_write_varint(buf, CH_CLIENT_QUERY) != 0) return -1;

    /* Query ID */
    if (clickhouse_buffer_write_string(buf, query_id, strlen(query_id)) != 0) return -1;

    /* Client info (revision >= 54032) */
    if (server_revision >= CH_REVISION_CLIENT_INFO) {
        if (clickhouse_client_info_write(buf, client_info, server_revision) != 0) return -1;
    }

    /* Settings */
    if (clickhouse_settings_write(buf, settings) != 0) return -1;

    /* Interserver secret (revision >= 54441) - empty for client connections */
    if (server_revision >= CH_REVISION_INTERSERVER_SECRET) {
        if (clickhouse_buffer_write_string(buf, "", 0) != 0) return -1;
    }

    /* Query stage */
    if (clickhouse_buffer_write_varint(buf, stage) != 0) return -1;

    /* Compression - this is a boolean flag (0 = no compression, 1 = compression enabled)
     * The actual compression method (LZ4/ZSTD) is determined by the method byte in each block */
    if (clickhouse_buffer_write_varint(buf, compression != CH_COMPRESS_NONE ? 1 : 0) != 0) return -1;

    /* Query text */
    if (clickhouse_buffer_write_string(buf, query, strlen(query)) != 0) return -1;

    /* Parameters (if any, revision >= 54459) */
    if (server_revision >= CH_REVISION_PARAMETERS && params && params->count > 0) {
        if (clickhouse_params_write(buf, params) != 0) return -1;
        /* Empty string to end parameters */
        if (clickhouse_buffer_write_string(buf, "", 0) != 0) return -1;
    }

    return 0;
}

/* Cancel query */

int clickhouse_write_cancel(clickhouse_buffer *buf) {
    return clickhouse_buffer_write_varint(buf, CH_CLIENT_CANCEL);
}

/* Compression functions */

clickhouse_compressed_block *clickhouse_compress_lz4(const uint8_t *data, size_t len) {
#ifdef HAVE_LZ4
    if (!data || len == 0) return NULL;

    clickhouse_compressed_block *block = calloc(1, sizeof(clickhouse_compressed_block));
    if (!block) return NULL;

    /* Calculate max compressed size */
    int max_compressed = LZ4_compressBound((int)len);
    if (max_compressed <= 0) {
        free(block);
        return NULL;
    }

    /* Allocate buffer: 16 bytes checksum + 1 byte method + 4 bytes compressed + 4 bytes original + data */
    size_t header_size = 16 + CH_COMPRESS_HEADER_SIZE;
    block->data = malloc(header_size + max_compressed);
    if (!block->data) {
        free(block);
        return NULL;
    }

    /* Compress the data */
    int compressed_len = LZ4_compress_default((const char *)data,
                                               (char *)(block->data + header_size),
                                               (int)len, max_compressed);
    if (compressed_len <= 0) {
        free(block->data);
        free(block);
        return NULL;
    }

    /* Fill header after checksum (16 bytes) */
    uint8_t *header = block->data + 16;
    header[0] = CH_COMPRESS_BYTE_LZ4;

    /* Compressed size (including header) in little-endian */
    uint32_t total_compressed = compressed_len + CH_COMPRESS_HEADER_SIZE;
    memcpy(header + 1, &total_compressed, 4);

    /* Original size in little-endian */
    uint32_t orig_size = (uint32_t)len;
    memcpy(header + 5, &orig_size, 4);

    /* Compute checksum of header + compressed data */
    compute_checksum(header, CH_COMPRESS_HEADER_SIZE + compressed_len, block->data);

    block->size = header_size + compressed_len;
    block->original_size = len;
    block->method = CH_COMPRESS_LZ4;

    return block;
#else
    (void)data;
    (void)len;
    return NULL;
#endif
}

clickhouse_compressed_block *clickhouse_compress_zstd(const uint8_t *data, size_t len) {
#ifdef HAVE_ZSTD
    if (!data || len == 0) return NULL;

    clickhouse_compressed_block *block = calloc(1, sizeof(clickhouse_compressed_block));
    if (!block) return NULL;

    /* Calculate max compressed size */
    size_t max_compressed = ZSTD_compressBound(len);

    /* Allocate buffer: 16 bytes checksum + 1 byte method + 4 bytes compressed + 4 bytes original + data */
    size_t header_size = 16 + CH_COMPRESS_HEADER_SIZE;
    block->data = malloc(header_size + max_compressed);
    if (!block->data) {
        free(block);
        return NULL;
    }

    /* Compress the data */
    size_t compressed_len = ZSTD_compress(block->data + header_size, max_compressed,
                                          data, len, 1); /* compression level 1 */
    if (ZSTD_isError(compressed_len)) {
        free(block->data);
        free(block);
        return NULL;
    }

    /* Fill header after checksum (16 bytes) */
    uint8_t *header = block->data + 16;
    header[0] = CH_COMPRESS_BYTE_ZSTD;

    /* Compressed size (including header) in little-endian */
    uint32_t total_compressed = (uint32_t)(compressed_len + CH_COMPRESS_HEADER_SIZE);
    memcpy(header + 1, &total_compressed, 4);

    /* Original size in little-endian */
    uint32_t orig_size = (uint32_t)len;
    memcpy(header + 5, &orig_size, 4);

    /* Compute checksum of header + compressed data */
    compute_checksum(header, CH_COMPRESS_HEADER_SIZE + compressed_len, block->data);

    block->size = header_size + compressed_len;
    block->original_size = len;
    block->method = CH_COMPRESS_ZSTD;

    return block;
#else
    (void)data;
    (void)len;
    return NULL;
#endif
}

uint8_t *clickhouse_decompress(const uint8_t *data, size_t compressed_size,
                               size_t original_size, uint8_t method, size_t *out_size) {
    if (!data || compressed_size == 0 || original_size == 0) return NULL;

    uint8_t *output = malloc(original_size);
    if (!output) return NULL;

    switch (method) {
#ifdef HAVE_LZ4
        case CH_COMPRESS_LZ4:
        case CH_COMPRESS_BYTE_LZ4: {
            int result = LZ4_decompress_safe((const char *)data, (char *)output,
                                             (int)compressed_size, (int)original_size);
            if (result < 0 || (size_t)result != original_size) {
                free(output);
                return NULL;
            }
            break;
        }
#endif

#ifdef HAVE_ZSTD
        case CH_COMPRESS_ZSTD:
        case CH_COMPRESS_BYTE_ZSTD: {
            size_t result = ZSTD_decompress(output, original_size, data, compressed_size);
            if (ZSTD_isError(result) || result != original_size) {
                free(output);
                return NULL;
            }
            break;
        }
#endif

        default:
            free(output);
            return NULL;
    }

    if (out_size) {
        *out_size = original_size;
    }

    return output;
}

void clickhouse_compressed_block_free(clickhouse_compressed_block *block) {
    if (block) {
        free(block->data);
        free(block);
    }
}

/* Check if compression method is supported */
int clickhouse_compression_supported(uint8_t method) {
    switch (method) {
        case CH_COMPRESS_NONE:
            return 1;
#ifdef HAVE_LZ4
        case CH_COMPRESS_LZ4:
            return 1;
#endif
#ifdef HAVE_ZSTD
        case CH_COMPRESS_ZSTD:
            return 1;
#endif
        default:
            return 0;
    }
}

/* Check if the buffer contains a compressed block (peek at method byte) */
int clickhouse_is_compressed_block(clickhouse_buffer *buf) {
    if (!buf) return 0;
    size_t available = buf->size - buf->position;
    if (available < 17) return 0; /* Need at least checksum + method byte */

    /* Method byte is at offset 16 (after checksum) */
    uint8_t method = buf->data[buf->position + 16];
    return (method == CH_COMPRESS_BYTE_LZ4 ||
            method == CH_COMPRESS_BYTE_ZSTD ||
            method == CH_COMPRESS_BYTE_NONE);
}

/* Read and decompress a compressed block from buffer */
int clickhouse_read_compressed_block(clickhouse_buffer *buf, clickhouse_buffer **out_buf) {
    if (!buf || !out_buf) return -1;

    size_t saved_pos = buf->position;
    size_t available = buf->size - buf->position;

    /* Need at least 16 bytes checksum + 9 bytes header */
    if (available < 25) {
        return -2; /* Need more data */
    }

    /* Save pointer to checksum */
    const uint8_t *checksum = buf->data + buf->position;
    buf->position += 16;

    /* Save pointer to start of block (method + sizes + data) for checksum verification */
    const uint8_t *block_start = buf->data + buf->position;

    /* Read compression method */
    uint8_t method;
    if (clickhouse_buffer_read_uint8(buf, &method) != 0) {
        buf->position = saved_pos;
        return -2;
    }

    /* Read compressed size (includes 9-byte header) */
    uint32_t compressed_size_with_header;
    if (clickhouse_buffer_read_bytes(buf, (uint8_t *)&compressed_size_with_header, 4) != 0) {
        buf->position = saved_pos;
        return -2;
    }

    /* Read original size */
    uint32_t original_size;
    if (clickhouse_buffer_read_bytes(buf, (uint8_t *)&original_size, 4) != 0) {
        buf->position = saved_pos;
        return -2;
    }

    /* Calculate actual compressed data size */
    if (compressed_size_with_header < CH_COMPRESS_HEADER_SIZE) {
        buf->position = saved_pos;
        return -1; /* Invalid header */
    }
    uint32_t compressed_data_size = compressed_size_with_header - CH_COMPRESS_HEADER_SIZE;

    /* Sanity check - prevent unreasonable sizes (max 1GB like clickhouse-cpp) */
    if (compressed_size_with_header > 0x40000000ULL || original_size > 0x40000000ULL) {
        buf->position = saved_pos;
        return -1; /* Size too large */
    }

    /* Check if we have enough data */
    available = buf->size - buf->position;
    if (available < compressed_data_size) {
        buf->position = saved_pos;
        return -2; /* Need more data */
    }

    /* Verify checksum - covers header + compressed data */
    if (!verify_checksum(checksum, block_start, compressed_size_with_header)) {
        buf->position = saved_pos;
        return -3; /* Checksum mismatch */
    }

    /* Get pointer to compressed data */
    const uint8_t *compressed_data = buf->data + buf->position;
    buf->position += compressed_data_size;

    /* Handle uncompressed case */
    if (method == CH_COMPRESS_BYTE_NONE) {
        *out_buf = clickhouse_buffer_create(original_size);
        if (!*out_buf) return -1;
        memcpy((*out_buf)->data, compressed_data, original_size);
        (*out_buf)->size = original_size;
        return 0;
    }

    /* Decompress */
    size_t decompressed_size;
    uint8_t *decompressed = clickhouse_decompress(compressed_data, compressed_data_size,
                                                   original_size, method, &decompressed_size);
    if (!decompressed) {
        return -1; /* Decompression failed */
    }

    /* Create buffer from decompressed data */
    *out_buf = clickhouse_buffer_create(decompressed_size);
    if (!*out_buf) {
        free(decompressed);
        return -1;
    }
    memcpy((*out_buf)->data, decompressed, decompressed_size);
    (*out_buf)->size = decompressed_size;
    free(decompressed);

    return 0;
}

/* ========================= External Tables ========================= */

clickhouse_external_tables *clickhouse_external_tables_create(void) {
    clickhouse_external_tables *tables = calloc(1, sizeof(clickhouse_external_tables));
    return tables;
}

void clickhouse_external_tables_free(clickhouse_external_tables *tables) {
    if (!tables) return;

    clickhouse_external_table *table = tables->head;
    while (table) {
        clickhouse_external_table *next = table->next;
        clickhouse_external_table_free(table);
        table = next;
    }
    free(tables);
}

clickhouse_external_table *clickhouse_external_table_create(const char *name) {
    if (!name) return NULL;

    clickhouse_external_table *table = calloc(1, sizeof(clickhouse_external_table));
    if (!table) return NULL;

    table->name = strdup(name);
    if (!table->name) {
        free(table);
        return NULL;
    }

    return table;
}

void clickhouse_external_table_free(clickhouse_external_table *table) {
    if (!table) return;

    free(table->name);

    if (table->columns) {
        for (size_t i = 0; i < table->column_count; i++) {
            free(table->columns[i].name);
            free(table->columns[i].type);
            /* Note: data is managed externally (PHP zvals), don't free here */
        }
        free(table->columns);
    }

    free(table);
}

int clickhouse_external_table_add_column(clickhouse_external_table *table, const char *name, const char *type) {
    if (!table || !name || !type) return -1;

    /* Reallocate columns array */
    clickhouse_external_column *new_columns = realloc(table->columns,
        (table->column_count + 1) * sizeof(clickhouse_external_column));
    if (!new_columns) return -1;

    table->columns = new_columns;

    /* Initialize new column */
    clickhouse_external_column *col = &table->columns[table->column_count];
    memset(col, 0, sizeof(clickhouse_external_column));

    col->name = strdup(name);
    col->type = strdup(type);
    if (!col->name || !col->type) {
        free(col->name);
        free(col->type);
        return -1;
    }

    table->column_count++;
    return 0;
}

int clickhouse_external_tables_add(clickhouse_external_tables *tables, clickhouse_external_table *table) {
    if (!tables || !table) return -1;

    /* Add to linked list */
    table->next = tables->head;
    tables->head = table;
    tables->count++;

    return 0;
}
