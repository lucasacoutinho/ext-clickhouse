/*
  +----------------------------------------------------------------------+
  | ClickHouse Native Driver - Connection Implementation                |
  +----------------------------------------------------------------------+
*/

/* Enable POSIX and GNU extensions for strdup, getaddrinfo, etc. */
#define _GNU_SOURCE
#define _POSIX_C_SOURCE 200809L

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

#include "connection.h"
#include <netdb.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <sys/select.h>
#include <sys/time.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>

/* Counter for unique query IDs within the process */
static unsigned long query_id_counter = 0;

/* Generate a unique query ID if none provided */
static char *generate_query_id(void) {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    char *query_id = malloc(64);
    if (query_id) {
        snprintf(query_id, 64, "php-%ld-%ld-%lu",
                 (long)getpid(), (long)tv.tv_sec, ++query_id_counter);
    }
    return query_id;
}

#ifdef HAVE_OPENSSL
#include <openssl/ssl.h>
#include <openssl/err.h>
#include <openssl/x509v3.h>

static int ssl_initialized = 0;

static void clickhouse_ssl_init_library(void) {
    if (!ssl_initialized) {
        SSL_library_init();
        SSL_load_error_strings();
        OpenSSL_add_all_algorithms();
        ssl_initialized = 1;
    }
}

static void clickhouse_ssl_cleanup_connection(clickhouse_connection *conn) {
    if (conn->ssl) {
        SSL_shutdown((SSL *)conn->ssl);
        SSL_free((SSL *)conn->ssl);
        conn->ssl = NULL;
    }
    if (conn->ssl_ctx) {
        SSL_CTX_free((SSL_CTX *)conn->ssl_ctx);
        conn->ssl_ctx = NULL;
    }
}
#endif /* HAVE_OPENSSL */

#define DEFAULT_CONNECT_TIMEOUT 10
#define DEFAULT_READ_TIMEOUT 30
#define DEFAULT_WRITE_TIMEOUT 30
#define READ_BUFFER_SIZE 65536

clickhouse_connection *clickhouse_connection_create(const char *host, uint16_t port,
                                                    const char *user, const char *password,
                                                    const char *database) {
    clickhouse_connection *conn = calloc(1, sizeof(clickhouse_connection));
    if (!conn) {
        return NULL;
    }

    conn->socket_fd = -1;
    conn->host = strdup(host);
    conn->port = port;
    conn->user = strdup(user);
    conn->password = strdup(password);
    conn->database = strdup(database);
    conn->state = CONN_STATE_DISCONNECTED;
    conn->server_info = NULL;
    conn->last_error = NULL;
    conn->connect_timeout = DEFAULT_CONNECT_TIMEOUT;
    conn->read_timeout = DEFAULT_READ_TIMEOUT;
    conn->write_timeout = DEFAULT_WRITE_TIMEOUT;

    conn->read_buf = clickhouse_buffer_create(READ_BUFFER_SIZE);
    conn->write_buf = clickhouse_buffer_create(READ_BUFFER_SIZE);

    if (!conn->host || !conn->user || !conn->password || !conn->database ||
        !conn->read_buf || !conn->write_buf) {
        clickhouse_connection_free(conn);
        return NULL;
    }

    return conn;
}

void clickhouse_connection_free(clickhouse_connection *conn) {
    if (conn) {
#ifdef HAVE_OPENSSL
        clickhouse_ssl_cleanup_connection(conn);
#endif
        if (conn->socket_fd >= 0) {
            close(conn->socket_fd);
        }
        free(conn->host);
        free(conn->user);
        free(conn->password);
        free(conn->database);
        free(conn->last_error);
        free(conn->ssl_opts.ca_cert);
        free(conn->ssl_opts.client_cert);
        free(conn->ssl_opts.client_key);
        clickhouse_server_info_free(conn->server_info);
        clickhouse_buffer_free(conn->read_buf);
        clickhouse_buffer_free(conn->write_buf);
        free(conn);
    }
}

/* SSL availability check */
int clickhouse_ssl_available(void) {
#ifdef HAVE_OPENSSL
    return 1;
#else
    return 0;
#endif
}

/* SSL option setters */
void clickhouse_connection_set_ssl_enabled(clickhouse_connection *conn, int enabled) {
    conn->ssl_opts.enabled = enabled;
}

void clickhouse_connection_set_ssl_verify(clickhouse_connection *conn, int verify_peer, int verify_host) {
    conn->ssl_opts.verify_peer = verify_peer;
    conn->ssl_opts.verify_host = verify_host;
}

void clickhouse_connection_set_ssl_ca_cert(clickhouse_connection *conn, const char *ca_cert) {
    free(conn->ssl_opts.ca_cert);
    conn->ssl_opts.ca_cert = ca_cert ? strdup(ca_cert) : NULL;
}

void clickhouse_connection_set_ssl_client_cert(clickhouse_connection *conn, const char *cert, const char *key) {
    free(conn->ssl_opts.client_cert);
    free(conn->ssl_opts.client_key);
    conn->ssl_opts.client_cert = cert ? strdup(cert) : NULL;
    conn->ssl_opts.client_key = key ? strdup(key) : NULL;
}

#ifdef HAVE_OPENSSL
static int clickhouse_ssl_setup(clickhouse_connection *conn) {
    clickhouse_ssl_init_library();

    /* Create SSL context */
    const SSL_METHOD *method = TLS_client_method();
    conn->ssl_ctx = SSL_CTX_new(method);
    if (!conn->ssl_ctx) {
        clickhouse_connection_set_error(conn, "Failed to create SSL context");
        return -1;
    }

    SSL_CTX *ctx = (SSL_CTX *)conn->ssl_ctx;

    /* Set minimum TLS version */
    SSL_CTX_set_min_proto_version(ctx, TLS1_2_VERSION);

    /* Configure verification */
    if (conn->ssl_opts.verify_peer) {
        SSL_CTX_set_verify(ctx, SSL_VERIFY_PEER, NULL);

        if (conn->ssl_opts.ca_cert) {
            if (SSL_CTX_load_verify_locations(ctx, conn->ssl_opts.ca_cert, NULL) != 1) {
                clickhouse_connection_set_error(conn, "Failed to load CA certificate");
                return -1;
            }
        } else {
            SSL_CTX_set_default_verify_paths(ctx);
        }
    } else {
        SSL_CTX_set_verify(ctx, SSL_VERIFY_NONE, NULL);
    }

    /* Load client certificate if provided */
    if (conn->ssl_opts.client_cert) {
        if (SSL_CTX_use_certificate_file(ctx, conn->ssl_opts.client_cert, SSL_FILETYPE_PEM) != 1) {
            clickhouse_connection_set_error(conn, "Failed to load client certificate");
            return -1;
        }
    }

    if (conn->ssl_opts.client_key) {
        if (SSL_CTX_use_PrivateKey_file(ctx, conn->ssl_opts.client_key, SSL_FILETYPE_PEM) != 1) {
            clickhouse_connection_set_error(conn, "Failed to load client private key");
            return -1;
        }
    }

    /* Create SSL connection */
    conn->ssl = SSL_new(ctx);
    if (!conn->ssl) {
        clickhouse_connection_set_error(conn, "Failed to create SSL connection");
        return -1;
    }

    SSL *ssl = (SSL *)conn->ssl;
    SSL_set_fd(ssl, conn->socket_fd);

    /* Set hostname for SNI and verification */
    SSL_set_tlsext_host_name(ssl, conn->host);

    if (conn->ssl_opts.verify_host) {
        SSL_set_hostflags(ssl, X509_CHECK_FLAG_NO_PARTIAL_WILDCARDS);
        SSL_set1_host(ssl, conn->host);
    }

    /* Perform SSL handshake */
    int result = SSL_connect(ssl);
    if (result != 1) {
        char err_buf[256];
        ERR_error_string_n(ERR_get_error(), err_buf, sizeof(err_buf));
        clickhouse_connection_set_error(conn, err_buf);
        return -1;
    }

    return 0;
}
#endif /* HAVE_OPENSSL */

static int set_socket_timeout(int fd, int timeout_sec, int type) {
    struct timeval tv;
    tv.tv_sec = timeout_sec;
    tv.tv_usec = 0;
    return setsockopt(fd, SOL_SOCKET, type, &tv, sizeof(tv));
}

static int set_socket_timeout_ms(int fd, int timeout_ms, int type) {
    struct timeval tv;
    tv.tv_sec = timeout_ms / 1000;
    tv.tv_usec = (timeout_ms % 1000) * 1000;
    return setsockopt(fd, SOL_SOCKET, type, &tv, sizeof(tv));
}

int clickhouse_connection_connect(clickhouse_connection *conn) {
    struct addrinfo hints, *result, *rp;
    char port_str[6];
    int status;

    snprintf(port_str, sizeof(port_str), "%d", conn->port);

    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;

    status = getaddrinfo(conn->host, port_str, &hints, &result);
    if (status != 0) {
        clickhouse_connection_set_error(conn, gai_strerror(status));
        return -1;
    }

    for (rp = result; rp != NULL; rp = rp->ai_next) {
        conn->socket_fd = socket(rp->ai_family, rp->ai_socktype, rp->ai_protocol);
        if (conn->socket_fd == -1) {
            continue;
        }

        /* Set socket options */
        int flag = 1;
        setsockopt(conn->socket_fd, IPPROTO_TCP, TCP_NODELAY, &flag, sizeof(flag));

        /* Set non-blocking for connect with timeout */
        int flags = fcntl(conn->socket_fd, F_GETFL, 0);
        fcntl(conn->socket_fd, F_SETFL, flags | O_NONBLOCK);

        status = connect(conn->socket_fd, rp->ai_addr, rp->ai_addrlen);
        if (status == -1 && errno != EINPROGRESS) {
            close(conn->socket_fd);
            conn->socket_fd = -1;
            continue;
        }

        if (status == -1) {
            /* Wait for connection with timeout */
            fd_set write_fds;
            struct timeval tv;

            FD_ZERO(&write_fds);
            FD_SET(conn->socket_fd, &write_fds);
            tv.tv_sec = conn->connect_timeout;
            tv.tv_usec = 0;

            status = select(conn->socket_fd + 1, NULL, &write_fds, NULL, &tv);
            if (status <= 0) {
                close(conn->socket_fd);
                conn->socket_fd = -1;
                continue;
            }

            /* Check for socket errors */
            int error;
            socklen_t len = sizeof(error);
            if (getsockopt(conn->socket_fd, SOL_SOCKET, SO_ERROR, &error, &len) < 0 || error != 0) {
                close(conn->socket_fd);
                conn->socket_fd = -1;
                continue;
            }
        }

        /* Set back to blocking mode */
        fcntl(conn->socket_fd, F_SETFL, flags);

        /* Set read/write timeouts */
        set_socket_timeout(conn->socket_fd, conn->read_timeout, SO_RCVTIMEO);
        set_socket_timeout(conn->socket_fd, conn->write_timeout, SO_SNDTIMEO);

        break;
    }

    freeaddrinfo(result);

    if (conn->socket_fd == -1) {
        clickhouse_connection_set_error(conn, "Failed to connect to server");
        return -1;
    }

    conn->state = CONN_STATE_CONNECTED;

#ifdef HAVE_OPENSSL
    /* Setup SSL if enabled */
    if (conn->ssl_opts.enabled) {
        if (clickhouse_ssl_setup(conn) != 0) {
            return -1;
        }
    }
#else
    if (conn->ssl_opts.enabled) {
        clickhouse_connection_set_error(conn, "SSL support not compiled in");
        return -1;
    }
#endif

    /* Send hello packet */
    clickhouse_buffer_reset(conn->write_buf);
    if (clickhouse_write_hello(conn->write_buf, conn->database, conn->user, conn->password) != 0) {
        clickhouse_connection_set_error(conn, "Failed to build hello packet");
        return -1;
    }

    if (clickhouse_connection_send(conn) != 0) {
        return -1;
    }

    /* Receive hello response */
    if (clickhouse_connection_receive(conn) != 0) {
        return -1;
    }

    conn->server_info = clickhouse_server_info_create();
    if (!conn->server_info) {
        clickhouse_connection_set_error(conn, "Failed to allocate server info");
        return -1;
    }

    status = clickhouse_read_hello(conn->read_buf, conn->server_info);
    if (status == -2) {
        /* Server exception */
        clickhouse_exception *ex = clickhouse_exception_read(conn->read_buf);
        if (ex) {
            clickhouse_connection_set_error(conn, ex->message);
            clickhouse_exception_free(ex);
        } else {
            clickhouse_connection_set_error(conn, "Server returned exception");
        }
        return -1;
    } else if (status != 0) {
        clickhouse_connection_set_error(conn, "Failed to read hello response");
        return -1;
    }

    /* Send addendum (empty quota key) if OUR claimed revision supports it (>= 54458) */
    /* The server expects the addendum based on what WE claimed, not what IT supports */
    if (CLICKHOUSE_REVISION >= 54458) {
        clickhouse_buffer_reset(conn->write_buf);
        if (clickhouse_buffer_write_string(conn->write_buf, "", 0) != 0) {
            clickhouse_connection_set_error(conn, "Failed to write addendum");
            return -1;
        }
        if (clickhouse_connection_send(conn) != 0) {
            return -1;
        }
    }

    conn->state = CONN_STATE_AUTHENTICATED;
    return 0;
}

void clickhouse_connection_close(clickhouse_connection *conn) {
    if (conn) {
#ifdef HAVE_OPENSSL
        clickhouse_ssl_cleanup_connection(conn);
#endif
        if (conn->socket_fd >= 0) {
            close(conn->socket_fd);
            conn->socket_fd = -1;
        }
        conn->state = CONN_STATE_DISCONNECTED;
    }
}

int clickhouse_connection_ping(clickhouse_connection *conn) {
    if (conn->state != CONN_STATE_AUTHENTICATED) {
        clickhouse_connection_set_error(conn, "Not connected");
        return -1;
    }

    clickhouse_buffer_reset(conn->write_buf);
    if (clickhouse_write_ping(conn->write_buf) != 0) {
        return -1;
    }

    if (clickhouse_connection_send(conn) != 0) {
        return -1;
    }

    if (clickhouse_connection_receive(conn) != 0) {
        return -1;
    }

    uint64_t packet_type;
    if (clickhouse_buffer_read_varint(conn->read_buf, &packet_type) != 0) {
        return -1;
    }

    return (packet_type == CH_SERVER_PONG) ? 0 : -1;
}

int clickhouse_connection_send(clickhouse_connection *conn) {
    if (conn->socket_fd < 0) {
        clickhouse_connection_set_error(conn, "Not connected");
        return -1;
    }

    size_t total_sent = 0;
    size_t to_send = conn->write_buf->size;

    while (total_sent < to_send) {
        ssize_t sent;

#ifdef HAVE_OPENSSL
        if (conn->ssl) {
            sent = SSL_write((SSL *)conn->ssl, conn->write_buf->data + total_sent,
                            (int)(to_send - total_sent));
            if (sent <= 0) {
                int err = SSL_get_error((SSL *)conn->ssl, (int)sent);
                if (err == SSL_ERROR_WANT_WRITE || err == SSL_ERROR_WANT_READ) {
                    continue;
                }
                char err_buf[256];
                ERR_error_string_n(ERR_get_error(), err_buf, sizeof(err_buf));
                clickhouse_connection_set_error(conn, err_buf);
                return -1;
            }
        } else
#endif
        {
            sent = send(conn->socket_fd, conn->write_buf->data + total_sent,
                       to_send - total_sent, 0);
            if (sent <= 0) {
                if (errno == EINTR) {
                    continue;
                }
                clickhouse_connection_set_error(conn, strerror(errno));
                return -1;
            }
        }
        total_sent += sent;
    }

    return 0;
}

int clickhouse_connection_receive(clickhouse_connection *conn) {
    if (conn->socket_fd < 0) {
        clickhouse_connection_set_error(conn, "Not connected");
        return -1;
    }

    clickhouse_buffer_reset(conn->read_buf);

    uint8_t temp_buf[READ_BUFFER_SIZE];
    ssize_t received;

#ifdef HAVE_OPENSSL
    if (conn->ssl) {
        received = SSL_read((SSL *)conn->ssl, temp_buf, sizeof(temp_buf));
        if (received <= 0) {
            int err = SSL_get_error((SSL *)conn->ssl, (int)received);
            if (err == SSL_ERROR_WANT_READ || err == SSL_ERROR_WANT_WRITE) {
                return clickhouse_connection_receive(conn);
            }
            if (err == SSL_ERROR_ZERO_RETURN) {
                clickhouse_connection_set_error(conn, "Connection closed by server");
                return -1;
            }
            char err_buf[256];
            ERR_error_string_n(ERR_get_error(), err_buf, sizeof(err_buf));
            clickhouse_connection_set_error(conn, err_buf);
            return -1;
        }
    } else
#endif
    {
        received = recv(conn->socket_fd, temp_buf, sizeof(temp_buf), 0);

        if (received < 0) {
            if (errno == EINTR) {
                return clickhouse_connection_receive(conn);
            }
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                /* Retry with a short wait */
                struct timeval tv;
                fd_set read_fds;
                FD_ZERO(&read_fds);
                FD_SET(conn->socket_fd, &read_fds);
                tv.tv_sec = conn->read_timeout > 0 ? conn->read_timeout : 30;
                tv.tv_usec = 0;
                int sel = select(conn->socket_fd + 1, &read_fds, NULL, NULL, &tv);
                if (sel > 0) {
                    return clickhouse_connection_receive(conn);
                }
                clickhouse_connection_set_error(conn, "Read timeout");
                return -1;
            }
            clickhouse_connection_set_error(conn, strerror(errno));
            return -1;
        }

        if (received == 0) {
            clickhouse_connection_set_error(conn, "Connection closed by server");
            return -1;
        }
    }

    if (clickhouse_buffer_write_bytes(conn->read_buf, temp_buf, received) != 0) {
        clickhouse_connection_set_error(conn, "Buffer overflow");
        return -1;
    }

    return 0;
}

/* Receive more data, compacting unread data and appending new data */
static int clickhouse_connection_receive_more(clickhouse_connection *conn) {
    if (conn->state == CONN_STATE_DISCONNECTED) {
        clickhouse_connection_set_error(conn, "Not connected");
        return -1;
    }

    if (conn->socket_fd < 0) {
        clickhouse_connection_set_error(conn, "Not connected");
        return -1;
    }

    /* Compact buffer to move unread data to beginning */
    clickhouse_buffer_compact(conn->read_buf);

    uint8_t temp_buf[READ_BUFFER_SIZE];
    ssize_t received;

#ifdef HAVE_OPENSSL
    if (conn->ssl) {
        received = SSL_read((SSL *)conn->ssl, temp_buf, sizeof(temp_buf));
        if (received <= 0) {
            int err = SSL_get_error((SSL *)conn->ssl, (int)received);
            if (err == SSL_ERROR_WANT_READ || err == SSL_ERROR_WANT_WRITE) {
                return clickhouse_connection_receive_more(conn);
            }
            if (err == SSL_ERROR_ZERO_RETURN) {
                clickhouse_connection_set_error(conn, "Connection closed by server");
                return -1;
            }
            char err_buf[256];
            ERR_error_string_n(ERR_get_error(), err_buf, sizeof(err_buf));
            clickhouse_connection_set_error(conn, err_buf);
            return -1;
        }
    } else
#endif
    {
        received = recv(conn->socket_fd, temp_buf, sizeof(temp_buf), 0);

        if (received < 0) {
            if (errno == EINTR) {
                return clickhouse_connection_receive_more(conn);
            }
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                /* Retry with a short wait */
                struct timeval tv;
                fd_set read_fds;
                FD_ZERO(&read_fds);
                FD_SET(conn->socket_fd, &read_fds);
                tv.tv_sec = conn->read_timeout > 0 ? conn->read_timeout : 30;
                tv.tv_usec = 0;
                int sel = select(conn->socket_fd + 1, &read_fds, NULL, NULL, &tv);
                if (sel > 0) {
                    return clickhouse_connection_receive_more(conn);
                }
                clickhouse_connection_set_error(conn, "Read timeout");
                return -1;
            }
            clickhouse_connection_set_error(conn, strerror(errno));
            return -1;
        }

        if (received == 0) {
            clickhouse_connection_set_error(conn, "Connection closed by server");
            return -1;
        }
    }

    if (clickhouse_buffer_write_bytes(conn->read_buf, temp_buf, received) != 0) {
        clickhouse_connection_set_error(conn, "Buffer overflow");
        return -1;
    }

    return 0;
}

int clickhouse_connection_read_packet_type(clickhouse_connection *conn, uint64_t *packet_type) {
    return clickhouse_buffer_read_varint(conn->read_buf, packet_type);
}

const char *clickhouse_connection_get_error(const clickhouse_connection *conn) {
    return conn->last_error;
}

void clickhouse_connection_set_error(clickhouse_connection *conn, const char *error) {
    free(conn->last_error);
    conn->last_error = error ? strdup(error) : NULL;
}

void clickhouse_connection_set_connect_timeout(clickhouse_connection *conn, int seconds) {
    conn->connect_timeout = seconds;
}

void clickhouse_connection_set_read_timeout(clickhouse_connection *conn, int seconds) {
    conn->read_timeout = seconds;
    if (conn->socket_fd >= 0) {
        set_socket_timeout(conn->socket_fd, seconds, SO_RCVTIMEO);
    }
}

void clickhouse_connection_set_write_timeout(clickhouse_connection *conn, int seconds) {
    conn->write_timeout = seconds;
    if (conn->socket_fd >= 0) {
        set_socket_timeout(conn->socket_fd, seconds, SO_SNDTIMEO);
    }
}

void clickhouse_connection_set_query_timeout_ms(clickhouse_connection *conn, int timeout_ms) {
    if (conn->socket_fd >= 0) {
        if (timeout_ms > 0) {
            set_socket_timeout_ms(conn->socket_fd, timeout_ms, SO_RCVTIMEO);
            set_socket_timeout_ms(conn->socket_fd, timeout_ms, SO_SNDTIMEO);
        } else {
            /* Reset to default timeouts (in seconds) */
            set_socket_timeout(conn->socket_fd, conn->read_timeout, SO_RCVTIMEO);
            set_socket_timeout(conn->socket_fd, conn->write_timeout, SO_SNDTIMEO);
        }
    }
}

/* Result functions */

clickhouse_result *clickhouse_result_create(void) {
    clickhouse_result *result = calloc(1, sizeof(clickhouse_result));
    if (!result) return NULL;

    result->block_capacity = 8;
    result->blocks = calloc(result->block_capacity, sizeof(clickhouse_block *));
    if (!result->blocks) {
        free(result);
        return NULL;
    }

    return result;
}

void clickhouse_result_free(clickhouse_result *result) {
    if (result) {
        for (size_t i = 0; i < result->block_count; i++) {
            clickhouse_block_free(result->blocks[i]);
        }
        free(result->blocks);
        clickhouse_block_free(result->totals);
        clickhouse_block_free(result->extremes);
        clickhouse_exception_free(result->exception);
        free(result->query_id);
        free(result);
    }
}

int clickhouse_result_add_block(clickhouse_result *result, clickhouse_block *block) {
    if (result->block_count >= result->block_capacity) {
        size_t new_capacity = result->block_capacity * 2;
        clickhouse_block **new_blocks = realloc(result->blocks,
                                                 new_capacity * sizeof(clickhouse_block *));
        if (!new_blocks) return -1;
        result->blocks = new_blocks;
        result->block_capacity = new_capacity;
    }

    result->blocks[result->block_count++] = block;
    return 0;
}


/* Send empty data block (signals end of data) */
int clickhouse_connection_send_empty_block(clickhouse_connection *conn) {
    clickhouse_buffer_reset(conn->write_buf);

    /* Data packet type */
    if (clickhouse_buffer_write_varint(conn->write_buf, CH_CLIENT_DATA) != 0) return -1;

    /* Empty table name */
    if (clickhouse_buffer_write_string(conn->write_buf, "", 0) != 0) return -1;

    /* Check if compression is enabled */
    if (conn->compression != CH_COMPRESS_NONE) {
        /* Build block data to a temporary buffer */
        clickhouse_buffer *block_buf = clickhouse_buffer_create(256);
        if (!block_buf) return -1;

        /* Block info */
        if (clickhouse_buffer_write_varint(block_buf, 1) != 0) goto compress_error;
        if (clickhouse_buffer_write_uint8(block_buf, 0) != 0) goto compress_error;
        if (clickhouse_buffer_write_varint(block_buf, 2) != 0) goto compress_error;
        if (clickhouse_buffer_write_int32(block_buf, -1) != 0) goto compress_error;
        if (clickhouse_buffer_write_varint(block_buf, 0) != 0) goto compress_error;

        /* 0 columns, 0 rows */
        if (clickhouse_buffer_write_varint(block_buf, 0) != 0) goto compress_error;
        if (clickhouse_buffer_write_varint(block_buf, 0) != 0) goto compress_error;

        /* Compress the block data */
        clickhouse_compressed_block *compressed = NULL;
        if (conn->compression == CH_COMPRESS_LZ4) {
            compressed = clickhouse_compress_lz4(block_buf->data, block_buf->size);
        } else if (conn->compression == CH_COMPRESS_ZSTD) {
            compressed = clickhouse_compress_zstd(block_buf->data, block_buf->size);
        }

        clickhouse_buffer_free(block_buf);

        if (!compressed) {
            clickhouse_connection_set_error(conn, "Failed to compress block");
            return -1;
        }

        /* Write compressed data */
        if (clickhouse_buffer_write_bytes(conn->write_buf, compressed->data, compressed->size) != 0) {
            clickhouse_compressed_block_free(compressed);
            return -1;
        }

        clickhouse_compressed_block_free(compressed);
        return clickhouse_connection_send(conn);

compress_error:
        clickhouse_buffer_free(block_buf);
        return -1;
    }

    /* No compression - write directly */
    /* Block info */
    if (clickhouse_buffer_write_varint(conn->write_buf, 1) != 0) return -1;  /* field num: is_overflows */
    if (clickhouse_buffer_write_uint8(conn->write_buf, 0) != 0) return -1;   /* is_overflows = false */
    if (clickhouse_buffer_write_varint(conn->write_buf, 2) != 0) return -1;  /* field num: bucket_num */
    if (clickhouse_buffer_write_int32(conn->write_buf, -1) != 0) return -1;  /* bucket_num = -1 */
    if (clickhouse_buffer_write_varint(conn->write_buf, 0) != 0) return -1;  /* end of block info */

    /* 0 columns, 0 rows */
    if (clickhouse_buffer_write_varint(conn->write_buf, 0) != 0) return -1;
    if (clickhouse_buffer_write_varint(conn->write_buf, 0) != 0) return -1;

    return clickhouse_connection_send(conn);
}

/* Send data block */
int clickhouse_connection_send_data(clickhouse_connection *conn, clickhouse_block *block) {
    clickhouse_buffer_reset(conn->write_buf);

    /* Data packet type */
    if (clickhouse_buffer_write_varint(conn->write_buf, CH_CLIENT_DATA) != 0) return -1;

    /* Empty table name */
    if (clickhouse_buffer_write_string(conn->write_buf, "", 0) != 0) return -1;

    /* Check if compression is enabled */
    if (conn->compression != CH_COMPRESS_NONE) {
        /* Build block data to a temporary buffer */
        clickhouse_buffer *block_buf = clickhouse_buffer_create(4096);
        if (!block_buf) return -1;

        /* Write block to temporary buffer */
        if (clickhouse_block_write(block_buf, block) != 0) {
            clickhouse_buffer_free(block_buf);
            return -1;
        }

        /* Compress the block data */
        clickhouse_compressed_block *compressed = NULL;
        if (conn->compression == CH_COMPRESS_LZ4) {
            compressed = clickhouse_compress_lz4(block_buf->data, block_buf->size);
        } else if (conn->compression == CH_COMPRESS_ZSTD) {
            compressed = clickhouse_compress_zstd(block_buf->data, block_buf->size);
        }

        clickhouse_buffer_free(block_buf);

        if (!compressed) {
            clickhouse_connection_set_error(conn, "Failed to compress data block");
            return -1;
        }

        /* Write compressed data */
        if (clickhouse_buffer_write_bytes(conn->write_buf, compressed->data, compressed->size) != 0) {
            clickhouse_compressed_block_free(compressed);
            return -1;
        }

        clickhouse_compressed_block_free(compressed);
        return clickhouse_connection_send(conn);
    }

    /* No compression - write block directly */
    if (clickhouse_block_write(conn->write_buf, block) != 0) return -1;

    return clickhouse_connection_send(conn);
}

/* Send data block with custom table name (for external tables) */
int clickhouse_connection_send_data_named(clickhouse_connection *conn, clickhouse_block *block, const char *table_name) {
    clickhouse_buffer_reset(conn->write_buf);

    /* Data packet type */
    if (clickhouse_buffer_write_varint(conn->write_buf, CH_CLIENT_DATA) != 0) return -1;

    /* Table name */
    if (clickhouse_buffer_write_string(conn->write_buf, table_name, strlen(table_name)) != 0) return -1;

    /* Check if compression is enabled */
    if (conn->compression != CH_COMPRESS_NONE) {
        /* Build block data to a temporary buffer */
        clickhouse_buffer *block_buf = clickhouse_buffer_create(4096);
        if (!block_buf) return -1;

        /* Write block to temporary buffer */
        if (clickhouse_block_write(block_buf, block) != 0) {
            clickhouse_buffer_free(block_buf);
            return -1;
        }

        /* Compress the block data */
        clickhouse_compressed_block *compressed = NULL;
        if (conn->compression == CH_COMPRESS_LZ4) {
            compressed = clickhouse_compress_lz4(block_buf->data, block_buf->size);
        } else if (conn->compression == CH_COMPRESS_ZSTD) {
            compressed = clickhouse_compress_zstd(block_buf->data, block_buf->size);
        }

        clickhouse_buffer_free(block_buf);

        if (!compressed) {
            clickhouse_connection_set_error(conn, "Failed to compress external data block");
            return -1;
        }

        /* Write compressed data */
        if (clickhouse_buffer_write_bytes(conn->write_buf, compressed->data, compressed->size) != 0) {
            clickhouse_compressed_block_free(compressed);
            return -1;
        }

        clickhouse_compressed_block_free(compressed);
        return clickhouse_connection_send(conn);
    }

    /* No compression - write block directly */
    if (clickhouse_block_write(conn->write_buf, block) != 0) return -1;

    return clickhouse_connection_send(conn);
}

/* Send external tables */
int clickhouse_connection_send_external_tables(clickhouse_connection *conn, clickhouse_external_tables *tables) {
    if (!conn || !tables) return -1;

    clickhouse_external_table *table = tables->head;
    while (table) {
        if (table->row_count > 0 && table->column_count > 0) {
            /* Create a block for this external table */
            clickhouse_block *block = clickhouse_block_create();
            if (!block) {
                clickhouse_connection_set_error(conn, "Failed to create external table block");
                return -1;
            }

            block->row_count = table->row_count;

            /* Allocate columns array */
            block->columns = calloc(table->column_count, sizeof(clickhouse_column *));
            if (!block->columns) {
                clickhouse_block_free(block);
                clickhouse_connection_set_error(conn, "Failed to allocate external columns");
                return -1;
            }
            block->column_count = table->column_count;

            /* Create columns from external table definition */
            for (size_t i = 0; i < table->column_count; i++) {
                clickhouse_external_column *ext_col = &table->columns[i];

                /* Parse type first */
                clickhouse_type_info *type = clickhouse_type_parse(ext_col->type);
                if (!type) {
                    clickhouse_block_free(block);
                    clickhouse_connection_set_error(conn, "Failed to parse external column type");
                    return -1;
                }

                /* Create column with parsed type */
                clickhouse_column *col = clickhouse_column_create(ext_col->name, type);
                if (!col) {
                    clickhouse_type_free(type);
                    clickhouse_block_free(block);
                    clickhouse_connection_set_error(conn, "Failed to create external column");
                    return -1;
                }

                /* The data pointer is set from PHP - it should point to the actual data */
                col->data = ext_col->data;
                col->row_count = ext_col->row_count;

                block->columns[i] = col;
            }

            /* Send block with table name */
            int send_result = clickhouse_connection_send_data_named(conn, block, table->name);

            /* Clear data pointers before freeing (data owned by PHP) */
            for (size_t i = 0; i < block->column_count; i++) {
                if (block->columns[i]) {
                    block->columns[i]->data = NULL;
                }
            }
            clickhouse_block_free(block);

            if (send_result != 0) {
                return -1;
            }
        }

        table = table->next;
    }

    return 0;
}

/* Query options management */

clickhouse_query_options *clickhouse_query_options_create(void) {
    clickhouse_query_options *opts = calloc(1, sizeof(clickhouse_query_options));
    if (!opts) return NULL;

    opts->stage = CH_STAGE_COMPLETE;
    opts->compression = CH_COMPRESS_NONE;

    return opts;
}

void clickhouse_query_options_free(clickhouse_query_options *opts) {
    if (opts) {
        if (opts->settings) {
            clickhouse_settings_free(opts->settings);
        }
        if (opts->params) {
            clickhouse_params_free(opts->params);
        }
        if (opts->external_tables) {
            clickhouse_external_tables_free(opts->external_tables);
        }
        free(opts->query_id);
        free(opts->session_id);
        free(opts);
    }
}

int clickhouse_query_options_set_setting(clickhouse_query_options *opts, const char *name, const char *value) {
    if (!opts) return -1;

    if (!opts->settings) {
        opts->settings = clickhouse_settings_create();
        if (!opts->settings) return -1;
    }

    /* Use flags=1 for "custom" setting (user-defined) in ClickHouse protocol */
    return clickhouse_settings_add(opts->settings, name, value, 1);
}

int clickhouse_query_options_set_param(clickhouse_query_options *opts, const char *name, const char *value, const char *type) {
    if (!opts) return -1;

    if (!opts->params) {
        opts->params = clickhouse_params_create();
        if (!opts->params) return -1;
    }

    return clickhouse_params_add(opts->params, name, value, type);
}

/* Cancel a running query */
int clickhouse_connection_cancel(clickhouse_connection *conn) {
    if (conn->state != CONN_STATE_AUTHENTICATED) {
        clickhouse_connection_set_error(conn, "Not connected");
        return -1;
    }

    clickhouse_buffer_reset(conn->write_buf);
    if (clickhouse_write_cancel(conn->write_buf) != 0) {
        clickhouse_connection_set_error(conn, "Failed to build cancel packet");
        return -1;
    }

    return clickhouse_connection_send(conn);
}

/* Internal: execute query with full options */
static int connection_execute_query_internal(clickhouse_connection *conn, const char *query,
                                              clickhouse_query_options *options,
                                              clickhouse_result **result_out) {
    if (conn->state != CONN_STATE_AUTHENTICATED) {
        clickhouse_connection_set_error(conn, "Not connected");
        return -1;
    }

    /* Create client info */
    clickhouse_client_info *client_info = clickhouse_client_info_create();
    if (!client_info) {
        clickhouse_connection_set_error(conn, "Failed to create client info");
        return -1;
    }

    /* Determine query settings */
    uint8_t stage = options ? options->stage : CH_STAGE_COMPLETE;
    uint8_t compression = options ? options->compression : CH_COMPRESS_NONE;

    /* Store compression method in connection for use by send_empty_block and send_data */
    conn->compression = compression;

    /* Build query packet */
    clickhouse_buffer_reset(conn->write_buf);

    int write_result;
    /* Use min of server and client revision for protocol negotiation */
    uint64_t server_rev = conn->server_info ? conn->server_info->revision : CLICKHOUSE_REVISION;
    uint64_t protocol_revision = (server_rev < CLICKHOUSE_REVISION) ? server_rev : CLICKHOUSE_REVISION;
    /* Get query_id from options if provided, or generate one */
    const char *query_id = (options && options->query_id) ? options->query_id : NULL;
    char *generated_query_id = NULL;
    if (!query_id || !*query_id) {
        generated_query_id = generate_query_id();
        query_id = generated_query_id ? generated_query_id : "";
    }
    if (options && (options->settings || options->params)) {
        /* Use extended query with settings/params */
        write_result = clickhouse_write_query_ext(conn->write_buf, query_id, client_info, query,
                                                   options->settings, options->params,
                                                   stage, compression, protocol_revision);
    } else {
        /* Use simple query */
        write_result = clickhouse_write_query(conn->write_buf, query_id, client_info, query,
                                              stage, compression, protocol_revision);
    }

    if (write_result != 0) {
        clickhouse_client_info_free(client_info);
        clickhouse_connection_set_error(conn, "Failed to build query packet");
        if (generated_query_id) free(generated_query_id);
        return -1;
    }
    clickhouse_client_info_free(client_info);

    /* Send query */
    if (clickhouse_connection_send(conn) != 0) {
        if (generated_query_id) free(generated_query_id);
        return -1;
    }

    /* Send external tables if provided */
    if (options && options->external_tables) {
        if (clickhouse_connection_send_external_tables(conn, options->external_tables) != 0) {
            if (generated_query_id) free(generated_query_id);
            return -1;
        }
    }

    /* Send empty data block to signal end of external data */
    if (clickhouse_connection_send_empty_block(conn) != 0) {
        if (generated_query_id) free(generated_query_id);
        return -1;
    }

    /* Create result */
    clickhouse_result *result = clickhouse_result_create();
    if (!result) {
        clickhouse_connection_set_error(conn, "Failed to create result");
        if (generated_query_id) free(generated_query_id);
        return -1;
    }

    /* Store query_id in result for tracking */
    if (query_id && *query_id) {
        result->query_id = strdup(query_id);
    }
    /* Free the generated query_id since it's been copied to result */
    if (generated_query_id) free(generated_query_id);

    /* Read response packets */
    int done = 0;
    int first_receive = 1;  /* Track if this is the first receive */
    while (!done) {
        /* Receive data - first time reset buffer, subsequent times append */
        if (first_receive) {
            if (clickhouse_connection_receive(conn) != 0) {
                clickhouse_result_free(result);
                return -1;
            }
            first_receive = 0;
        } else {
            if (clickhouse_connection_receive_more(conn) != 0) {
                clickhouse_result_free(result);
                return -1;
            }
        }

        /* Process all packets in buffer */
        while (clickhouse_buffer_remaining(conn->read_buf) > 0) {
            size_t saved_pos = conn->read_buf->position;
            uint64_t packet_type;

            if (clickhouse_buffer_read_varint(conn->read_buf, &packet_type) != 0) {
                conn->read_buf->position = saved_pos;
                break;
            }

            switch (packet_type) {
                case CH_SERVER_DATA:
                case CH_SERVER_TOTALS:
                case CH_SERVER_EXTREMES: {
                    /* Read table name */
                    char *table_name;
                    size_t table_name_len;
                    if (clickhouse_buffer_read_string(conn->read_buf, &table_name, &table_name_len) != 0) {
                        conn->read_buf->position = saved_pos;
                        goto need_more_data;
                    }
                    free(table_name);

                    /* Read block - handle compression if enabled */
                    clickhouse_block *block = clickhouse_block_create();
                    if (!block) {
                        clickhouse_result_free(result);
                        return -1;
                    }

                    /* Check if server actually sent compressed data */
                    if (compression != CH_COMPRESS_NONE && clickhouse_is_compressed_block(conn->read_buf)) {
                        /* Decompress block data first */
                        clickhouse_buffer *decompressed_buf = NULL;
                        int decomp_result = clickhouse_read_compressed_block(conn->read_buf, &decompressed_buf);
                        if (decomp_result == -2) {
                            /* Need more data */
                            clickhouse_block_free(block);
                            conn->read_buf->position = saved_pos;
                            goto need_more_data;
                        } else if (decomp_result != 0 || !decompressed_buf) {
                            clickhouse_block_free(block);
                            clickhouse_result_free(result);
                            clickhouse_connection_set_error(conn, "Failed to decompress block");
                            return -1;
                        }

                        /* Read block from decompressed buffer */
                        if (clickhouse_block_read(decompressed_buf, block) != 0) {
                            clickhouse_buffer_free(decompressed_buf);
                            clickhouse_block_free(block);
                            clickhouse_result_free(result);
                            clickhouse_connection_set_error(conn, "Failed to parse decompressed block");
                            return -1;
                        }
                        clickhouse_buffer_free(decompressed_buf);
                    } else {
                        /* No compression or server sent uncompressed - read directly */
                        if (clickhouse_block_read(conn->read_buf, block) != 0) {
                            clickhouse_block_free(block);
                            conn->read_buf->position = saved_pos;
                            goto need_more_data;
                        }
                    }

                    /* Store block based on packet type */
                    if (block->row_count > 0) {
                        if (packet_type == CH_SERVER_TOTALS) {
                            /* Free any existing totals block and store new one */
                            clickhouse_block_free(result->totals);
                            result->totals = block;
                        } else if (packet_type == CH_SERVER_EXTREMES) {
                            /* Free any existing extremes block and store new one */
                            clickhouse_block_free(result->extremes);
                            result->extremes = block;
                        } else {
                            /* Regular data block */
                            clickhouse_result_add_block(result, block);
                        }
                    } else {
                        clickhouse_block_free(block);
                    }
                    break;
                }

                case CH_SERVER_EXCEPTION: {
                    result->exception = clickhouse_exception_read(conn->read_buf);
                    if (result->exception) {
                        clickhouse_connection_set_error(conn, result->exception->message);
                    }
                    done = 1;
                    break;
                }

                case CH_SERVER_PROGRESS: {
                    clickhouse_progress_read(conn->read_buf, &result->progress);
                    /* Call progress callback if provided */
                    if (options && options->progress_callback) {
                        options->progress_callback(&result->progress, options->progress_user_data);
                    }
                    break;
                }

                case CH_SERVER_PROFILE_INFO: {
                    clickhouse_profile_info_read(conn->read_buf, &result->profile);
                    break;
                }

                case CH_SERVER_END_OF_STREAM: {
                    done = 1;
                    break;
                }

                case CH_SERVER_LOG:
                case CH_SERVER_TABLE_COLUMNS: {
                    /* Skip these by reading and discarding the table name and block */
                    char *dummy;
                    size_t dummy_len;
                    if (clickhouse_buffer_read_string(conn->read_buf, &dummy, &dummy_len) == 0) {
                        free(dummy);
                        clickhouse_block *dummy_block = clickhouse_block_create();
                        if (dummy_block) {
                            clickhouse_block_read(conn->read_buf, dummy_block);
                            clickhouse_block_free(dummy_block);
                        }
                    }
                    break;
                }

                default:
                    /* Unknown packet type */
                    clickhouse_connection_set_error(conn, "Unknown packet type from server");
                    clickhouse_result_free(result);
                    return -1;
            }
        }
        continue;

need_more_data:
        if (done) break;
        /* Need more data - continue receiving */
    }

    if (result->exception) {
        *result_out = result;
        return -1;
    }

    *result_out = result;
    return 0;
}

/* Public query execution - simple version */
int clickhouse_connection_execute_query(clickhouse_connection *conn, const char *query,
                                        clickhouse_result **result_out) {
    return connection_execute_query_internal(conn, query, NULL, result_out);
}

/* Public query execution - with options (settings, params, callbacks) */
int clickhouse_connection_execute_query_ext(clickhouse_connection *conn, const char *query,
                                            clickhouse_query_options *options,
                                            clickhouse_result **result_out) {
    return connection_execute_query_internal(conn, query, options, result_out);
}

/* ========================= Async Query Support ========================= */

clickhouse_async_query *clickhouse_async_query_create(void) {
    clickhouse_async_query *async = calloc(1, sizeof(clickhouse_async_query));
    if (!async) return NULL;

    async->state = ASYNC_STATE_IDLE;
    async->result = clickhouse_result_create();
    if (!async->result) {
        free(async);
        return NULL;
    }

    return async;
}

void clickhouse_async_query_free(clickhouse_async_query *async) {
    if (async) {
        if (async->result) {
            clickhouse_result_free(async->result);
        }
        if (async->options) {
            clickhouse_query_options_free(async->options);
        }
        free(async->error);
        free(async);
    }
}

/* Check if socket has data ready to read */
int clickhouse_connection_has_data(clickhouse_connection *conn, int timeout_ms) {
    if (conn->socket_fd < 0) return -1;

    fd_set read_fds;
    FD_ZERO(&read_fds);
    FD_SET(conn->socket_fd, &read_fds);

    struct timeval tv;
    tv.tv_sec = timeout_ms / 1000;
    tv.tv_usec = (timeout_ms % 1000) * 1000;

    int result = select(conn->socket_fd + 1, &read_fds, NULL, NULL, &tv);
    if (result < 0) {
        return -1;
    }

    return (result > 0 && FD_ISSET(conn->socket_fd, &read_fds)) ? 1 : 0;
}

/* Start async query - sends query and returns immediately */
int clickhouse_connection_query_async(clickhouse_connection *conn, const char *query,
                                       clickhouse_query_options *options,
                                       clickhouse_async_query **async_out) {
    if (conn->state != CONN_STATE_AUTHENTICATED) {
        clickhouse_connection_set_error(conn, "Not connected");
        return -1;
    }

    clickhouse_async_query *async = clickhouse_async_query_create();
    if (!async) {
        clickhouse_connection_set_error(conn, "Failed to create async query");
        return -1;
    }

    /* Copy options if provided */
    if (options) {
        async->options = clickhouse_query_options_create();
        if (async->options && options->settings) {
            async->options->settings = options->settings;
            options->settings = NULL; /* Transfer ownership */
        }
        if (async->options && options->params) {
            async->options->params = options->params;
            options->params = NULL; /* Transfer ownership */
        }
        if (async->options) {
            async->options->stage = options->stage;
            async->options->compression = options->compression;
            async->options->progress_callback = options->progress_callback;
            async->options->progress_user_data = options->progress_user_data;
        }
    }

    /* Create client info */
    clickhouse_client_info *client_info = clickhouse_client_info_create();
    if (!client_info) {
        clickhouse_async_query_free(async);
        clickhouse_connection_set_error(conn, "Failed to create client info");
        return -1;
    }

    /* Build query packet */
    clickhouse_buffer_reset(conn->write_buf);

    uint8_t stage = async->options ? async->options->stage : CH_STAGE_COMPLETE;
    uint8_t compression = async->options ? async->options->compression : CH_COMPRESS_NONE;

    /* Store compression method in connection for use by send_empty_block and send_data */
    conn->compression = compression;

    /* Use min of server and client revision for protocol negotiation */
    uint64_t server_rev = conn->server_info ? conn->server_info->revision : CLICKHOUSE_REVISION;
    uint64_t protocol_revision = (server_rev < CLICKHOUSE_REVISION) ? server_rev : CLICKHOUSE_REVISION;

    int write_result;
    if (async->options && (async->options->settings || async->options->params)) {
        write_result = clickhouse_write_query_ext(conn->write_buf, "", client_info, query,
                                                   async->options->settings, async->options->params,
                                                   stage, compression, protocol_revision);
    } else {
        write_result = clickhouse_write_query(conn->write_buf, "", client_info, query,
                                              stage, compression, protocol_revision);
    }
    clickhouse_client_info_free(client_info);

    if (write_result != 0) {
        clickhouse_async_query_free(async);
        clickhouse_connection_set_error(conn, "Failed to build query packet");
        return -1;
    }

    /* Send query */
    if (clickhouse_connection_send(conn) != 0) {
        clickhouse_async_query_free(async);
        return -1;
    }

    /* Send empty data block */
    if (clickhouse_connection_send_empty_block(conn) != 0) {
        clickhouse_async_query_free(async);
        return -1;
    }

    async->state = ASYNC_STATE_WAITING;
    *async_out = async;
    return 0;
}

/* Poll for async results - non-blocking */
int clickhouse_async_poll(clickhouse_connection *conn, clickhouse_async_query *async) {
    if (!async || async->state == ASYNC_STATE_COMPLETE || async->state == ASYNC_STATE_ERROR) {
        return async ? (async->state == ASYNC_STATE_COMPLETE ? 1 : -1) : -1;
    }

    /* Check if data is available (0ms timeout = immediate return) */
    int has_data = clickhouse_connection_has_data(conn, 0);
    if (has_data < 0) {
        async->state = ASYNC_STATE_ERROR;
        async->error = strdup("Error checking for data");
        return -1;
    }

    if (has_data == 0) {
        return 0; /* No data yet, still pending */
    }

    /* Read available data */
    async->state = ASYNC_STATE_READING;

    if (clickhouse_connection_receive(conn) != 0) {
        async->state = ASYNC_STATE_ERROR;
        async->error = strdup(clickhouse_connection_get_error(conn));
        return -1;
    }

    /* Process packets in buffer */
    while (clickhouse_buffer_remaining(conn->read_buf) > 0) {
        size_t saved_pos = conn->read_buf->position;
        uint64_t packet_type;

        if (clickhouse_buffer_read_varint(conn->read_buf, &packet_type) != 0) {
            conn->read_buf->position = saved_pos;
            break;
        }

        /* Get compression setting from options */
        uint8_t compression = (async->options) ? async->options->compression : CH_COMPRESS_NONE;

        switch (packet_type) {
            case CH_SERVER_DATA:
            case CH_SERVER_TOTALS:
            case CH_SERVER_EXTREMES: {
                char *table_name;
                size_t table_name_len;
                if (clickhouse_buffer_read_string(conn->read_buf, &table_name, &table_name_len) != 0) {
                    conn->read_buf->position = saved_pos;
                    async->state = ASYNC_STATE_WAITING;
                    return 0;
                }
                free(table_name);

                clickhouse_block *block = clickhouse_block_create();
                if (!block) {
                    async->state = ASYNC_STATE_ERROR;
                    async->error = strdup("Failed to create block");
                    return -1;
                }

                /* Check if server actually sent compressed data */
                if (compression != CH_COMPRESS_NONE && clickhouse_is_compressed_block(conn->read_buf)) {
                    /* Decompress block data first */
                    clickhouse_buffer *decompressed_buf = NULL;
                    int decomp_result = clickhouse_read_compressed_block(conn->read_buf, &decompressed_buf);
                    if (decomp_result == -2) {
                        /* Need more data */
                        clickhouse_block_free(block);
                        conn->read_buf->position = saved_pos;
                        async->state = ASYNC_STATE_WAITING;
                        return 0;
                    } else if (decomp_result != 0 || !decompressed_buf) {
                        clickhouse_block_free(block);
                        async->state = ASYNC_STATE_ERROR;
                        async->error = strdup("Failed to decompress block");
                        return -1;
                    }

                    if (clickhouse_block_read(decompressed_buf, block) != 0) {
                        clickhouse_buffer_free(decompressed_buf);
                        clickhouse_block_free(block);
                        async->state = ASYNC_STATE_ERROR;
                        async->error = strdup("Failed to parse decompressed block");
                        return -1;
                    }
                    clickhouse_buffer_free(decompressed_buf);
                } else {
                    if (clickhouse_block_read(conn->read_buf, block) != 0) {
                        clickhouse_block_free(block);
                        conn->read_buf->position = saved_pos;
                        async->state = ASYNC_STATE_WAITING;
                        return 0;
                    }
                }

                /* Store block based on packet type */
                if (block->row_count > 0) {
                    if (packet_type == CH_SERVER_TOTALS) {
                        clickhouse_block_free(async->result->totals);
                        async->result->totals = block;
                    } else if (packet_type == CH_SERVER_EXTREMES) {
                        clickhouse_block_free(async->result->extremes);
                        async->result->extremes = block;
                    } else {
                        clickhouse_result_add_block(async->result, block);
                    }
                } else {
                    clickhouse_block_free(block);
                }
                break;
            }

            case CH_SERVER_EXCEPTION: {
                async->result->exception = clickhouse_exception_read(conn->read_buf);
                async->state = ASYNC_STATE_ERROR;
                if (async->result->exception) {
                    async->error = strdup(async->result->exception->message);
                }
                return -1;
            }

            case CH_SERVER_PROGRESS: {
                clickhouse_progress_read(conn->read_buf, &async->result->progress);
                if (async->options && async->options->progress_callback) {
                    async->options->progress_callback(&async->result->progress,
                                                      async->options->progress_user_data);
                }
                break;
            }

            case CH_SERVER_PROFILE_INFO: {
                clickhouse_profile_info_read(conn->read_buf, &async->result->profile);
                break;
            }

            case CH_SERVER_END_OF_STREAM: {
                async->state = ASYNC_STATE_COMPLETE;
                return 1;
            }

            case CH_SERVER_LOG:
            case CH_SERVER_TABLE_COLUMNS: {
                char *dummy;
                size_t dummy_len;
                if (clickhouse_buffer_read_string(conn->read_buf, &dummy, &dummy_len) == 0) {
                    free(dummy);
                    clickhouse_block *dummy_block = clickhouse_block_create();
                    if (dummy_block) {
                        clickhouse_block_read(conn->read_buf, dummy_block);
                        clickhouse_block_free(dummy_block);
                    }
                }
                break;
            }

            default:
                async->state = ASYNC_STATE_ERROR;
                async->error = strdup("Unknown packet type from server");
                return -1;
        }
    }

    async->state = ASYNC_STATE_WAITING;
    return 0;
}
