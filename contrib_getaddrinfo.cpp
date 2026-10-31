#include <netdb.h>
#include <sys/socket.h>

/* clickhouse-cpp resolves with AI_ADDRCONFIG, which makes glibc dump the interface addresses over
 * netlink on every connection, even for an IP literal. PHP streams and mysqlnd resolve without it.
 * An address family the host cannot reach fails in connect(), and the client tries the next one. */
extern "C" int php_clickhouse_getaddrinfo(const char *node, const char *service,
                                          const struct addrinfo *hints, struct addrinfo **res)
{
    struct addrinfo resolved_hints = *hints;
    resolved_hints.ai_flags &= ~AI_ADDRCONFIG;
    return getaddrinfo(node, service, &resolved_hints, res);
}
