dnl config.m4 for extension clickhouse

PHP_ARG_ENABLE([clickhouse],
  [whether to enable clickhouse support],
  [AS_HELP_STRING([--enable-clickhouse],
    [Enable clickhouse support])],
  [no])

PHP_ARG_WITH([clickhouse-lz4],
  [for LZ4 compression support],
  [AS_HELP_STRING([--with-clickhouse-lz4],
    [Enable LZ4 compression support])],
  [yes],
  [no])

PHP_ARG_WITH([clickhouse-zstd],
  [for ZSTD compression support],
  [AS_HELP_STRING([--with-clickhouse-zstd],
    [Enable ZSTD compression support])],
  [yes],
  [no])

PHP_ARG_WITH([clickhouse-ssl],
  [for SSL/TLS support],
  [AS_HELP_STRING([--with-clickhouse-ssl],
    [Enable SSL/TLS support])],
  [yes],
  [no])

if test "$PHP_CLICKHOUSE" != "no"; then
  dnl Check for required headers
  AC_CHECK_HEADERS([sys/socket.h netinet/in.h arpa/inet.h netdb.h], [], [
    AC_MSG_ERROR([Required network headers not found])
  ])

  EXTRA_CFLAGS=""
  EXTRA_LIBS=""

  dnl Check for LZ4 library
  if test "$PHP_CLICKHOUSE_LZ4" != "no"; then
    AC_CHECK_HEADERS([lz4.h], [
      AC_CHECK_LIB([lz4], [LZ4_compress_default], [
        AC_DEFINE([HAVE_LZ4], [1], [Have LZ4 compression library])
        EXTRA_CFLAGS="$EXTRA_CFLAGS -DHAVE_LZ4"
        EXTRA_LIBS="$EXTRA_LIBS -llz4"
        PHP_ADD_LIBRARY(lz4, 1, CLICKHOUSE_SHARED_LIBADD)
      ], [
        AC_MSG_WARN([LZ4 library not found, compression support disabled])
      ])
    ], [
      AC_MSG_WARN([LZ4 headers not found, compression support disabled])
    ])
  fi

  dnl Check for ZSTD library
  if test "$PHP_CLICKHOUSE_ZSTD" != "no"; then
    AC_CHECK_HEADERS([zstd.h], [
      AC_CHECK_LIB([zstd], [ZSTD_compress], [
        AC_DEFINE([HAVE_ZSTD], [1], [Have ZSTD compression library])
        EXTRA_CFLAGS="$EXTRA_CFLAGS -DHAVE_ZSTD"
        EXTRA_LIBS="$EXTRA_LIBS -lzstd"
        PHP_ADD_LIBRARY(zstd, 1, CLICKHOUSE_SHARED_LIBADD)
      ], [
        AC_MSG_WARN([ZSTD library not found, compression support disabled])
      ])
    ], [
      AC_MSG_WARN([ZSTD headers not found, compression support disabled])
    ])
  fi

  dnl Check for OpenSSL library
  if test "$PHP_CLICKHOUSE_SSL" != "no"; then
    AC_CHECK_HEADERS([openssl/ssl.h], [
      AC_CHECK_LIB([ssl], [SSL_new], [
        AC_DEFINE([HAVE_OPENSSL], [1], [Have OpenSSL library])
        EXTRA_CFLAGS="$EXTRA_CFLAGS -DHAVE_OPENSSL"
        EXTRA_LIBS="$EXTRA_LIBS -lssl -lcrypto"
        PHP_ADD_LIBRARY(ssl, 1, CLICKHOUSE_SHARED_LIBADD)
        PHP_ADD_LIBRARY(crypto, 1, CLICKHOUSE_SHARED_LIBADD)
      ], [
        AC_MSG_WARN([OpenSSL library not found, SSL/TLS support disabled])
      ])
    ], [
      AC_MSG_WARN([OpenSSL headers not found, SSL/TLS support disabled])
    ])
  fi

  PHP_SUBST(CLICKHOUSE_SHARED_LIBADD)

  dnl Source files
  PHP_NEW_EXTENSION(clickhouse,
    clickhouse.c \
    src/buffer.c \
    src/protocol.c \
    src/connection.c \
    src/column.c \
    src/cityhash.c,
    $ext_shared,, -DZEND_ENABLE_STATIC_TSRMLS_CACHE=1 -std=c11 $EXTRA_CFLAGS)

  PHP_ADD_BUILD_DIR($ext_builddir/src)
  PHP_ADD_INCLUDE($ext_srcdir/src)
fi
