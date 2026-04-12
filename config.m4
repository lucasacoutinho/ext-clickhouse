dnl config.m4 for ext-clickhouse
dnl PHP extension wrapping clickhouse-cpp C++ client library

PHP_ARG_ENABLE([clickhouse],
  [whether to enable ClickHouse native driver],
  [AS_HELP_STRING([--enable-clickhouse],
    [Enable ClickHouse native driver extension])],
  [no])

if test "$PHP_CLICKHOUSE" != "no"; then

  PHP_REQUIRE_CXX()
  PHP_CXX_COMPILE_STDCXX([17], [mandatory], [PHP_CLICKHOUSE_STDCXX])

  dnl ext_srcdir can be empty when building in-tree; default to "."
  if test -z "$ext_srcdir"; then
    ext_srcdir="."
  fi
  CLICKHOUSE_CPP_DIR="$ext_srcdir/clickhouse-cpp"

  if test ! -d "$CLICKHOUSE_CPP_DIR/clickhouse"; then
    AC_MSG_ERROR([clickhouse-cpp not found at $CLICKHOUSE_CPP_DIR. Run: git submodule update --init --recursive])
  fi

  PHP_ADD_INCLUDE([$ext_srcdir/clickhouse-cpp])
  PHP_ADD_INCLUDE([$ext_srcdir/clickhouse-cpp/contrib/absl])
  PHP_ADD_INCLUDE([$ext_srcdir/clickhouse-cpp/contrib/cityhash/cityhash])
  PHP_ADD_INCLUDE([$ext_srcdir/clickhouse-cpp/contrib/lz4/lz4])
  PHP_ADD_INCLUDE([$ext_srcdir/clickhouse-cpp/contrib/zstd/zstd])
  PHP_ADD_INCLUDE([$ext_srcdir])

  PHP_CLICKHOUSE_SOURCES="php_clickhouse.cpp \
    contrib_absl_int128.cpp \
    contrib_cityhash.cpp \
    src/exceptions.cpp \
    src/client_options.cpp \
    src/client.cpp \
    src/block.cpp \
    src/column.cpp \
    src/column_convert.cpp \
    src/column_write.cpp \
    src/error_codes.cpp"

  dnl OpenSSL for TLS connections (e.g., ClickHouse Cloud on port 9440)
  PKG_CHECK_MODULES([OPENSSL], [openssl >= 1.1.0], [
    PHP_EVAL_INCLINE($OPENSSL_CFLAGS)
    PHP_EVAL_LIBLINE($OPENSSL_LIBS, CLICKHOUSE_SHARED_LIBADD)
    CLICKHOUSE_OPENSSL_FLAGS="-DWITH_OPENSSL=1"
  ], [
    AC_MSG_WARN([OpenSSL not found — building without TLS support])
    CLICKHOUSE_OPENSSL_FLAGS=""
  ])

  CLICKHOUSE_COMMON_FLAGS="-DZEND_ENABLE_STATIC_TSRMLS_CACHE=1 $CLICKHOUSE_OPENSSL_FLAGS"

  PHP_NEW_EXTENSION([clickhouse],
    [$PHP_CLICKHOUSE_SOURCES],
    [$ext_shared],,
    [$CLICKHOUSE_COMMON_FLAGS $PHP_CLICKHOUSE_STDCXX],
    [cxx])

  CLICKHOUSE_CPP_CXX_SOURCES=" \
    clickhouse-cpp/clickhouse/base/compressed.cpp \
    clickhouse-cpp/clickhouse/base/input.cpp \
    clickhouse-cpp/clickhouse/base/output.cpp \
    clickhouse-cpp/clickhouse/base/platform.cpp \
    clickhouse-cpp/clickhouse/base/socket.cpp \
    clickhouse-cpp/clickhouse/base/wire_format.cpp \
    clickhouse-cpp/clickhouse/base/endpoints_iterator.cpp \
    clickhouse-cpp/clickhouse/columns/array.cpp \
    clickhouse-cpp/clickhouse/columns/column.cpp \
    clickhouse-cpp/clickhouse/columns/date.cpp \
    clickhouse-cpp/clickhouse/columns/decimal.cpp \
    clickhouse-cpp/clickhouse/columns/enum.cpp \
    clickhouse-cpp/clickhouse/columns/factory.cpp \
    clickhouse-cpp/clickhouse/columns/geo.cpp \
    clickhouse-cpp/clickhouse/columns/ip4.cpp \
    clickhouse-cpp/clickhouse/columns/ip6.cpp \
    clickhouse-cpp/clickhouse/columns/lowcardinality.cpp \
    clickhouse-cpp/clickhouse/columns/nullable.cpp \
    clickhouse-cpp/clickhouse/columns/numeric.cpp \
    clickhouse-cpp/clickhouse/columns/map.cpp \
    clickhouse-cpp/clickhouse/columns/string.cpp \
    clickhouse-cpp/clickhouse/columns/tuple.cpp \
    clickhouse-cpp/clickhouse/columns/uuid.cpp \
    clickhouse-cpp/clickhouse/columns/itemview.cpp \
    clickhouse-cpp/clickhouse/types/type_parser.cpp \
    clickhouse-cpp/clickhouse/types/types.cpp \
    clickhouse-cpp/clickhouse/block.cpp \
    clickhouse-cpp/clickhouse/client.cpp \
    clickhouse-cpp/clickhouse/query.cpp"

  dnl Add SSL socket source when OpenSSL is available
  if test -n "$CLICKHOUSE_OPENSSL_FLAGS"; then
    CLICKHOUSE_CPP_CXX_SOURCES="$CLICKHOUSE_CPP_CXX_SOURCES \
      clickhouse-cpp/clickhouse/base/sslsocket.cpp"
  fi

  CLICKHOUSE_CPP_CXX_FLAGS="$PHP_CLICKHOUSE_STDCXX -Wno-write-strings $CLICKHOUSE_OPENSSL_FLAGS"

  AS_VAR_IF([ext_shared], [no],
    [PHP_ADD_SOURCES([$ext_dir],
      [$CLICKHOUSE_CPP_CXX_SOURCES],
      [$CLICKHOUSE_CPP_CXX_FLAGS])],
    [PHP_ADD_SOURCES_X([$ext_dir],
      [$CLICKHOUSE_CPP_CXX_SOURCES],
      [$CLICKHOUSE_CPP_CXX_FLAGS],
      [shared_objects_clickhouse],
      [yes])])

  CLICKHOUSE_LZ4_SOURCES=" \
    clickhouse-cpp/contrib/lz4/lz4/lz4.c \
    clickhouse-cpp/contrib/lz4/lz4/lz4hc.c"

  AS_VAR_IF([ext_shared], [no],
    [PHP_ADD_SOURCES([$ext_dir],
      [$CLICKHOUSE_LZ4_SOURCES],
      [])],
    [PHP_ADD_SOURCES_X([$ext_dir],
      [$CLICKHOUSE_LZ4_SOURCES],
      [],
      [shared_objects_clickhouse],
      [yes])])

  CLICKHOUSE_ZSTD_SOURCES=" \
    clickhouse-cpp/contrib/zstd/zstd/common/debug.c \
    clickhouse-cpp/contrib/zstd/zstd/common/entropy_common.c \
    clickhouse-cpp/contrib/zstd/zstd/common/error_private.c \
    clickhouse-cpp/contrib/zstd/zstd/common/fse_decompress.c \
    clickhouse-cpp/contrib/zstd/zstd/common/pool.c \
    clickhouse-cpp/contrib/zstd/zstd/common/threading.c \
    clickhouse-cpp/contrib/zstd/zstd/common/xxhash.c \
    clickhouse-cpp/contrib/zstd/zstd/common/zstd_common.c \
    clickhouse-cpp/contrib/zstd/zstd/compress/fse_compress.c \
    clickhouse-cpp/contrib/zstd/zstd/compress/hist.c \
    clickhouse-cpp/contrib/zstd/zstd/compress/huf_compress.c \
    clickhouse-cpp/contrib/zstd/zstd/compress/zstd_compress.c \
    clickhouse-cpp/contrib/zstd/zstd/compress/zstd_compress_literals.c \
    clickhouse-cpp/contrib/zstd/zstd/compress/zstd_compress_sequences.c \
    clickhouse-cpp/contrib/zstd/zstd/compress/zstd_compress_superblock.c \
    clickhouse-cpp/contrib/zstd/zstd/compress/zstd_double_fast.c \
    clickhouse-cpp/contrib/zstd/zstd/compress/zstd_fast.c \
    clickhouse-cpp/contrib/zstd/zstd/compress/zstd_lazy.c \
    clickhouse-cpp/contrib/zstd/zstd/compress/zstd_ldm.c \
    clickhouse-cpp/contrib/zstd/zstd/compress/zstdmt_compress.c \
    clickhouse-cpp/contrib/zstd/zstd/compress/zstd_opt.c \
    clickhouse-cpp/contrib/zstd/zstd/decompress/huf_decompress.c \
    clickhouse-cpp/contrib/zstd/zstd/decompress/zstd_ddict.c \
    clickhouse-cpp/contrib/zstd/zstd/decompress/zstd_decompress_block.c \
    clickhouse-cpp/contrib/zstd/zstd/decompress/zstd_decompress.c \
    clickhouse-cpp/contrib/zstd/zstd/dictBuilder/cover.c \
    clickhouse-cpp/contrib/zstd/zstd/dictBuilder/divsufsort.c \
    clickhouse-cpp/contrib/zstd/zstd/dictBuilder/fastcover.c \
    clickhouse-cpp/contrib/zstd/zstd/dictBuilder/zdict.c \
    clickhouse-cpp/contrib/zstd/zstd/legacy/zstd_v01.c \
    clickhouse-cpp/contrib/zstd/zstd/legacy/zstd_v02.c \
    clickhouse-cpp/contrib/zstd/zstd/legacy/zstd_v03.c \
    clickhouse-cpp/contrib/zstd/zstd/legacy/zstd_v04.c \
    clickhouse-cpp/contrib/zstd/zstd/legacy/zstd_v05.c \
    clickhouse-cpp/contrib/zstd/zstd/legacy/zstd_v06.c \
    clickhouse-cpp/contrib/zstd/zstd/legacy/zstd_v07.c"

  CLICKHOUSE_ZSTD_FLAGS="-DZSTD_LEGACY_SUPPORT=1 -DZSTD_DISABLE_ASM=1"

  AS_VAR_IF([ext_shared], [no],
    [PHP_ADD_SOURCES([$ext_dir],
      [$CLICKHOUSE_ZSTD_SOURCES],
      [$CLICKHOUSE_ZSTD_FLAGS])],
    [PHP_ADD_SOURCES_X([$ext_dir],
      [$CLICKHOUSE_ZSTD_SOURCES],
      [$CLICKHOUSE_ZSTD_FLAGS],
      [shared_objects_clickhouse],
      [yes])])

  PHP_ADD_BUILD_DIR([$ext_builddir/src])
  PHP_ADD_BUILD_DIR([$ext_builddir/clickhouse-cpp/clickhouse])
  PHP_ADD_BUILD_DIR([$ext_builddir/clickhouse-cpp/clickhouse/base])
  PHP_ADD_BUILD_DIR([$ext_builddir/clickhouse-cpp/clickhouse/columns])
  PHP_ADD_BUILD_DIR([$ext_builddir/clickhouse-cpp/clickhouse/types])
  PHP_ADD_BUILD_DIR([$ext_builddir/clickhouse-cpp/contrib/lz4/lz4])
  PHP_ADD_BUILD_DIR([$ext_builddir/clickhouse-cpp/contrib/zstd/zstd/common])
  PHP_ADD_BUILD_DIR([$ext_builddir/clickhouse-cpp/contrib/zstd/zstd/compress])
  PHP_ADD_BUILD_DIR([$ext_builddir/clickhouse-cpp/contrib/zstd/zstd/decompress])
  PHP_ADD_BUILD_DIR([$ext_builddir/clickhouse-cpp/contrib/zstd/zstd/dictBuilder])
  PHP_ADD_BUILD_DIR([$ext_builddir/clickhouse-cpp/contrib/zstd/zstd/legacy])

  PHP_ADD_LIBRARY(stdc++, 1, CLICKHOUSE_SHARED_LIBADD)
  PHP_ADD_LIBRARY(pthread, 1, CLICKHOUSE_SHARED_LIBADD)
  PHP_SUBST([CLICKHOUSE_SHARED_LIBADD])

  AC_DEFINE([HAVE_CLICKHOUSE], [1],
    [Define to 1 if the PHP extension 'clickhouse' is available.])
fi
