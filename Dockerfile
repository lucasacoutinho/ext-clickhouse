ARG PHP_VERSION=8.4

# --- Stage 1: Build ---
FROM php:${PHP_VERSION}-cli AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    autoconf g++ make \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /ext
COPY . .

RUN phpize \
    && ./configure --enable-clickhouse \
    && make -j$(nproc)

# --- Stage 2: Runtime ---
FROM php:${PHP_VERSION}-cli

COPY --from=builder /ext/modules/clickhouse.so /tmp/clickhouse.so
RUN cp /tmp/clickhouse.so $(php -r 'echo ini_get("extension_dir");')/ \
    && rm /tmp/clickhouse.so \
    && docker-php-ext-enable clickhouse

RUN php -m | grep clickhouse
