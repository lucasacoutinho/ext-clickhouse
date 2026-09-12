#!/usr/bin/env bash

set -euo pipefail

repo_root="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
evidence_root="$repo_root/../evidence"
mkdir -p "$evidence_root"
evidence_root="$(CDPATH= cd -- "$evidence_root" && pwd)"
run_id="$(date -u +%Y%m%dT%H%M%SZ)-$$"
run_root="$evidence_root/time64-build-$run_id"
image="clickhouse-time64-${run_id}-current-386"
tests_dir="$repo_root/tests"
runner_tests="$run_root/phpt-tests"

mkdir -p "$runner_tests"
cp -- "$tests_dir/019-clickhouse-cpp-2.6.2-types.phpt" "$runner_tests/"
cp -- "$tests_dir/022-time64-boundaries.phpt" "$runner_tests/"
cp -- "$tests_dir/clickhouse_compat.inc" "$runner_tests/"

cleanup()
{
    docker image rm "$image" >/dev/null 2>&1 || true
}
trap cleanup EXIT

{
    printf 'repository: %s\n' "$repo_root"
    printf 'commit: '
    git -C "$repo_root" rev-parse HEAD
    printf 'docker platform: linux/386\n'
    printf 'php image: php:8.5-cli\n'
    printf 'php test runner: /usr/local/lib/php/build/run-tests.php\n'
    docker version --format '{{.Server.Version}}'
} > "$run_root/metadata.txt"

printf 'Building current checkout (%s)\n' "$image"
tar \
    --exclude='*.dep' \
    --exclude='.git' \
    --exclude='./autom4te.cache' \
    --exclude='./modules' \
    --exclude='./.libs' \
    --exclude='*.o' \
    --exclude='*.lo' \
    --exclude='*.la' \
    --exclude='*.so' \
    --exclude='./configure' \
    --exclude='./configure~' \
    --exclude='./config.nice' \
    --exclude='./config.h' \
    --exclude='./config.h.in' \
    --exclude='./config.log' \
    --exclude='./config.status' \
    --exclude='./Makefile' \
    --exclude='./Makefile.global' \
    --exclude='./Makefile.objects' \
    --exclude='./Makefile.fragments' \
    --exclude='./libtool' \
    --exclude='./run-tests.php' \
    --exclude='./.github' \
    --exclude='./docs' \
    -C "$repo_root" -cf - . |
    docker build \
        --platform=linux/386 \
        --progress=plain \
        --build-arg PHP_VERSION=8.5 \
        --tag "$image" \
        --file Dockerfile \
        - 2>&1 | tee "$run_root/build.log"

printf 'Checking 32-bit PHP and extension load\n'
docker run --rm \
    --platform=linux/386 \
    "$image" \
    php -n -d extension=clickhouse -r \
    'if (PHP_INT_SIZE !== 4 || !extension_loaded("clickhouse")) { fwrite(STDERR, "PHP_INT_SIZE/module assertion failed\n"); exit(1); } echo "PHP_INT_SIZE=", PHP_INT_SIZE, " clickhouse=loaded\n";' \
    2>&1 | tee "$run_root/runtime.log"

printf 'Running PHPT 022 with the official PHP runner\n'
docker run --rm \
    --platform=linux/386 \
    -e REPORT_EXIT_STATUS=1 \
    --mount "type=bind,source=$runner_tests,target=/tmp/ext-tests" \
    "$image" \
    php -n /usr/local/lib/php/build/run-tests.php \
    -n -d extension=clickhouse --show-diff -q \
    /tmp/ext-tests/022-time64-boundaries.phpt \
    2>&1 | tee "$run_root/phpt-022.log"
grep -Eq 'Tests passed +: +1' "$run_root/phpt-022.log"
grep -Eq 'Tests skipped +: +0' "$run_root/phpt-022.log"
grep -Eq 'Tests failed +: +0' "$run_root/phpt-022.log"

printf 'Running PHPT 019 baseline with the official PHP runner\n'
docker run --rm \
    --platform=linux/386 \
    -e REPORT_EXIT_STATUS=1 \
    --mount "type=bind,source=$runner_tests,target=/tmp/ext-tests" \
    "$image" \
    php -n /usr/local/lib/php/build/run-tests.php \
    -n -d extension=clickhouse --show-diff -q \
    /tmp/ext-tests/019-clickhouse-cpp-2.6.2-types.phpt \
    2>&1 | tee "$run_root/phpt-019.log"
grep -Eq 'Tests passed +: +1' "$run_root/phpt-019.log"
grep -Eq 'Tests skipped +: +0' "$run_root/phpt-019.log"
grep -Eq 'Tests failed +: +0' "$run_root/phpt-019.log"

{
    printf 'current-386: PASS\n'
    printf 'runtime: PHP_INT_SIZE=4, clickhouse loaded\n'
    printf 'phpt-022: PASS\n'
    printf 'phpt-019: PASS\n'
    printf 'evidence: %s\n' "$run_root"
} | tee "$run_root/result.txt"
