#!/bin/sh

set -eu

: "${PHP_BIN:=/opt/php/bin/php}"
: "${RUN_TESTS:=/work/run-tests.php}"
: "${SANITIZER_TEST_DIR:=tests}"
: "${SANITIZER_EXTENSION_PATHS:=modules/clickhouse.so}"
: "${SANITIZER_REQUIRED_EXTENSIONS:=date,json,pcntl,clickhouse}"
: "${SANITIZER_KIND:=asan}"
: "${SANITIZER_TEST_SUITE:=native}"
: "${SANITIZER_TEST_PATTERN:=0*.phpt}"

set -- "$SANITIZER_TEST_DIR"/$SANITIZER_TEST_PATTERN
if [ ! -f "$1" ]; then
    echo "No PHPT files found in $SANITIZER_TEST_DIR" >&2
    exit 1
fi

php_args=""
for extension_path in $SANITIZER_EXTENSION_PATHS; do
    php_args="$php_args -d extension=$extension_path"
done

required_extensions=$(printf '%s' "$SANITIZER_REQUIRED_EXTENSIONS" | tr ',' ' ')
if ! env PHP_REQUIRED_EXTENSIONS="$required_extensions" "$PHP_BIN" -n $php_args -r '
    $missing = [];
    foreach (preg_split("/\\s+/", trim((string) getenv("PHP_REQUIRED_EXTENSIONS"))) as $extension) {
        if ($extension !== "" && !extension_loaded($extension)) {
            $missing[] = $extension;
        }
    }
    if ($missing !== []) {
        fwrite(STDERR, "Missing required PHP extensions: " . implode(", ", $missing) . PHP_EOL);
        exit(1);
    }
    printf("Sanitizer PHP %s (%s); loaded extensions: %s\n", PHP_VERSION, PHP_BINARY, implode(", ", get_loaded_extensions()));
'; then
    exit 1
fi

php_version_id=$($PHP_BIN -n -r 'echo PHP_VERSION_ID;')
allowed_skips=
if [ "$SANITIZER_TEST_SUITE" = "pdo" ] && [ "$php_version_id" -lt 80100 ]; then
    allowed_skips='tests/007-pdo-types.phpt
tests/008-pdo-lowcardinality.phpt'
fi

log_file=$(mktemp)
normalized_log=$(mktemp)
trap 'rm -f "$log_file" "$normalized_log"' EXIT

set +e
env \
    USE_ZEND_ALLOC=0 \
    CLICKHOUSE_SANITIZER=1 \
    TEST_PHP_EXECUTABLE="$PHP_BIN" \
    UBSAN_OPTIONS="halt_on_error=1:print_stacktrace=1" \
    ASAN_OPTIONS="detect_leaks=0:abort_on_error=1" \
    "$PHP_BIN" -n "$RUN_TESTS" -n $php_args \
    --show-diff --no-color -q "$@" >"$log_file" 2>&1
runner_status=$?
set -e
cat "$log_file"
tr '\r' '\n' <"$log_file" >"$normalized_log"

if [ "$runner_status" -ne 0 ]; then
    echo "PHPT runner exited with status $runner_status" >&2
    exit "$runner_status"
fi

summary_value() {
    key=$1
    awk -v key="$key" '$1 == "Tests" && $2 == key { gsub(/[^0-9]/, "", $4); print $4; exit }' "$normalized_log"
}

test_count=$(awk '$1 == "Number" && $2 == "of" && $3 == "tests" { gsub(/[^0-9]/, "", $5); print $5; exit }' "$normalized_log")
skipped=$(summary_value skipped)
warned=$(summary_value warned)
failed=$(summary_value failed)
borked=$(summary_value borked)
passed=$(summary_value passed)

: "${skipped:=0}"
: "${warned:=0}"
: "${failed:=0}"
: "${borked:=0}"

for value_name in test_count skipped warned failed borked passed; do
    eval "value=\${$value_name:-}"
    case "$value" in
        ''|*[!0-9]*)
            echo "PHPT summary is missing a numeric $value_name value" >&2
            exit 1
            ;;
    esac
done

expected_test_count=$#
if [ "$test_count" -ne "$expected_test_count" ]; then
    echo "PHPT runner counted $test_count tests; expected $expected_test_count" >&2
    exit 1
fi

actual_skips=$(sed -nE 's/^SKIP .* \[([^]]+)\].*$/\1/p' "$normalized_log" | sort)
expected_skips=$(printf '%s\n' "$allowed_skips" | sed '/^$/d' | sort)
if [ "$actual_skips" != "$expected_skips" ]; then
    echo "Unexpected skipped PHPT files" >&2
    printf 'Expected:\n%s\nActual:\n%s\n' "$expected_skips" "$actual_skips" >&2
    exit 1
fi

allowed_skip_count=$(printf '%s\n' "$expected_skips" | sed '/^$/d' | wc -l | tr -d ' ')
expected_passed=$((expected_test_count - allowed_skip_count))
if [ "$passed" -ne "$expected_passed" ] || [ "$skipped" -ne "$allowed_skip_count" ] || [ "$warned" -ne 0 ] || [ "$failed" -ne 0 ] || [ "$borked" -ne 0 ]; then
    echo "PHPT execution gate failed: passed=$passed skipped=$skipped warned=$warned failed=$failed borked=$borked" >&2
    exit 1
fi

echo "PHPT execution gate passed: $passed passed, $skipped allowed skips, no warnings/failures/borked tests"
