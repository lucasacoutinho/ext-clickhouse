cmake_minimum_required(VERSION 3.16)
project(php-clickhouse VERSION 1.0.0 LANGUAGES C)

set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

# Options
option(WITH_LZ4 "Enable LZ4 compression support" ON)
option(WITH_ZSTD "Enable ZSTD compression support" ON)
option(WITH_SSL "Enable SSL/TLS support" ON)

# Find PHP
find_program(PHP_CONFIG php-config)
if(NOT PHP_CONFIG)
    message(FATAL_ERROR "php-config not found. Please install PHP development packages.")
endif()

execute_process(
    COMMAND ${PHP_CONFIG} --includes
    OUTPUT_VARIABLE PHP_INCLUDES
    OUTPUT_STRIP_TRAILING_WHITESPACE
)

execute_process(
    COMMAND ${PHP_CONFIG} --extension-dir
    OUTPUT_VARIABLE PHP_EXTENSION_DIR
    OUTPUT_STRIP_TRAILING_WHITESPACE
)

execute_process(
    COMMAND ${PHP_CONFIG} --php-binary
    OUTPUT_VARIABLE PHP_BINARY
    OUTPUT_STRIP_TRAILING_WHITESPACE
)

execute_process(
    COMMAND ${PHP_CONFIG} --version
    OUTPUT_VARIABLE PHP_VERSION
    OUTPUT_STRIP_TRAILING_WHITESPACE
)

message(STATUS "PHP Version: ${PHP_VERSION}")
message(STATUS "PHP Extension Dir: ${PHP_EXTENSION_DIR}")
message(STATUS "PHP Includes: ${PHP_INCLUDES}")

# Convert includes to proper format
string(REPLACE "-I" "" PHP_INCLUDE_DIRS "${PHP_INCLUDES}")
string(REPLACE " " ";" PHP_INCLUDE_DIRS "${PHP_INCLUDE_DIRS}")

# Source files
set(CLICKHOUSE_SOURCES
    clickhouse.c
    src/buffer.c
    src/column.c
    src/connection.c
    src/protocol.c
    src/cityhash.c
)

# Create the shared library
add_library(clickhouse SHARED ${CLICKHOUSE_SOURCES})

# Set output name without 'lib' prefix
set_target_properties(clickhouse PROPERTIES
    PREFIX ""
    OUTPUT_NAME "clickhouse"
    SUFFIX ".so"
)

# Include directories
target_include_directories(clickhouse PRIVATE
    ${PHP_INCLUDE_DIRS}
    ${CMAKE_SOURCE_DIR}
    ${CMAKE_SOURCE_DIR}/src
)

# Compile definitions
target_compile_definitions(clickhouse PRIVATE
    HAVE_CONFIG_H
    COMPILE_DL_CLICKHOUSE
    PHP_ATOM_INC
)

# Find and link LZ4
if(WITH_LZ4)
    find_package(PkgConfig QUIET)
    if(PkgConfig_FOUND)
        pkg_check_modules(LZ4 liblz4)
    endif()

    if(LZ4_FOUND)
        target_include_directories(clickhouse PRIVATE ${LZ4_INCLUDE_DIRS})
        target_link_libraries(clickhouse ${LZ4_LIBRARIES})
        target_compile_definitions(clickhouse PRIVATE HAVE_LZ4)
        message(STATUS "LZ4 support: enabled")
    else()
        find_library(LZ4_LIBRARY NAMES lz4)
        find_path(LZ4_INCLUDE_DIR NAMES lz4.h)
        if(LZ4_LIBRARY AND LZ4_INCLUDE_DIR)
            target_include_directories(clickhouse PRIVATE ${LZ4_INCLUDE_DIR})
            target_link_libraries(clickhouse ${LZ4_LIBRARY})
            target_compile_definitions(clickhouse PRIVATE HAVE_LZ4)
            message(STATUS "LZ4 support: enabled")
        else()
            message(STATUS "LZ4 support: disabled (library not found)")
        endif()
    endif()
endif()

# Find and link ZSTD
if(WITH_ZSTD)
    find_package(PkgConfig QUIET)
    if(PkgConfig_FOUND)
        pkg_check_modules(ZSTD libzstd)
    endif()

    if(ZSTD_FOUND)
        target_include_directories(clickhouse PRIVATE ${ZSTD_INCLUDE_DIRS})
        target_link_libraries(clickhouse ${ZSTD_LIBRARIES})
        target_compile_definitions(clickhouse PRIVATE HAVE_ZSTD)
        message(STATUS "ZSTD support: enabled")
    else()
        find_library(ZSTD_LIBRARY NAMES zstd)
        find_path(ZSTD_INCLUDE_DIR NAMES zstd.h)
        if(ZSTD_LIBRARY AND ZSTD_INCLUDE_DIR)
            target_include_directories(clickhouse PRIVATE ${ZSTD_INCLUDE_DIR})
            target_link_libraries(clickhouse ${ZSTD_LIBRARY})
            target_compile_definitions(clickhouse PRIVATE HAVE_ZSTD)
            message(STATUS "ZSTD support: enabled")
        else()
            message(STATUS "ZSTD support: disabled (library not found)")
        endif()
    endif()
endif()

# Find and link OpenSSL
if(WITH_SSL)
    find_package(OpenSSL)
    if(OpenSSL_FOUND)
        target_include_directories(clickhouse PRIVATE ${OPENSSL_INCLUDE_DIR})
        target_link_libraries(clickhouse OpenSSL::SSL OpenSSL::Crypto)
        target_compile_definitions(clickhouse PRIVATE HAVE_OPENSSL)
        message(STATUS "OpenSSL support: enabled (${OPENSSL_VERSION})")
    else()
        message(STATUS "OpenSSL support: disabled (library not found)")
    endif()
endif()

# Compiler warnings
target_compile_options(clickhouse PRIVATE
    -Wall
    -Wextra
    -Wno-unused-parameter
    -Wno-sign-compare
)

# Install target
install(TARGETS clickhouse
    LIBRARY DESTINATION ${PHP_EXTENSION_DIR}
)

# Generate config.h
configure_file(
    ${CMAKE_SOURCE_DIR}/config.h.cmake.in
    ${CMAKE_BINARY_DIR}/config.h
    @ONLY
)
target_include_directories(clickhouse PRIVATE ${CMAKE_BINARY_DIR})

# Custom target for running tests
add_custom_target(test
    COMMAND ${CMAKE_COMMAND} -E env
        CLICKHOUSE_HOST=localhost
        CLICKHOUSE_PORT=9000
        ${PHP_BINARY} -d "extension=${CMAKE_BINARY_DIR}/clickhouse.so"
        ${CMAKE_SOURCE_DIR}/run-tests.php
        ${CMAKE_SOURCE_DIR}/tests/
    DEPENDS clickhouse
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
    COMMENT "Running PHP extension tests"
)

# Print summary
message(STATUS "")
message(STATUS "=== Build Configuration ===")
message(STATUS "PHP Version:     ${PHP_VERSION}")
message(STATUS "Install Dir:     ${PHP_EXTENSION_DIR}")
message(STATUS "LZ4 Support:     ${WITH_LZ4}")
message(STATUS "ZSTD Support:    ${WITH_ZSTD}")
message(STATUS "SSL Support:     ${WITH_SSL}")
message(STATUS "===========================")
