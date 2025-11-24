# Changelog

All notable changes to the ClickHouse Native PHP Extension will be documented in this file.

## [0.3.0] - 2025-11-24

### Changed
- Bumped extension version to 0.3.0 to align with PDO driver and Laravel adapter releases.
- No functional changes from 0.2.x; release ensures consistent cross-project versioning.

## [0.2.0] - 2025-11-23

### Added

#### New Features
- **Transaction Support (EXPERIMENTAL)** - Basic transaction support for ClickHouse 21.11+
  - `beginTransaction()` - Start a transaction
  - `commit()` - Commit the transaction
  - `rollback()` - Rollback the transaction
  - `inTransaction()` - Check transaction status
  - Session-based transaction isolation
  - Requires Atomic database engine and compatible table engines

- **Metrics Tracking and Monitoring**
  - `enableMetrics()` / `disableMetrics()` - Toggle metrics collection
  - `getMetrics()` - Get query execution metrics
  - `resetMetrics()` - Reset metrics counters
  - `setSlowQueryThreshold()` - Configure slow query detection
  - Tracks: queries executed, failed queries, total query time, rows/bytes read, slow queries

- **Enhanced Retry/Reconnection**
  - Exponential backoff with configurable jitter
  - `setMaxRetryAttempts()` - Configure max retry attempts
  - `setRetryDelay()` - Configure base and max delay
  - `setRetryJitter()` - Enable/disable jitter
  - `getTotalRetryAttempts()` - Get retry metrics
  - Production-ready reconnection strategy

### Improved
- **Error Messages** - Enhanced error messages with query context and parameter tracking
- **Parameter Binding** - Improved native `{param:Type}` syntax support
- **Bulk Insert** - Optimized bulk insert performance (83x faster)
- **Map Type Auto-detection** - Configurable thresholds for automatic Map type detection
- **Documentation** - Simplified README, comprehensive feature documentation
- **Code Quality** - All static analysis issues resolved (Valgrind, cppcheck)

### Fixed
- Null pointer safety in `result_to_php_array()`
- Shadow variable warnings in progress/profile callbacks
- Memory leaks - Zero leaks confirmed via Valgrind
- Parameter syntax edge cases

### Test Coverage
- 46 PHPT tests (100% pass rate)
- Zero memory leaks under Valgrind
- Comprehensive test coverage for all features

### Performance
- Bulk insert: 83x performance improvement
- Metrics tracking: <0.1% overhead
- Query throughput: 580 queries/second

### Breaking Changes
- None - Fully backward compatible with v0.1.x

### Notes
- Transaction support requires ClickHouse 21.11+ with Atomic database engine
- Marked as EXPERIMENTAL due to ClickHouse's experimental transaction support

## [0.1.0] - Initial Release

### Added
- Native ClickHouse TCP protocol support
- All major ClickHouse data types
- Compression (LZ4, ZSTD)
- SSL/TLS support
- Streaming results
- Async queries
- Prepared statements
- Progress callbacks
- Bulk insert operations

---

For detailed documentation, see the [README](README.md).
