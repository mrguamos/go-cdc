# Go PostgreSQL CDC Tool

A Change Data Capture (CDC) tool for PostgreSQL written in Go, designed to capture and stream database changes in a Debezium-like format.

## Features

- Logical replication using PostgreSQL's native replication protocol
- Debezium-like message format for compatibility
- Automatic reconnection and error recovery
- Circuit breaker pattern for handling database failures
- Configurable retry mechanisms
- Metrics endpoint for monitoring
- Support for multiple databases
- Initial snapshot capability
- Configurable error handling for snapshots

## Configuration

The tool is configured using a YAML file (`config.yaml`). Here's an example configuration:

```yaml
connector_name: "go-cdc"
flush_interval: "10s"
offset_file: "offset.json"
fail_on_snapshot_error: true  # Whether to fail the application if initial snapshot fails

retry_config:
  max_retries: 3
  initial_backoff: "1s"
  max_backoff: "30s"
  backoff_multiplier: 2.0

databases:
  - conn_str: "postgres://user:password@localhost:5432/db1"
    slot_name: "slot1"
    publication_name: "pub1"
    tables:
      - "public.users"
      - "public.orders"
  - conn_str: "postgres://user:password@localhost:5432/db2"
    slot_name: "slot2"
    publication_name: "pub2"
    tables:
      - "public.products"
```

### Configuration Options

- `connector_name`: Name of the connector instance
- `flush_interval`: How often to flush the LSN offset to disk
- `offset_file`: Path to store the replication offset
- `fail_on_snapshot_error`: Whether to fail the application if initial snapshot fails (default: true)
- `retry_config`: Configuration for retry behavior
  - `max_retries`: Maximum number of retry attempts
  - `initial_backoff`: Initial backoff duration
  - `max_backoff`: Maximum backoff duration
  - `backoff_multiplier`: Multiplier for exponential backoff

### Database Configuration

Each database configuration includes:
- `conn_str`: PostgreSQL connection string
- `slot_name`: Name of the replication slot
- `publication_name`: Name of the publication
- `tables`: List of tables to monitor (format: schema.table)

## Error Handling

The tool implements several error handling mechanisms:

1. **Circuit Breaker Pattern**: Automatically opens the circuit after multiple failures to prevent cascading failures
2. **Exponential Backoff**: Implements retry with exponential backoff for transient failures
3. **Snapshot Error Handling**: Configurable behavior for initial snapshot failures
   - When `fail_on_snapshot_error` is true, the application will exit if the snapshot fails
   - When false, it will log a warning and continue with replication

## Metrics

The tool exposes metrics via HTTP endpoint (`/metrics`). Metrics include:
- Total errors
- Database errors
- Processing errors
- File errors
- Error rate
- Database connection status
- Circuit breaker state

## Building and Running

```bash
# Build the application
go build -o go-cdc

# Run with default config
./go-cdc

# Run with custom config
./go-cdc -config custom-config.yaml
```

## Requirements

- PostgreSQL 10 or later
- Logical replication enabled
- Replication permissions for the database user
- Go 1.16 or later

## PostgreSQL Setup

1. Enable logical replication in `postgresql.conf`:
```ini
wal_level = logical
max_wal_senders = 10
max_replication_slots = 10
```

2. Create a user with replication permissions:
```sql
CREATE USER cdc_user WITH REPLICATION PASSWORD 'password';
```

3. Grant necessary permissions:
```sql
GRANT USAGE ON SCHEMA public TO cdc_user;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO cdc_user;
```

## License

MIT License