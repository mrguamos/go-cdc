# Go PostgreSQL CDC Tool

A Change Data Capture (CDC) tool for PostgreSQL written in Go, implementing logical replication to capture database changes in real-time.

## Features

- Real-time change capture using PostgreSQL logical replication
- Support for multiple databases
- Automatic failover with active/standby instances
- Circuit breaker pattern for handling database connection issues
- Metrics endpoint for monitoring
- Offset tracking for reliable message delivery
- Graceful shutdown handling

## Requirements

- Go 1.16 or later
- PostgreSQL 10 or later with logical replication enabled

### PostgreSQL Configuration

The following settings must be configured in your PostgreSQL server's `postgresql.conf`:

```ini
# Enable logical replication
wal_level = logical

# Increase the number of replication connections
max_wal_senders = 20

# Increase the number of replication slots
max_replication_slots = 20
```

To apply these changes:
1. Edit your postgresql.conf file (usually located in /etc/postgresql/[version]/main/)
2. Set the values as shown above
3. Restart PostgreSQL: `sudo systemctl restart postgresql`

Note: The values shown are minimum recommendations. You may need to adjust them based on your specific needs:
- `max_wal_senders`: Each replication connection requires one sender. Set this higher if you have multiple replicas or tools using replication.
- `max_replication_slots`: Each logical replication consumer requires a slot. Set this higher if you have multiple consumers.

## Configuration

Create a `config.yaml` file in your project root:

```yaml
connector_name: "go-cdc"
flush_interval: 10s
offset_file: "offset.json"
inactive_slot_check: true
retry_config:
  max_retries: 3
  initial_backoff: 1s
  max_backoff: 30s
  backoff_multiplier: 2.0

databases:
  - conn_str: "postgres://user1:pass1@host1:5432/db1"
    slot_name: "slot1"
    publication_name: "pub1"
    tables:
      - "schema1.table1"
      - "schema1.table2"
  
  - conn_str: "postgres://user2:pass2@host2:5432/db2"
    slot_name: "slot2"
    publication_name: "pub2"
    tables:
      - "schema2.table1"
```

Configuration options:
- `connector_name`: Name of the CDC connector
- `flush_interval`: How often to flush LSN offsets to disk
- `offset_file`: Base name for offset files (each database will append its slot name)
- `inactive_slot_check`: Whether to drop inactive replication slots on startup
- `retry_config`: Configuration for retry behavior
  - `max_retries`: Maximum number of retry attempts (default: 3)
  - `initial_backoff`: Initial wait time between retries (default: 1s)
  - `max_backoff`: Maximum wait time between retries (default: 30s)
  - `backoff_multiplier`: Factor to multiply backoff by each retry (default: 2.0)
- `databases`: List of database configurations
  - `conn_str`: PostgreSQL connection string
  - `slot_name`: Replication slot name (must be unique)
  - `publication_name`: Publication name
  - `tables`: List of tables to monitor in "schema.table" format

## Build

1. Save all the `.go` files and `go.mod` into a directory
2. Navigate to the directory in your terminal
3. Tidy dependencies: `go mod tidy`
4. Build the executable: `go build -o go-cdc`

## Run

```bash
go run .
```

The application will:
1. Read the configuration from `config.yaml`
2. Connect to each configured database
3. Create replication slots and publications if they don't exist
4. Perform initial snapshots if no offset exists
5. Start streaming changes from all databases concurrently

## Output Format

The application outputs Debezium-like JSON messages to stdout:

```json
{
  "message": {
    "before": null,
    "after": {
      "aggregate_id": "order-123",
      "aggregate_type": "order",
      "created_at": "2025-04-10 18:43:26.986146+08",
      "id": "d9851c73-7f9f-4ede-8896-5d8110ed9a6d",
      "payload": "{\"amount\": 150.0, \"orderId\": \"order-123\"}",
      "topic": "order-events",
      "type": "OrderCreated"
    },
    "source": {
      "version": "1.0",
      "connector": "food_bear_publication",
      "name": "food_bear_publication_public",
      "ts_ms": 1744281806989,
      "snapshot": "false",
      "db": "public",
      "schema": "public",
      "table": "outbox",
      "txId": 0,
      "lsn": 146,
      "xmin": null
    },
    "op": "c"
  }
}
```

Each message contains:
- `message`: The root object containing the change event
  - `before`: The state before the change (null for inserts)
  - `after`: The state after the change (null for deletes)
  - `source`: Metadata about the source of the change
    - `version`: Connector version
    - `connector`: Name of the connector
    - `name`: Logical name of the source
    - `ts_ms`: Timestamp of the change in milliseconds
    - `snapshot`: Whether this is from a snapshot
    - `db`: Database name
    - `schema`: Schema name
    - `table`: Table name
    - `txId`: Transaction ID
    - `lsn`: Log Sequence Number
    - `xmin`: Optional transaction XMIN
  - `op`: Operation type ("c"=create, "u"=update, "d"=delete)

## Shutdown

The application handles graceful shutdown on SIGINT/SIGTERM:
1. Stops all replication streams
2. Flushes final LSN offsets to disk
3. Closes all database connections

## Error Handling & Recovery

The application implements several error handling and recovery mechanisms:

1. **Circuit Breaker Pattern**:
   - Each database connection has its own circuit breaker
   - After 5 consecutive failures, the circuit opens
   - After 5 minutes, enters half-open state for testing
   - Successful operations reset the circuit

2. **Automatic Reconnection**:
   - Automatic retry of failed database connections
   - Exponential backoff for retry attempts
   - Per-database error isolation
   - LSN tracking ensures no data loss during disconnects

3. **Runtime Recovery**:
   - Automatic handling of network interruptions
   - Recovery from database connection drops
   - Message parsing error handling
   - Standby status update retries

4. **Error Metrics**:
   - Tracks different types of errors
   - Monitors error rates
   - Provides error context in logs