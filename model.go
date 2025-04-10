package main

import "github.com/jackc/pglogrepl" // Need this for RelationMessageColumn

// Represents the source information in a Debezium-like event
type SourceInfo struct {
	Version   string `json:"version"`   // Version of the connector
	Connector string `json:"connector"` // Name of the connector
	Name      string `json:"name"`      // Logical name of the source database server/cluster
	TsMs      int64  `json:"ts_ms"`     // Time the change was committed on the source DB (milliseconds since epoch)
	Snapshot  string `json:"snapshot"`  // "true", "false", or "last" (for snapshot phase)
	DB        string `json:"db"`        // Database name (Consider making this dynamic)
	Schema    string `json:"schema"`    // Schema name
	Table     string `json:"table"`     // Table name
	TxID      int64  `json:"txId"`      // Transaction ID from Postgres (XID)
	LSN       uint64 `json:"lsn"`       // LSN of the WAL record causing the event
	Xmin      *int64 `json:"xmin"`      // Optional transaction XMIN value (usually null for streaming)
}

// Represents the main payload of a Debezium-like event
type Payload struct {
	Before map[string]interface{} `json:"before"` // State before the change (null for inserts)
	After  map[string]interface{} `json:"after"`  // State after the change (null for deletes)
	Source SourceInfo             `json:"source"` // Source metadata block
	Op     string                 `json:"op"`     // Operation: "c" (create), "u" (update), "d" (delete), "r" (read/snapshot), potentially "t" (truncate)
	TsMs   int64                  `json:"ts_ms"`  // Time the event was processed by the connector (milliseconds since epoch)
	// Transaction *TransactionMetadata `json:"transaction,omitempty"` // Optional block for transaction metadata (id, total_order, data_collection_order)
}

// Simple internal representation of relation (table) schema information.
// This is built from pglogrepl.RelationMessage.
type Relation struct {
	ID              uint32                            // Relation OID
	Namespace       string                            // Schema name
	Name            string                            // Table name
	Columns         []pglogrepl.RelationMessageColumn // Column definitions
	ReplicaIdentity uint8                             // From RelationMessage.ReplicaIdentity ('d'=default, 'n'=nothing, 'f'=full, 'i'=index)
}
