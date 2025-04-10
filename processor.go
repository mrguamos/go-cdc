package main

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/jackc/pglogrepl"
)

// ProcessWALMessage decodes pgoutput messages and formats them into Debezium-like JSON.
func ProcessWALMessage(logicalMsg pglogrepl.Message, cfg *Config) error {
	switch msg := logicalMsg.(type) {
	case *pglogrepl.RelationMessage:
		// Cache relation/schema info
		log.Printf("Received Relation: ID=%d Namespace=%s Name=%s ReplicaIdentity=%c Columns=%d",
			msg.RelationID, msg.Namespace, msg.RelationName, msg.ReplicaIdentity, len(msg.Columns))
		columns := make([]pglogrepl.RelationMessageColumn, len(msg.Columns))
		for i, col := range msg.Columns {
			if col == nil {
				continue
			}
			columns[i] = *col
		}
		relations[msg.RelationID] = &Relation{
			ID:              msg.RelationID,
			Namespace:       msg.Namespace,
			Name:            msg.RelationName,
			Columns:         columns,
			ReplicaIdentity: msg.ReplicaIdentity,
		}

	case *pglogrepl.BeginMessage:
		// Transaction boundary information
		log.Printf("Begin TX: Final LSN: %s, Commit Time: %s, XID: %d", msg.FinalLSN, msg.CommitTime, msg.Xid)

	case *pglogrepl.CommitMessage:
		log.Printf("Commit TX: LSN: %s, Commit LSN: %s, Commit Time: %s", msg.CommitLSN, msg.CommitLSN, msg.CommitTime)

	case *pglogrepl.InsertMessage:
		rel, ok := relations[msg.RelationID]
		if !ok {
			errorMetrics.IncrementError("processing")
			return fmt.Errorf("insert: unknown relation ID %d, cannot process message", msg.RelationID)
		}
		values, err := parseTuple(rel, msg.Tuple)
		if err != nil {
			errorMetrics.IncrementError("processing")
			return fmt.Errorf("insert: error parsing tuple for %s.%s: %w", rel.Namespace, rel.Name, err)
		}
		dbzMsg := createDebeziumMessage(rel, nil, values, "c", cfg, time.Now(), 0)
		if err := outputJSON(dbzMsg); err != nil {
			errorMetrics.IncrementError("processing")
			return err
		}

	case *pglogrepl.UpdateMessage:
		rel, ok := relations[msg.RelationID]
		if !ok {
			errorMetrics.IncrementError("processing")
			return fmt.Errorf("update: unknown relation ID %d, cannot process message", msg.RelationID)
		}
		var oldValues map[string]interface{}
		var err error
		if msg.OldTuple != nil {
			oldValues, err = parseTuple(rel, msg.OldTuple)
			if err != nil {
				errorMetrics.IncrementError("processing")
				return fmt.Errorf("update: error parsing old tuple for %s.%s: %w", rel.Namespace, rel.Name, err)
			}
		}
		newValues, err := parseTuple(rel, msg.NewTuple)
		if err != nil {
			errorMetrics.IncrementError("processing")
			return fmt.Errorf("update: error parsing new tuple for %s.%s: %w", rel.Namespace, rel.Name, err)
		}
		dbzMsg := createDebeziumMessage(rel, oldValues, newValues, "u", cfg, time.Now(), 0)
		if err := outputJSON(dbzMsg); err != nil {
			errorMetrics.IncrementError("processing")
			return err
		}

	case *pglogrepl.DeleteMessage:
		rel, ok := relations[msg.RelationID]
		if !ok {
			errorMetrics.IncrementError("processing")
			return fmt.Errorf("delete: unknown relation ID %d, cannot process message", msg.RelationID)
		}
		var oldValues map[string]interface{}
		var err error
		if msg.OldTuple != nil {
			oldValues, err = parseTuple(rel, msg.OldTuple)
			if err != nil {
				errorMetrics.IncrementError("processing")
				return fmt.Errorf("delete: error parsing old tuple for %s.%s: %w", rel.Namespace, rel.Name, err)
			}
		}
		dbzMsg := createDebeziumMessage(rel, oldValues, nil, "d", cfg, time.Now(), 0)
		if err := outputJSON(dbzMsg); err != nil {
			errorMetrics.IncrementError("processing")
			return err
		}

	case *pglogrepl.TruncateMessage:
		log.Printf("Truncate: Relation IDs: %v, Options: %d", msg.RelationIDs, msg.Option)

	case *pglogrepl.TypeMessage:
		log.Printf("Type Message: Namespace=%s Name=%s", msg.Namespace, msg.Name)

	case *pglogrepl.OriginMessage:
		log.Printf("Origin Message: Commit LSN=%s Name=%s", msg.CommitLSN, msg.Name)

	default:
		log.Printf("Received unhandled logical replication message type: %T", logicalMsg)
	}
	return nil
}

// parseTuple decodes the tuple data based on the relation schema.
func parseTuple(rel *Relation, tupleData *pglogrepl.TupleData) (map[string]interface{}, error) {
	values := make(map[string]interface{}, len(rel.Columns))
	if tupleData == nil {
		return nil, nil
	}

	for i, colData := range tupleData.Columns {
		col := rel.Columns[i]
		switch colData.DataType {
		case pglogrepl.TupleDataTypeNull:
			values[col.Name] = nil
		case pglogrepl.TupleDataTypeToast:
			values[col.Name] = nil
		case pglogrepl.TupleDataTypeText:
			values[col.Name] = string(colData.Data)
		default:
			return nil, fmt.Errorf("unknown column data type '%c' for column %s", colData.DataType, col.Name)
		}
	}
	return values, nil
}

// createDebeziumMessage constructs the output message structure.
func createDebeziumMessage(rel *Relation, before, after map[string]interface{}, op string, cfg *Config, commitTime time.Time, xid uint32) *DebeziumMessage {
	commitTsMillis := commitTime.UnixMilli() // Use commit time from PG
	processedLSN := GetCurrentLSN()          // Get LSN at the time of processing this specific change

	// Basic source info population (enhance as needed)
	source := SourceInfo{
		Version:   "1.0", // Your connector version
		Connector: cfg.ConnectorName,
		Name:      fmt.Sprintf("%s_%s", cfg.ConnectorName, rel.Namespace), // Include schema name in source name
		TsMs:      commitTsMillis,                                         // Timestamp from the database commit time
		Snapshot:  "false",                                                // Assume streaming; handle snapshots separately if implemented
		DB:        rel.Namespace,                                          // Use schema name as database identifier
		Schema:    rel.Namespace,
		Table:     rel.Name,
		TxID:      int64(xid), // Transaction ID from Begin/Commit/DML message
		LSN:       uint64(processedLSN),
		Xmin:      nil, // Xmin is typically for snapshots, not usually in WAL stream messages
	}

	payload := Payload{
		Before: before, // Map of column names to values before the change
		After:  after,  // Map of column names to values after the change
		Source: source,
		Op:     op, // "c" (create), "u" (update), "d" (delete), "r" (read/snapshot), maybe "t" (truncate)
	}

	// Schema part is complex, often omitted for simplicity unless strict schema validation is needed downstream
	return &DebeziumMessage{
		Payload: payload,
	}
}

// outputJSON prints the Debezium message to stdout, handling potential errors.
func outputJSON(msg *DebeziumMessage) error {
	// Use compact JSON encoding for output efficiency
	jsonData, err := json.Marshal(msg)
	if err != nil {
		errorMetrics.IncrementError("processing")
		return fmt.Errorf("failed to marshal Debezium message for %s.%s (op %s) to JSON: %w",
			msg.Payload.Source.Schema, msg.Payload.Source.Table, msg.Payload.Op, err)
	}
	emitEvent(jsonData)
	return nil
}

func emitEvent(jsonData []byte) {
	// log is already thread-safe and includes timestamps
	log.Println(string(jsonData))
}

// DebeziumMessage represents the structure of a Debezium-like message
type DebeziumMessage struct {
	Payload Payload `json:"message"`
}
