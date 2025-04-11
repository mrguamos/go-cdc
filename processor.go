package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"sync"
	"time"

	"github.com/jackc/pglogrepl"
)

var (
	publisher = NewDefaultEventPublisher()
)

// CleanupRelations removes old relation entries that haven't been used recently
func CleanupRelations() {
	// TODO: Implement cleanup logic based on last access time
	// This could be called periodically from a background goroutine
}

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
			LastAccessTime:  time.Now(), // Track last access time for cleanup
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
		dbzMsg, err := createDebeziumMessage(rel, nil, values, "c", cfg, time.Now(), 0)
		if err != nil {
			errorMetrics.IncrementError("processing")
			return fmt.Errorf("insert: error creating Debezium message for %s.%s: %w", rel.Namespace, rel.Name, err)
		}
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
		dbzMsg, err := createDebeziumMessage(rel, oldValues, newValues, "u", cfg, time.Now(), 0)
		if err != nil {
			errorMetrics.IncrementError("processing")
			return fmt.Errorf("update: error creating Debezium message for %s.%s: %w", rel.Namespace, rel.Name, err)
		}
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
		dbzMsg, err := createDebeziumMessage(rel, oldValues, nil, "d", cfg, time.Now(), 0)
		if err != nil {
			errorMetrics.IncrementError("processing")
			return fmt.Errorf("delete: error creating Debezium message for %s.%s: %w", rel.Namespace, rel.Name, err)
		}
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
		log.Printf("WARN: Received unknown message type: %T", msg)
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
func createDebeziumMessage(rel *Relation, before, after map[string]interface{}, op string, cfg *Config, commitTime time.Time, xid uint32) (*DebeziumMessage, error) {
	commitTsMillis := commitTime.UnixMilli()         // Use commit time from PG
	processedLSN := GetCurrentLSN(cfg.ConnectorName) // Get LSN at the time of processing this specific change

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
	}, nil
}

// outputJSON prints the Debezium message to stdout, handling potential errors.
func outputJSON(msg *DebeziumMessage) error {
	if msg == nil {
		return fmt.Errorf("cannot marshal nil message")
	}

	jsonData, err := json.Marshal(msg)
	if err != nil {
		errorMetrics.IncrementError("processing")
		return fmt.Errorf("failed to marshal Debezium message to JSON: %w", err)
	}

	if err := emitEvent(jsonData); err != nil {
		errorMetrics.IncrementError("publishing")
		return fmt.Errorf("failed to emit event: %w", err)
	}
	return nil
}

// EventPublisher defines the interface for publishing events
type EventPublisher interface {
	Publish(ctx context.Context, message []byte) error
	Close() error
	IsRetryableError(err error) bool
}

// DLQMessage represents a message in the dead letter queue
type DLQMessage struct {
	Message   []byte
	Timestamp time.Time
	Error     string
}

// DLQ defines the interface for dead letter queue implementations
type DLQ interface {
	// Store adds a message to the queue
	Store(message []byte) error
	// Get retrieves all messages from the queue
	Get() []*DLQMessage
	// Close cleans up resources
	Close() error
}

// DeadLetterQueue is an in-memory implementation of DLQ
type DeadLetterQueue struct {
	queue []*DLQMessage
	mu    sync.Mutex
}

// NewDeadLetterQueue creates a new in-memory dead letter queue
func NewDeadLetterQueue() *DeadLetterQueue {
	return &DeadLetterQueue{
		queue: make([]*DLQMessage, 0),
	}
}

// Store adds a message to the DLQ
func (d *DeadLetterQueue) Store(message []byte) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.queue = append(d.queue, &DLQMessage{
		Message:   message,
		Timestamp: time.Now(),
	})

	// TODO: Implement persistence to disk/database
	return nil
}

// Get retrieves all messages from the queue
func (d *DeadLetterQueue) Get() []*DLQMessage {
	d.mu.Lock()
	defer d.mu.Unlock()

	messages := make([]*DLQMessage, len(d.queue))
	copy(messages, d.queue)
	return messages
}

// Close implements the DLQ interface
func (d *DeadLetterQueue) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()

	// Clear the queue
	d.queue = nil
	return nil
}

// DefaultEventPublisher implements the EventPublisher interface
type DefaultEventPublisher struct {
	dlq DLQ
}

// NewDefaultEventPublisher creates a new DefaultEventPublisher
func NewDefaultEventPublisher() *DefaultEventPublisher {
	return &DefaultEventPublisher{
		dlq: NewDeadLetterQueue(),
	}
}

// Publish implements the EventPublisher interface
func (p *DefaultEventPublisher) Publish(ctx context.Context, message []byte) error {
	if err := p.publishToBroker(ctx, message); err != nil {
		// If publishing fails, store in DLQ
		if err := p.dlq.Store(message); err != nil {
			return fmt.Errorf("failed to store message in DLQ: %w", err)
		}
		return fmt.Errorf("failed to publish message: %w", err)
	}
	return nil
}

// Close implements the EventPublisher interface
func (p *DefaultEventPublisher) Close() error {
	return p.dlq.Close()
}

// publishToBroker publishes the event to the message broker
func (p *DefaultEventPublisher) publishToBroker(ctx context.Context, message []byte) error {
	// TODO: Implement actual broker publishing
	// For now, just log the event
	log.Printf("Publishing event: %+v", message)
	return nil
}

// calculateBackoff calculates the backoff duration
func calculateBackoff(attempt int, config *RetryConfig) time.Duration {
	backoff := config.InitialBackoff * time.Duration(math.Pow(config.BackoffMultiplier, float64(attempt)))
	if backoff > config.MaxBackoff {
		return config.MaxBackoff
	}
	return backoff
}

// emitEvent publishes an event with retry and error handling
func emitEvent(jsonData []byte) error {
	if jsonData == nil {
		return fmt.Errorf("cannot publish nil message")
	}

	// Use a shorter timeout for publishing
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := publisher.Publish(ctx, jsonData); err != nil {
		return fmt.Errorf("publish failed: %w", err)
	}
	return nil
}

// ClosePublisher closes the global publisher instance
func ClosePublisher() error {
	return publisher.Close()
}

// DebeziumMessage represents the structure of a Debezium-like message
type DebeziumMessage struct {
	Payload Payload `json:"message"`
}
