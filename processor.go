package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pglogrepl"
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

// EventPublisher defines the interface for publishing events
type EventPublisher interface {
	Publish(ctx context.Context, message []byte) error
	Close() error
	IsRetryableError(err error) bool
}

// DefaultEventPublisher implements EventPublisher interface
type DefaultEventPublisher struct {
	circuitBreaker *CircuitBreaker
	retryConfig    *RetryConfig
	dlq            *DeadLetterQueue
	metrics        *ErrorMetrics
}

// NewDefaultEventPublisher creates a new event publisher
func NewDefaultEventPublisher(retryConfig *RetryConfig, metrics *ErrorMetrics) *DefaultEventPublisher {
	return &DefaultEventPublisher{
		circuitBreaker: NewCircuitBreaker(), // Use default circuit breaker
		retryConfig:    retryConfig,
		dlq:            NewDeadLetterQueue(),
		metrics:        metrics,
	}
}

// Publish attempts to publish a message with retries and circuit breaker
func (p *DefaultEventPublisher) Publish(ctx context.Context, message []byte) error {
	if !p.circuitBreaker.CanProceed() {
		p.metrics.IncrementError("publisher_circuit_open")
		return fmt.Errorf("publisher circuit breaker is open")
	}

	var lastErr error
	for attempt := 0; attempt < p.retryConfig.MaxRetries; attempt++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			// Try to publish the message
			err := p.publishToBroker(ctx, message)
			if err == nil {
				p.circuitBreaker.RecordSuccess()
				return nil
			}

			lastErr = err
			p.metrics.IncrementError("publisher")

			// Check if error is retryable
			if !p.IsRetryableError(err) {
				break
			}

			// Calculate backoff
			backoff := calculateBackoff(attempt, p.retryConfig)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff):
				continue
			}
		}
	}

	// If we've exhausted all retries, send to DLQ
	if err := p.dlq.Store(message); err != nil {
		p.metrics.IncrementError("dlq")
		return fmt.Errorf("failed to store in DLQ after retries: %v (original error: %v)", err, lastErr)
	}

	p.circuitBreaker.RecordFailure()
	return fmt.Errorf("message sent to DLQ after %d retries: %v", p.retryConfig.MaxRetries, lastErr)
}

// IsRetryableError determines if an error is retryable for the default publisher
func (p *DefaultEventPublisher) IsRetryableError(err error) bool {
	// Default implementation considers network errors, timeouts, and temporary failures as retryable
	if err == nil {
		return false
	}

	// Network errors are retryable
	if strings.Contains(err.Error(), "connection") ||
		strings.Contains(err.Error(), "network") ||
		strings.Contains(err.Error(), "timeout") {
		return true
	}

	// Temporary failures are retryable
	if strings.Contains(err.Error(), "temporary") ||
		strings.Contains(err.Error(), "retry") {
		return true
	}

	// Rate limiting errors are retryable
	if strings.Contains(err.Error(), "rate limit") ||
		strings.Contains(err.Error(), "throttle") {
		return true
	}

	return false
}

// publishToBroker is the actual implementation of publishing to the broker
func (p *DefaultEventPublisher) publishToBroker(ctx context.Context, message []byte) error {
	// TODO: Implement actual broker publishing logic
	// This is where you'd integrate with Kafka, RabbitMQ, etc.
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

// DeadLetterQueue handles failed messages
type DeadLetterQueue struct {
	mu    sync.Mutex
	queue []*DLQMessage
}

// DLQMessage represents a message in the dead letter queue
type DLQMessage struct {
	Message   []byte
	Timestamp time.Time
	Error     string
}

// NewDeadLetterQueue creates a new DLQ
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

// emitEvent publishes an event with retry and error handling
func emitEvent(jsonData []byte) {
	// TODO: Initialize publisher with proper configuration
	publisher := NewDefaultEventPublisher(&RetryConfig{
		MaxRetries:        3,
		InitialBackoff:    1 * time.Second,
		MaxBackoff:        30 * time.Second,
		BackoffMultiplier: 2.0,
	}, errorMetrics)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := publisher.Publish(ctx, jsonData); err != nil {
		log.Printf("ERROR: Failed to publish event: %v", err)
	}
}

// DebeziumMessage represents the structure of a Debezium-like message
type DebeziumMessage struct {
	Payload Payload `json:"message"`
}
