package main

import (
	"sync"
	"time"

	"github.com/jackc/pglogrepl" // Need this for RelationMessageColumn
)

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

// DatabaseInfo tracks information about a database connection
type DatabaseInfo struct {
	ConnStr         string    `json:"conn_str"`
	SlotName        string    `json:"slot_name"`
	PublicationName string    `json:"publication_name"`
	LastLSN         uint64    `json:"last_lsn"`
	Status          string    `json:"status"`
	LastError       string    `json:"last_error,omitempty"`
	ConnectedSince  time.Time `json:"connected_since"`
	CircuitState    string    `json:"circuit_state"`
}

// ErrorMetrics tracks error statistics for monitoring
type ErrorMetrics struct {
	TotalErrors      int64          `json:"total_errors"`
	DatabaseErrors   int64          `json:"database_errors"`
	ProcessingErrors int64          `json:"processing_errors"`
	FileErrors       int64          `json:"file_errors"`
	LastErrorTime    time.Time      `json:"last_error_time"`
	ErrorRate        float64        `json:"error_rate"`
	Databases        []DatabaseInfo `json:"databases"`
	mu               sync.RWMutex   // Protects all fields
}

// NewErrorMetrics creates a new ErrorMetrics instance
func NewErrorMetrics() *ErrorMetrics {
	return &ErrorMetrics{
		LastErrorTime: time.Now(),
	}
}

// IncrementError increments the appropriate error counter
func (em *ErrorMetrics) IncrementError(errorType string) {
	em.mu.Lock()
	defer em.mu.Unlock()

	em.TotalErrors++
	switch errorType {
	case "database":
		em.DatabaseErrors++
	case "processing":
		em.ProcessingErrors++
	case "file":
		em.FileErrors++
	}

	// Update error rate (errors per minute)
	now := time.Now()
	minutes := now.Sub(em.LastErrorTime).Minutes()
	if minutes > 0 {
		em.ErrorRate = float64(em.TotalErrors) / minutes
	}
	em.LastErrorTime = now
}

// GetMetrics returns a copy of current metrics
func (em *ErrorMetrics) GetMetrics() *ErrorMetrics {
	em.mu.RLock()
	defer em.mu.RUnlock()

	// Create a new instance and copy the values
	metrics := &ErrorMetrics{
		TotalErrors:      em.TotalErrors,
		DatabaseErrors:   em.DatabaseErrors,
		ProcessingErrors: em.ProcessingErrors,
		FileErrors:       em.FileErrors,
		LastErrorTime:    em.LastErrorTime,
		ErrorRate:        em.ErrorRate,
	}
	return metrics
}

// CircuitBreaker tracks failure counts and state for a database connection
type CircuitBreaker struct {
	FailureCount    int64
	LastFailureTime time.Time
	State           string // "closed", "open", "half-open"
	mu              sync.RWMutex
}

// NewCircuitBreaker creates a new CircuitBreaker instance
func NewCircuitBreaker() *CircuitBreaker {
	return &CircuitBreaker{
		State: "closed",
	}
}

// RecordFailure increments the failure count and updates the state
func (cb *CircuitBreaker) RecordFailure() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.FailureCount++
	cb.LastFailureTime = time.Now()

	if cb.FailureCount >= 5 { // After 5 failures, open the circuit
		cb.State = "open"
	}
}

// RecordSuccess resets the failure count and updates the state
func (cb *CircuitBreaker) RecordSuccess() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.FailureCount = 0
	cb.State = "closed"
}

// CanProceed checks if the circuit breaker allows the operation
func (cb *CircuitBreaker) CanProceed() bool {
	cb.mu.RLock()
	defer cb.mu.RUnlock()

	if cb.State == "closed" {
		return true
	}

	// If circuit is open, check if enough time has passed to try again
	if cb.State == "open" && time.Since(cb.LastFailureTime) > 5*time.Minute {
		cb.mu.Lock()
		cb.State = "half-open"
		cb.mu.Unlock()
		return true
	}

	return false
}

// GetState returns the current state of the circuit breaker
func (cb *CircuitBreaker) GetState() string {
	cb.mu.RLock()
	defer cb.mu.RUnlock()
	return cb.State
}
