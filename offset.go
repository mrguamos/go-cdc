package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/jackc/pglogrepl"

	// "strconv" // Not needed if using LSN.String() and ParseLSN
	"strings"
	"sync/atomic"
)

// Atomically stores the current LSN that has been processed and can be flushed.
var currentLSN atomic.Value // Stores pglogrepl.LSN

// LoadInitialLSN reads the LSN from the offset file and verifies it with PostgreSQL.
// Returns 0 if the file doesn't exist or is invalid, or if the LSN has been recycled.
func LoadInitialLSN(filePath string) pglogrepl.LSN {
	data, err := os.ReadFile(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			log.Printf("Offset file '%s' not found, starting replication from scratch (LSN 0).", filePath)
			return 0
		}
		log.Printf("Error reading offset file '%s': %v. Starting fresh (LSN 0).", filePath, err)
		return 0
	}

	lsnStr := strings.TrimSpace(string(data))
	if lsnStr == "" {
		log.Printf("Offset file '%s' is empty, starting replication from scratch (LSN 0).", filePath)
		return 0
	}

	lsn, err := pglogrepl.ParseLSN(lsnStr)
	if err != nil {
		log.Printf("Error parsing LSN ('%s') from offset file '%s': %v. Starting fresh (LSN 0).", lsnStr, filePath, err)
		return 0
	}

	// Connect to PostgreSQL to verify the LSN
	cfg := LoadConfig()
	for _, dbConfig := range cfg.Databases {
		conn, err := ConnectDB(context.Background(), dbConfig.ConnStr)
		if err != nil {
			log.Printf("Failed to connect to database to verify LSN: %v", err)
			continue
		}
		defer conn.Close(context.Background())

		// Get the current WAL position
		query := "SELECT pg_current_wal_lsn()"
		mrr := conn.Exec(context.Background(), query)
		results, err := mrr.ReadAll()
		if err != nil {
			log.Printf("Failed to get current WAL position: %v", err)
			continue
		}

		if len(results) > 0 && len(results[0].Rows) > 0 {
			currentLSNStr := string(results[0].Rows[0][0])
			currentLSN, err := pglogrepl.ParseLSN(currentLSNStr)
			if err != nil {
				log.Printf("Failed to parse current WAL position: %v", err)
				continue
			}

			// If our saved LSN is older than the current WAL position, it's still valid
			if lsn <= currentLSN {
				log.Printf("Resuming replication from LSN %s found in '%s'", lsn, filePath)
				return lsn
			}

			log.Printf("Saved LSN %s is newer than current WAL position %s, starting from scratch", lsn, currentLSN)
			return 0
		}
	}

	// If we couldn't verify with any database, start from scratch
	log.Printf("Could not verify LSN with any database, starting from scratch (LSN 0)")
	return 0
}

// SaveLSN writes the current LSN to the offset file atomically.
func SaveLSN(filePath string, lsn pglogrepl.LSN) error {
	if lsn == 0 {
		// Avoid writing zero LSN if we haven't processed anything meaningful yet
		// log.Println("Skipping save of zero LSN.")
		return nil
	}

	lsnStr := lsn.String()
	log.Printf("Flushing LSN %s to %s", lsnStr, filePath)

	// Write to a temporary file first, then rename for atomicity
	tempFilePath := filePath + ".tmp"
	err := os.WriteFile(tempFilePath, []byte(lsnStr), 0644)
	if err != nil {
		return fmt.Errorf("failed to write temporary offset file '%s': %w", tempFilePath, err)
	}

	// Rename the temporary file to the actual offset file
	err = os.Rename(tempFilePath, filePath)
	if err != nil {
		// Clean up temp file on rename error
		_ = os.Remove(tempFilePath)
		return fmt.Errorf("failed to rename temporary offset file to '%s': %w", filePath, err)
	}

	return nil
}

// GetCurrentLSN safely retrieves the current processed LSN.
func GetCurrentLSN() pglogrepl.LSN {
	val := currentLSN.Load()
	if val == nil {
		return 0
	}
	lsn, ok := val.(pglogrepl.LSN)
	if !ok {
		// This indicates a programming error (storing wrong type)
		log.Printf("CRITICAL: Invalid type stored in atomic LSN. Expected pglogrepl.LSN, got %T", val)
		return 0
	}
	return lsn
}

// SetCurrentLSN safely updates the current processed LSN.
func SetCurrentLSN(lsn pglogrepl.LSN) {
	currentLSN.Store(lsn)
}
