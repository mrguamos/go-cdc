package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/jackc/pglogrepl" // Ensure pglogrepl is imported if used directly here, though it's mostly in postgres.go
)

var (
	errorMetrics = NewErrorMetrics()
)

func main() {
	log.SetOutput(os.Stdout)
	log.Println("Starting Go PostgreSQL CDC Tool...")

	cfg := LoadConfig()

	// Create a context that we'll use to coordinate shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle OS signals
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigChan
		log.Printf("Received signal: %v. Initiating shutdown...", sig)
		cancel()
	}()

	var wg sync.WaitGroup

	// Start a CDC process for each database
	for _, dbConfig := range cfg.Databases {
		wg.Add(1)
		go func(dbConfig DatabaseConfig) {
			defer wg.Done()

			// Create a unique offset file for each database
			offsetFile := fmt.Sprintf("%s_%s", dbConfig.SlotName, cfg.OffsetFile)

			// Load initial offset
			startLSN := LoadInitialLSN(offsetFile)
			SetCurrentLSN(startLSN)

			// Connect to database
			conn, err := ConnectDB(ctx, dbConfig.ConnStr)
			if err != nil {
				log.Printf("FATAL: Database connection failed for %s: %v", dbConfig.ConnStr, err)
				return
			}
			defer func() {
				log.Printf("Closing database connection for %s...", dbConfig.ConnStr)
				closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer closeCancel()
				conn.Close(closeCtx)
			}()

			// Drop stale slots if enabled
			if cfg.InactiveSlotCheck {
				err = DropInactiveReplicationSlots(ctx, conn, dbConfig.SlotName)
				if err != nil {
					log.Printf("WARN: Failed during inactive slot check for %s: %v", dbConfig.ConnStr, err)
				}
			}

			// Ensure replication slot exists
			_, err = EnsureReplicationSlot(ctx, conn, dbConfig.SlotName, "pgoutput")
			if err != nil {
				log.Printf("FATAL: Failed to ensure replication slot '%s' for %s: %v", dbConfig.SlotName, dbConfig.ConnStr, err)
				return
			}

			// Ensure publication exists
			err = EnsurePublication(ctx, conn, dbConfig.PublicationName, dbConfig.Tables)
			if err != nil {
				log.Printf("FATAL: Failed to ensure publication '%s' for %s: %v", dbConfig.PublicationName, dbConfig.ConnStr, err)
				return
			}

			// Only perform initial snapshot if no offset exists
			if startLSN == 0 {
				// Perform initial snapshot of tables
				if err := PerformInitialSnapshot(ctx, conn, dbConfig); err != nil {
					log.Printf("WARN: Failed to perform initial snapshot for %s: %v", dbConfig.ConnStr, err)
				}

				// Get the current LSN after snapshot
				query := "SELECT pg_current_wal_lsn()"
				mrr := conn.Exec(ctx, query)
				results, err := mrr.ReadAll()
				if err != nil {
					log.Printf("WARN: Failed to get current LSN for %s: %v", dbConfig.ConnStr, err)
				} else if len(results) > 0 && len(results[0].Rows) > 0 {
					lsnStr := string(results[0].Rows[0][0])
					lsn, err := pglogrepl.ParseLSN(lsnStr)
					if err != nil {
						log.Printf("WARN: Failed to parse LSN for %s: %v", dbConfig.ConnStr, err)
					} else {
						SetCurrentLSN(lsn)
						// Save initial offset
						if err := SaveLSN(offsetFile, lsn); err != nil {
							log.Printf("WARN: Failed to save initial offset for %s: %v", dbConfig.ConnStr, err)
						}
					}
				}
			}

			// Start periodic LSN flusher
			flusherWg := sync.WaitGroup{}
			flusherWg.Add(1)
			go func() {
				defer flusherWg.Done()
				ticker := time.NewTicker(cfg.FlushInterval)
				defer ticker.Stop()

				log.Printf("Starting periodic LSN flush every %v for %s", cfg.FlushInterval, dbConfig.ConnStr)
				for {
					select {
					case <-ticker.C:
						lsnToFlush := GetCurrentLSN()
						err := SaveLSN(offsetFile, lsnToFlush)
						if err != nil {
							log.Printf("ERROR: Periodic flush failed for %s: %v", dbConfig.ConnStr, err)
						}
					case <-ctx.Done():
						log.Printf("Stopping periodic LSN flusher for %s", dbConfig.ConnStr)
						return
					}
				}
			}()

			// Start replication stream
			err = StartReplicationStream(ctx, conn, dbConfig, GetCurrentLSN(), func(msg pglogrepl.Message) error {
				return ProcessWALMessage(msg, &Config{
					ConnectorName: dbConfig.PublicationName,
				})
			})
			if err != nil && err != context.Canceled {
				log.Printf("ERROR: Replication stream stopped for %s: %v", dbConfig.ConnStr, err)
			}

			// Wait for flusher to stop
			flusherWg.Wait()
		}(dbConfig)
	}

	// Start metrics server
	go func() {
		http.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(errorMetrics)
		})
		log.Println("Starting metrics server on :8080")
		if err := http.ListenAndServe(":8080", nil); err != nil {
			log.Printf("Metrics server error: %v", err)
		}
	}()

	// Wait for all database CDC processes to complete
	wg.Wait()
	log.Println("CDC tool shutdown complete.")
}
