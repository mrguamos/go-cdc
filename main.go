package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/jackc/pglogrepl" // Ensure pglogrepl is imported if used directly here, though it's mostly in postgres.go
)

var (
	errorMetrics = NewErrorMetrics()

	// Create a single HTTP server instance
	httpServer *http.Server
	serverMux  = http.NewServeMux()
	serverMu   sync.Mutex // Protect httpServer access
)

func init() {
	// Register the metrics handler once during package initialization
	serverMux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(errorMetrics)
	})
}

// acquireLock attempts to acquire a file lock
func acquireLock(lockFile string) (*os.File, error) {
	// Ensure the directory exists
	dir := filepath.Dir(lockFile)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create lock directory: %w", err)
	}

	// Open or create the lock file
	file, err := os.OpenFile(lockFile, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open lock file: %w", err)
	}

	// Try to acquire an exclusive lock
	err = syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	if err != nil {
		file.Close()
		if err == syscall.EWOULDBLOCK {
			return nil, fmt.Errorf("lock already held by another process")
		}
		return nil, fmt.Errorf("failed to acquire lock: %w", err)
	}

	// Write the PID to the lock file
	pid := os.Getpid()
	if _, err := file.WriteString(fmt.Sprintf("%d\n", pid)); err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to write PID to lock file: %w", err)
	}
	if err := file.Sync(); err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to sync lock file: %w", err)
	}

	return file, nil
}

// monitorActiveInstance checks if the active instance is still running
func monitorActiveInstance(lockFile string, ctx context.Context) bool {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return false
		case <-ticker.C:
			// Try to read the lock file
			file, err := os.Open(lockFile)
			if err != nil {
				// Lock file doesn't exist, active instance is down
				return true
			}

			// Read PID from lock file
			var pid int
			_, err = fmt.Fscanf(file, "%d", &pid)
			file.Close()

			if err != nil {
				// Invalid lock file, active instance is down
				return true
			}

			// Check if process is still running
			process, err := os.FindProcess(pid)
			if err != nil {
				// Process not found, active instance is down
				return true
			}

			// Send signal 0 to check if process exists
			err = process.Signal(syscall.Signal(0))
			if err != nil {
				// Process is dead, active instance is down
				return true
			}
		}
	}
}

func startHTTPServer(port string, mode string) {
	serverMu.Lock()
	defer serverMu.Unlock()

	// Shutdown existing server if it exists
	if httpServer != nil {
		log.Printf("Shutting down existing metrics server...")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := httpServer.Shutdown(shutdownCtx); err != nil {
			log.Printf("Error shutting down server: %v", err)
		}
		httpServer = nil
	}

	// Create listener on the specified port
	var listener net.Listener
	var err error
	if port == "0" {
		// Use a random available port
		listener, err = net.Listen("tcp", ":0")
		if err != nil {
			log.Printf("Failed to listen on random port: %v", err)
			return
		}
		port = strconv.Itoa(listener.Addr().(*net.TCPAddr).Port)
	} else {
		listener, err = net.Listen("tcp", ":"+port)
		if err != nil {
			log.Printf("Failed to listen on port %s: %v", port, err)
			return
		}
	}

	// Create and start new server
	httpServer = &http.Server{
		Addr:    ":" + port,
		Handler: serverMux,
	}

	log.Printf("Starting metrics server on :%s (%s mode)", port, mode)
	go func() {
		if err := httpServer.Serve(listener); err != nil && err != http.ErrServerClosed {
			log.Printf("Metrics server error: %v", err)
		}
	}()
}

func main() {
	log.SetOutput(os.Stdout)
	log.Println("Starting Go PostgreSQL CDC Tool...")

	cfg := LoadConfig()

	// Try to acquire the lock
	lockFile := filepath.Join(os.TempDir(), "cdc.lock")
	lock, err := acquireLock(lockFile)
	if err != nil {
		log.Printf("Failed to acquire lock: %v. Running in standby mode.", err)

		// Start metrics server in standby mode with random port
		go startHTTPServer("0", "standby")

		// Create context for monitoring
		monitorCtx, monitorCancel := context.WithCancel(context.Background())
		defer monitorCancel()

		// Keep monitoring in a loop
		for {
			if monitorActiveInstance(lockFile, monitorCtx) {
				log.Println("Active instance appears to be down. Attempting to take over...")
				monitorCancel() // Stop monitoring

				// Try to acquire lock again
				lock, err := acquireLock(lockFile)
				if err != nil {
					log.Printf("Failed to take over: %v", err)
					// Reset monitoring
					monitorCtx, monitorCancel = context.WithCancel(context.Background())
					continue
				}
				defer lock.Close()

				// Start active mode
				startActiveMode(cfg)
				return // Exit if startActiveMode returns
			}

			// Sleep for a bit before checking again
			time.Sleep(5 * time.Second)
		}
	}
	defer func() {
		lock.Close()
		os.Remove(lockFile)
	}()

	log.Printf("Acquired lock (PID: %d). Running in active mode.", os.Getpid())
	startActiveMode(cfg)
}

// startActiveMode contains the main active mode logic
func startActiveMode(cfg *Config) {
	// Create a context that we'll use to coordinate shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle OS signals
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Create a WaitGroup to track all flushers
	var flusherWg sync.WaitGroup

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
			SetCurrentLSN(dbConfig.ConnStr, startLSN)

			// Initialize database info
			dbInfo := DatabaseInfo{
				ConnStr:         dbConfig.ConnStr,
				SlotName:        dbConfig.SlotName,
				PublicationName: dbConfig.PublicationName,
				Status:          "connecting",
				ConnectedSince:  time.Now(),
			}

			// Add database info to metrics
			errorMetrics.mu.Lock()
			errorMetrics.Databases = append(errorMetrics.Databases, dbInfo)
			errorMetrics.mu.Unlock()

			// Connect to database
			conn, err := ConnectDB(ctx, dbConfig.ConnStr)
			if err != nil {
				log.Printf("FATAL: Database connection failed for %s: %v", dbConfig.ConnStr, err)
				errorMetrics.mu.Lock()
				for i, db := range errorMetrics.Databases {
					if db.ConnStr == dbConfig.ConnStr {
						errorMetrics.Databases[i].Status = "error"
						errorMetrics.Databases[i].LastError = err.Error()
						errorMetrics.Databases[i].CircuitState = circuitBreakers[dbConfig.ConnStr].GetState()
					}
				}
				errorMetrics.mu.Unlock()
				return
			}

			// Update database status
			errorMetrics.mu.Lock()
			for i, db := range errorMetrics.Databases {
				if db.ConnStr == dbConfig.ConnStr {
					errorMetrics.Databases[i].Status = "connected"
					errorMetrics.Databases[i].CircuitState = circuitBreakers[dbConfig.ConnStr].GetState()
				}
			}
			errorMetrics.mu.Unlock()

			defer func() {
				log.Printf("Closing database connection for %s...", dbConfig.ConnStr)
				closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer closeCancel()
				conn.Close(closeCtx)

				// Update database status
				errorMetrics.mu.Lock()
				for i, db := range errorMetrics.Databases {
					if db.ConnStr == dbConfig.ConnStr {
						errorMetrics.Databases[i].Status = "disconnected"
					}
				}
				errorMetrics.mu.Unlock()
			}()

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
						SetCurrentLSN(dbConfig.ConnStr, lsn)
						// Save initial offset
						if err := SaveLSN(offsetFile, lsn); err != nil {
							log.Printf("WARN: Failed to save initial offset for %s: %v", dbConfig.ConnStr, err)
						}
					}
				}
			}

			// Start periodic LSN flusher
			flusherWg.Add(1)
			go func() {
				defer flusherWg.Done()
				ticker := time.NewTicker(cfg.FlushInterval)
				defer ticker.Stop()

				log.Printf("Starting periodic LSN flush every %v for %s", cfg.FlushInterval, dbConfig.ConnStr)
				for {
					select {
					case <-ticker.C:
						lsnToFlush := GetCurrentLSN(dbConfig.ConnStr)
						err := SaveLSN(offsetFile, lsnToFlush)
						if err != nil {
							log.Printf("ERROR: Periodic flush failed for %s: %v", dbConfig.ConnStr, err)
						}
					case <-ctx.Done():
						// On shutdown, do one final flush
						lsnToFlush := GetCurrentLSN(dbConfig.ConnStr)
						if err := SaveLSN(offsetFile, lsnToFlush); err != nil {
							log.Printf("ERROR: Final flush failed for %s: %v", dbConfig.ConnStr, err)
						} else {
							log.Printf("Successfully flushed final LSN %s for %s", lsnToFlush, dbConfig.ConnStr)
						}
						return
					}
				}
			}()

			// Start replication stream
			err = StartReplicationStream(ctx, conn, dbConfig, GetCurrentLSN(dbConfig.ConnStr), func(msg pglogrepl.Message) error {
				// Update last LSN
				errorMetrics.mu.Lock()
				for i, db := range errorMetrics.Databases {
					if db.ConnStr == dbConfig.ConnStr {
						errorMetrics.Databases[i].LastLSN = uint64(GetCurrentLSN(dbConfig.ConnStr))
						errorMetrics.Databases[i].CircuitState = circuitBreakers[dbConfig.ConnStr].GetState()
					}
				}
				errorMetrics.mu.Unlock()

				return ProcessWALMessage(msg, &Config{
					ConnectorName: dbConfig.PublicationName,
				})
			})
			if err != nil && err != context.Canceled {
				log.Printf("ERROR: Replication stream stopped for %s: %v", dbConfig.ConnStr, err)
				errorMetrics.mu.Lock()
				for i, db := range errorMetrics.Databases {
					if db.ConnStr == dbConfig.ConnStr {
						errorMetrics.Databases[i].Status = "error"
						errorMetrics.Databases[i].LastError = err.Error()
						errorMetrics.Databases[i].CircuitState = circuitBreakers[dbConfig.ConnStr].GetState()
					}
				}
				errorMetrics.mu.Unlock()
			}
		}(dbConfig)
	}

	// Start metrics server
	go startHTTPServer("8080", "active")

	// Wait for all database CDC processes to complete
	wg.Wait()

	// Wait for all flushers to complete their final flush
	flusherWg.Wait()
	log.Println("CDC tool shutdown complete.")
}
