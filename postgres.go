package main

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

// Holds schema information for tables encountered
var relations = make(map[uint32]*Relation) // OID -> Relation Info

// ConnectDB establishes a connection to the PostgreSQL database.
func ConnectDB(ctx context.Context, connStr string) (*pgconn.PgConn, error) {
	connConfig, err := pgconn.ParseConfig(connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse connection string: %w", err)
	}
	// Ensure Replication mode is set for pgconn
	connConfig.RuntimeParams["replication"] = "database"

	conn, err := pgconn.ConnectConfig(ctx, connConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to database for replication: %w", err)
	}
	log.Println("Successfully connected to PostgreSQL in replication mode")
	return conn, nil
}

// EnsureReplicationSlot creates the replication slot if it doesn't exist.
func EnsureReplicationSlot(ctx context.Context, conn *pgconn.PgConn, slotName, outputPlugin string) (created bool, err error) {
	_, err = pglogrepl.CreateReplicationSlot(ctx, conn, slotName, outputPlugin, pglogrepl.CreateReplicationSlotOptions{Mode: pglogrepl.LogicalReplication})
	if err != nil {
		pgErr, ok := err.(*pgconn.PgError)
		// 42710 is duplicate_object
		if ok && pgErr.Code == "42710" {
			log.Printf("Replication slot '%s' already exists.", slotName)
			return false, nil // Slot already exists
		}
		return false, fmt.Errorf("failed to create replication slot '%s': %w", slotName, err)
	}
	log.Printf("Replication slot '%s' created successfully.", slotName)
	return true, nil
}

// DropInactiveReplicationSlots finds and drops inactive logical replication slots.
// BE CAREFUL: This might drop slots used by other tools. Consider adding a prefix check.
func DropInactiveReplicationSlots(ctx context.Context, conn *pgconn.PgConn, currentSlotName string) error {
	log.Println("Checking for inactive replication slots to drop...")
	// Query to find inactive logical slots. Add WHERE slot_name LIKE 'prefix_%' if needed.
	query := "SELECT slot_name FROM pg_replication_slots WHERE slot_type = 'logical' AND active = 'f'"
	mrr := conn.Exec(ctx, query)
	results, err := mrr.ReadAll()
	if err != nil {
		return fmt.Errorf("failed to query inactive replication slots: %w", err)
	}

	if len(results) == 0 || len(results[0].Rows) == 0 {
		log.Println("No inactive logical replication slots found.")
		return nil
	}

	for _, row := range results[0].Rows {
		if len(row) == 0 || row[0] == nil {
			continue // Skip empty rows/cells
		}
		slotNameToDrop := string(row[0])
		if slotNameToDrop == currentSlotName {
			log.Printf("Skipping drop of own slot '%s' (it might appear inactive briefly).", currentSlotName)
			continue
		}

		// Optional: Add prefix check here
		// if !strings.HasPrefix(slotNameToDrop, "my_cdc_prefix_") {
		//     log.Printf("Skipping drop of slot '%s' as it doesn't match the expected prefix.", slotNameToDrop)
		//     continue
		// }

		log.Printf("Attempting to drop inactive replication slot: %s", slotNameToDrop)
		// Use proper SQL escaping by doubling single quotes
		escapedSlotName := strings.ReplaceAll(slotNameToDrop, "'", "''")
		dropQuery := fmt.Sprintf("SELECT pg_drop_replication_slot('%s');", escapedSlotName)
		dropMrr := conn.Exec(ctx, dropQuery)
		_, err := dropMrr.ReadAll() // Execute and ignore result unless error
		if err != nil {
			// Log error but continue trying others
			log.Printf("WARN: Failed to drop inactive replication slot '%s': %v", slotNameToDrop, err)
		} else {
			log.Printf("Successfully dropped inactive replication slot: %s", slotNameToDrop)
		}
	}
	return nil
}

// StartReplicationStream starts the logical replication process.
func StartReplicationStream(ctx context.Context, conn *pgconn.PgConn, dbConfig DatabaseConfig, startLSN pglogrepl.LSN, messageProcessor func(msg pglogrepl.Message) error) error {
	log.Printf("Starting replication on slot '%s' from LSN %s", dbConfig.SlotName, startLSN)

	pluginArguments := []string{"proto_version '1'", fmt.Sprintf("publication_names '%s'", dbConfig.PublicationName)}
	err := pglogrepl.StartReplication(ctx, conn, dbConfig.SlotName, startLSN, pglogrepl.StartReplicationOptions{
		PluginArgs: pluginArguments,
	})
	if err != nil {
		return fmt.Errorf("failed to start replication: %w", err)
	}
	log.Println("Replication stream started successfully")

	// Use a separate timer for sending standby status updates
	standbyUpdateInterval := 10 * time.Second
	nextStandbyStatusUpdateTime := time.Now().Add(standbyUpdateInterval)
	clientLSN := startLSN
	lastProcessedLSN := startLSN // Track the last successfully processed LSN

	for {
		if ctx.Err() != nil {
			log.Println("Replication context cancelled, stopping stream.")
			return ctx.Err()
		}

		receiveCtx, cancelReceive := context.WithTimeout(ctx, standbyUpdateInterval)
		rawMsg, err := conn.ReceiveMessage(receiveCtx)
		cancelReceive()

		if err != nil {
			if pgconn.Timeout(err) || strings.Contains(err.Error(), "context deadline exceeded") {
				// do nothing
			} else if ctx.Err() != nil {
				log.Println("ReceiveMessage interrupted by main context cancellation.")
				return ctx.Err()
			} else {
				return fmt.Errorf("failed to receive replication message: %w", err)
			}
		}

		if rawMsg != nil {
			if errMsg, ok := rawMsg.(*pgproto3.ErrorResponse); ok {
				return fmt.Errorf("received PostgreSQL error: %+v", errMsg)
			}

			msg, ok := rawMsg.(*pgproto3.CopyData)
			if !ok {
				log.Printf("WARN: Received unexpected message type: %T", rawMsg)
				continue
			}

			switch msg.Data[0] {
			case pglogrepl.PrimaryKeepaliveMessageByteID:
				pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
				if err != nil {
					log.Printf("WARN: Failed to parse primary keepalive message: %v", err)
					continue
				}

				if pkm.ReplyRequested {
					nextStandbyStatusUpdateTime = time.Now()
				}

			case pglogrepl.XLogDataByteID:
				xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
				if err != nil {
					log.Printf("ERROR: Failed to parse XLogData: %v", err)
					continue
				}

				logicalMsg, err := pglogrepl.Parse(xld.WALData)
				if err != nil {
					log.Printf("ERROR: Failed to parse logical replication message: %v", err)
					continue
				}

				err = messageProcessor(logicalMsg)
				if err != nil {
					log.Printf("ERROR: Failed to process message at LSN %s: %v", xld.WALStart, err)
					continue
				}

				clientLSN = xld.WALStart + pglogrepl.LSN(len(xld.WALData))
				lastProcessedLSN = clientLSN
				SetCurrentLSN(clientLSN)

			default:
				log.Printf("WARN: Received unknown CopyData message type: %x", msg.Data[0])
			}
		}

		now := time.Now()
		if now.After(nextStandbyStatusUpdateTime) || (rawMsg != nil && rawMsg.(*pgproto3.CopyData).Data[0] == pglogrepl.PrimaryKeepaliveMessageByteID && mustReplyToKeepalive(rawMsg.(*pgproto3.CopyData).Data)) {
			if lastProcessedLSN > 0 {
				err = pglogrepl.SendStandbyStatusUpdate(ctx, conn, pglogrepl.StandbyStatusUpdate{
					WALWritePosition: lastProcessedLSN,
					WALFlushPosition: lastProcessedLSN,
					WALApplyPosition: lastProcessedLSN,
				})
				if err != nil {
					log.Printf("ERROR: Failed to send standby status update (LSN %s): %v", lastProcessedLSN, err)
					if ctx.Err() != nil {
						return ctx.Err()
					}
				}
			}
			nextStandbyStatusUpdateTime = now.Add(standbyUpdateInterval)
		}
	}
}

// Helper to check if keepalive requested reply
func mustReplyToKeepalive(data []byte) bool {
	if len(data) < 2 {
		return false
	} // Basic sanity check
	pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(data[1:])
	return err == nil && pkm.ReplyRequested
}

// EnsurePublication creates a publication for the specified tables if it doesn't exist.
func EnsurePublication(ctx context.Context, conn *pgconn.PgConn, publicationName string, tables []string) error {
	// First check if publication exists
	checkQuery := fmt.Sprintf("SELECT 1 FROM pg_publication WHERE pubname = '%s'", publicationName)
	mrr := conn.Exec(ctx, checkQuery)
	results, err := mrr.ReadAll()
	if err != nil {
		return fmt.Errorf("failed to check for existing publication: %w", err)
	}

	// If publication exists, drop it to ensure clean state
	if len(results) > 0 && len(results[0].Rows) > 0 {
		dropQuery := fmt.Sprintf("DROP PUBLICATION IF EXISTS %s", publicationName)
		_, err = conn.Exec(ctx, dropQuery).ReadAll()
		if err != nil {
			return fmt.Errorf("failed to drop existing publication: %w", err)
		}
	}

	// Create the publication
	var createQuery string
	if len(tables) == 0 {
		// If no tables specified, create for all tables
		createQuery = fmt.Sprintf("CREATE PUBLICATION %s FOR ALL TABLES", publicationName)
	} else {
		// Create for specific tables
		createQuery = fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s", publicationName, strings.Join(tables, ", "))
	}

	_, err = conn.Exec(ctx, createQuery).ReadAll()
	if err != nil {
		return fmt.Errorf("failed to create publication: %w", err)
	}

	log.Printf("Publication '%s' created successfully for tables: %v", publicationName, tables)
	return nil
}

// PerformInitialSnapshot performs a snapshot of the current table data
func PerformInitialSnapshot(ctx context.Context, conn *pgconn.PgConn, dbConfig DatabaseConfig) error {
	for _, table := range dbConfig.Tables {
		// Split table name into schema and table name
		parts := strings.Split(table, ".")
		if len(parts) != 2 {
			return fmt.Errorf("invalid table format: %s, expected schema.table", table)
		}
		schema, tableName := parts[0], parts[1]

		// Query to get all rows from the table
		query := fmt.Sprintf("SELECT * FROM %s.%s", schema, tableName)
		mrr := conn.Exec(ctx, query)
		results, err := mrr.ReadAll()
		if err != nil {
			return fmt.Errorf("failed to query table %s: %w", table, err)
		}

		// Process each row
		for _, result := range results {
			for _, row := range result.Rows {
				values := make(map[string]interface{})
				for i, col := range result.FieldDescriptions {
					values[col.Name] = string(row[i])
				}

				// Create a relation message for the table
				rel := &Relation{
					ID:        0, // Not used for snapshot
					Namespace: schema,
					Name:      tableName,
				}

				// Create and output Debezium message
				dbzMsg := createDebeziumMessage(rel, nil, values, "r", &Config{
					ConnectorName: dbConfig.PublicationName,
				}, time.Now(), 0)
				outputJSON(dbzMsg)
			}
		}
	}
	return nil
}
