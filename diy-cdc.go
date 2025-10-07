package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

type Change struct {
	Kind         string        `json:"kind"`
	Schema       string        `json:"schema"`
	Table        string        `json:"table"`
	ColumnNames  []string      `json:"columnnames"`
	ColumnValues []interface{} `json:"columnvalues"`
}

type WAL2JSONPayload struct {
	Change []Change `json:"change"`
}

func main() {
	connStr := "connection_string_here" // e.g. "host=localhost port=5432 user=postgres password=postgres dbname=mydb"

	ctx := context.Background()
	config, err := pgconn.ParseConfig(connStr)
	if err != nil {
		log.Fatalf("failed to parse config: %v", err)
	}

	// Important: set replication mode
	config.RuntimeParams["replication"] = "database"

	conn, err := pgconn.ConnectConfig(ctx, config)
	if err != nil {
		log.Fatalf("failed to connect: %v", err)
	}
	defer conn.Close(ctx)

	slotName := "slot_" + uuid.New().String()[:8]

	// Create replication slot with wal2json
	_, err = conn.Exec(ctx, fmt.Sprintf(
		"SELECT * FROM pg_create_logical_replication_slot('%s', 'wal2json')",
		slotName,
	)).ReadAll()
	if err != nil {
		log.Fatalf("failed to create slot: %v", err)
	}
	defer conn.Exec(ctx, fmt.Sprintf("SELECT pg_drop_replication_slot('%s')", slotName))

	// Start replication
	err = pglogrepl.StartReplication(ctx, conn, slotName, 0, pglogrepl.StartReplicationOptions{
		PluginArgs: []string{"pretty-print=0"},
	})
	if err != nil {
		log.Fatalf("failed to start replication: %v", err)
	}

	fmt.Println("🚀 Started CDC stream")

	standbyMessageTimeout := time.Second * 10
	nextStatusDeadline := time.Now().Add(standbyMessageTimeout)

	for {
		if time.Now().After(nextStatusDeadline) {
			_ = pglogrepl.SendStandbyStatusUpdate(ctx, conn, pglogrepl.StandbyStatusUpdate{})
			nextStatusDeadline = time.Now().Add(standbyMessageTimeout)
		}

		ctx, cancel := context.WithTimeout(ctx, time.Second*5)
		msg, err := conn.ReceiveMessage(ctx)
		cancel()
		if err != nil {
			continue
		}

		switch m := msg.(type) {
		case *pgproto3.CopyData:
			if m.Data[0] == pglogrepl.XLogDataByteID {
				xld, _ := pglogrepl.ParseXLogData(m.Data[1:])
				var payload WAL2JSONPayload
				if err := json.Unmarshal(xld.WALData, &payload); err == nil {
					for _, change := range payload.Change {
						fmt.Printf("Change detected: %s.%s %s\n",
							change.Schema, change.Table, change.Kind)
					}
				}
			}
		}
	}
}
