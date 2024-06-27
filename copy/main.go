package main

import (
	"context"
	"io"
	"log/slog"
	"os"
	"os/signal"

	"github.com/jackc/pgx/v5"
)

func main() {
	var err error
	defer func() {
		if err != nil {
			slog.Error("error", "error", err)
			os.Exit(1)
		}
	}()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, os.Kill)
	defer stop()

	db1, err := pgx.Connect(ctx, "postgresql://localhost/db1")
	if err != nil {
		return
	}
	defer db1.Close(ctx)

	db2, err := pgx.Connect(ctx, "postgresql://localhost/db2")
	if err != nil {
		return
	}
	defer db2.Close(ctx)

	r, w := io.Pipe()

	go func() {
		// reader
		_, err := db1.PgConn().CopyTo(ctx, w, `copy table1 to stdout binary`)
		if err != nil {
			slog.Error("error", "error", err)
			return
		}
		_ = w.Close()
	}()

	// writer
	_, err = db2.PgConn().CopyFrom(ctx, r, `copy table1 from stdin binary`)
	_ = r.Close()

	return
}
