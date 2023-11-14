package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"time"

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

	db, err := pgx.Connect(ctx, "postgresql://localhost/db1")
	if err != nil {
		return
	}
	defer db.Close(ctx)

	var endBlock int
	row := db.QueryRow(ctx, `select relpages from pg_class where oid = 'table1'::regclass::oid`)
	err = row.Scan(&endBlock)
	if err != nil {
		return
	}

	startBlock := 0
	blocksPerIteration := 50
	maxTuplesPerBlock := 150

	for {
		var rows pgx.Rows
		rows, err = db.Query(ctx, `
				select id
				from table1
				where ctid = any (
					array(
    					select format('(%s, %s)', a, b)::tid
    					from generate_series($1::int, $2::int) a(a)
							cross join generate_series(0, $3) b(b)
					))
					and value = '100000'`,
			startBlock,
			startBlock+blocksPerIteration,
			maxTuplesPerBlock,
		)
		if err != nil {
			return
		}
		var id int
		for rows.Next() {
			err = rows.Scan(&id)
			if err != nil {
				return
			}

			slog.Info("found row", "id", id)
		}
		err = rows.Err()
		if err != nil {
			return
		}
		rows.Close()

		startBlock += blocksPerIteration
		if startBlock > endBlock {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	return
}
