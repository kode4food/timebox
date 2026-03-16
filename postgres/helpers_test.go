package postgres_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"

	"github.com/kode4food/timebox"
	"github.com/kode4food/timebox/postgres"
)

func withTestDatabase(
	t *testing.T, fn func(context.Context, postgres.Config),
) {
	t.Helper()

	ctx := context.Background()
	adminCfg, err := pgxpool.ParseConfig(postgres.DefaultURL)
	if !assert.NoError(t, err) {
		return
	}

	admin, err := pgxpool.NewWithConfig(ctx, adminCfg)
	if err != nil {
		t.Skipf("postgres unavailable: %v", err)
	}
	defer admin.Close()

	if err := admin.Ping(ctx); err != nil {
		t.Skipf("postgres unavailable: %v", err)
	}

	dbName := fmt.Sprintf(
		"timebox_test_%d", time.Now().UnixNano(),
	)
	_, err = admin.Exec(ctx,
		"CREATE DATABASE "+pgx.Identifier{dbName}.Sanitize(),
	)
	if !assert.NoError(t, err) {
		return
	}

	defer func() {
		_, _ = admin.Exec(ctx, `
			SELECT pg_terminate_backend(pid)
			FROM pg_stat_activity
			WHERE datname = $1
			  AND pid <> pg_backend_pid()
		`, dbName)
		_, _ = admin.Exec(ctx,
			"DROP DATABASE IF EXISTS "+
				pgx.Identifier{dbName}.Sanitize(),
		)
	}()

	cfg := postgres.DefaultConfig()
	cfg.URL = databaseURL(t, adminCfg.ConnString(), dbName)
	cfg.Prefix = "timebox-test"

	fn(ctx, cfg)
}

func databaseURL(t *testing.T, rawURL, dbName string) string {
	t.Helper()

	u, err := url.Parse(rawURL)
	if !assert.NoError(t, err) {
		return ""
	}
	u.Path = "/" + dbName
	return u.String()
}

func statusEnvIndexer(t *testing.T) timebox.Indexer {
	t.Helper()
	return func(evs []*timebox.Event) []*timebox.Index {
		res := make([]*timebox.Index, 0, len(evs))
		for _, ev := range evs {
			data := struct {
				Status string `json:"status"`
				Env    string `json:"env"`
			}{}
			if !assert.NoError(t,
				json.Unmarshal(ev.Data, &data),
			) {
				return nil
			}
			res = append(res, &timebox.Index{
				Status: &data.Status,
				Labels: map[string]string{
					"env": data.Env,
				},
			})
		}
		return res
	}
}

func testEvent(
	t *testing.T, at time.Time, status, env string, value int,
) *timebox.Event {
	t.Helper()

	data, err := json.Marshal(map[string]any{
		"status": status,
		"env":    env,
		"value":  value,
	})
	if !assert.NoError(t, err) {
		return nil
	}

	return &timebox.Event{
		Timestamp: at,
		Type:      "event.test",
		Data:      data,
	}
}
