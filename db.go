package main

import (
	"context"
	"fmt"
	"net/url"

	"github.com/jackc/pgx/v5/pgxpool"
)

type dbConfig struct {
	User    string
	Pass    string
	Host    string
	Port    int
	Name    string
	TLS     bool
	MaxConn int
}

func open(ctx context.Context, c dbConfig) (*pgxpool.Pool, error) {
	sslMode := "disable"
	if c.TLS {
		sslMode = "require"
	}

	v := make(url.Values)
	v.Set("sslmode", sslMode)
	u := url.URL{
		Scheme:   "postgres",
		User:     url.UserPassword(c.User, c.Pass),
		Host:     fmt.Sprintf("%s:%d", c.Host, c.Port),
		Path:     c.Name,
		RawQuery: v.Encode(),
	}
	cfg, err := pgxpool.ParseConfig(u.String())
	if err != nil {
		return nil, fmt.Errorf("parsing config: %v", err)
	}
	cfg.MaxConns = int32(c.MaxConn)
	return pgxpool.NewWithConfig(ctx, cfg)
}
