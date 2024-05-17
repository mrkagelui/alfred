package main

import (
	"context"
	"fmt"

	"github.com/mrkagelui/alfred/source"
)

func seed(ctx context.Context, seedSize int) (*source.Store, error) {
	sourceDB, err := open(ctx, dbConfig{
		User:    "source",
		Pass:    "sup3r_secret",
		Host:    "localhost",
		Port:    5441,
		Name:    "source",
		TLS:     false,
		MaxConn: 3,
	})
	if err != nil {
		return nil, fmt.Errorf("open: %v", err)
	}

	s := source.NewStore(sourceDB)
	if err := s.Seed(ctx, seedSize); err != nil {
		return nil, fmt.Errorf("seed: %v", err)
	}
	return s, nil
}
