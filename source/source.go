// Package source manages the dummy data source for jobs
package source

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Store contains necessary handle for managing data.
type Store struct {
	db *pgxpool.Pool
}

// NewStore returns a Store.
func NewStore(db *pgxpool.Pool) *Store {
	return &Store{db: db}
}

// Seed rinses the Store and insert n records.
func (s *Store) Seed(ctx context.Context, n int) error {
	if _, err := s.db.Exec(ctx, "TRUNCATE TABLE users"); err != nil {
		return fmt.Errorf("truncate: %v", err)
	}

	if _, err := s.db.Exec(ctx, `INSERT INTO users (status)
SELECT ('[0:1]={ACTIVE,DORMANT}'::TEXT[])[TRUNC(RANDOM() * 2)]
FROM GENERATE_SERIES(1, $1)
	`, n); err != nil {
		return fmt.Errorf("seeding: %v", err)
	}

	return nil
}

// Get returns all the fake data. Ignore package boundary and pretend it's ok to
// leak implementation detail and to return a pgx.Rows.
func (s *Store) Get(ctx context.Context) (pgx.Rows, error) {
	return s.db.Query(ctx, `SELECT id FROM users WHERE status='ACTIVE'`)
}

// CountActiveUsers returns the active user count.
func (s *Store) CountActiveUsers(ctx context.Context) (int, error) {
	var count int
	if err := s.db.QueryRow(ctx, `SELECT COUNT(*) FROM users WHERE status=$1`, "ACTIVE").Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}

// Close closes all handles Store holds
func (s *Store) Close() {
	s.db.Close()
}
