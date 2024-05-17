package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/kelseyhightower/envconfig"
	"github.com/lmittmann/tint"

	"github.com/mrkagelui/alfred/handle"
	"github.com/mrkagelui/alfred/handle/heavy"
	"github.com/mrkagelui/alfred/handle/message"
)

func main() {
	if err := run(); err != nil {
		slog.Error("run failed", "err", err)
		os.Exit(1)
	}
	slog.Info("completed")
	os.Exit(0)
}

type config struct {
	SeedSize            int           `envconfig:"SEED_SIZE" default:"100"`
	JobTimeout          time.Duration `envconfig:"JOB_TIMEOUT" default:"3s"`
	HeavyLiftMaxDelay   time.Duration `envconfig:"HEAVY_LIFT_MAX_DELAY" default:"0s"`
	HeavyLiftFailChance int           `envconfig:"HEAVY_LIFT_FAIL_CHANCE" default:"0"`
	MsgMaxDelay         time.Duration `envconfig:"MSG_MAX_DELAY" default:"0s"`
	MsgFailChance       int           `envconfig:"MSG_FAIL_CHANCE" default:"0"`
}

func run() error {
	lg := slog.New(tint.NewHandler(os.Stdout, nil))

	var cfg config
	if err := envconfig.Process("", &cfg); err != nil {
		return fmt.Errorf(": %v", err)
	}
	ctx := context.Background()

	// --- seed the source DB with random users
	src, err := seed(ctx, cfg.SeedSize)
	if err != nil {
		return fmt.Errorf("seeding: %v", err)
	}
	defer src.Close()

	lg.Info("seeding done", slog.Int("seed_size", cfg.SeedSize))
	// --- finish seeding

	handlerDB, err := open(ctx, dbConfig{
		User:    "alfred",
		Pass:    "mast3r_wayne",
		Host:    "localhost",
		Port:    5440,
		Name:    "alfred",
		TLS:     false,
		MaxConn: 100,
	})
	if err != nil {
		return fmt.Errorf("open handler DB: %v", err)
	}
	defer handlerDB.Close()

	if _, err := handlerDB.Exec(ctx, `TRUNCATE jobs`); err != nil {
		return fmt.Errorf("truncating jobs: %v", err)
	}

	h := handle.NewHandler(
		handlerDB,
		src,
		lg,
		cfg.JobTimeout,
		heavy.Lifter{
			MaxDelay:          cfg.HeavyLiftMaxDelay,
			FailurePercentage: cfg.HeavyLiftFailChance,
		},
		message.Messenger{
			MaxDelay:          cfg.MsgMaxDelay,
			FailurePercentage: cfg.MsgFailChance,
		},
	)
	start := time.Now()
	defer func() {
		lg.Info("handle completed", slog.Duration("elapsed", time.Since(start)))

		var successful, failed int
		if err := handlerDB.QueryRow(
			ctx,
			`SELECT COUNT(*) FROM jobs WHERE year = $1 AND month = $2 AND status = $3`,
			time.Now().Year(),
			int(time.Now().Month()),
			"SUCCESSFUL",
		).Scan(&successful); err != nil {
			lg.Error("querying jobs successful", "err", err)
		}
		if err := handlerDB.QueryRow(
			ctx,
			`SELECT COUNT(*) FROM jobs WHERE year = $1 AND month = $2 AND status = $3`,
			time.Now().Year(),
			int(time.Now().Month()),
			"FAILED",
		).Scan(&failed); err != nil {
			lg.Error("querying jobs successful", "err", err)
		}
		lg.Info("jobs completed", slog.Int("successful", successful), slog.Int("failed", failed))

		c, err := src.CountActiveUsers(ctx)
		if err != nil {
			lg.Error("counting active users", "err", err)
		}
		if successful+failed != c {
			lg.Error("job count doesn't tally!", slog.Int("total", c))
			return
		}
		lg.Info("job count tally!", slog.Int("successful", successful), slog.Int("failed", failed), slog.Int("total", c))
	}()

	if err := h.Handle(); err != nil {
		return fmt.Errorf("handling: %v", err)
	}

	return nil
}
