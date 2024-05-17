package handle

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/mrkagelui/alfred/handle/heavy"
	"github.com/mrkagelui/alfred/handle/message"
	"github.com/mrkagelui/alfred/source"
)

// Handler contains all it needs to handle jobs.
type Handler struct {
	db  *pgxpool.Pool
	src *source.Store
	lg  *slog.Logger
	jbt time.Duration
	lt  heavy.Lifter      // pretending we are working on something laborious and error-prone
	msg message.Messenger // pretending we are sending messages
}

// NewHandler returns a new *Handler.
func NewHandler(db *pgxpool.Pool, src *source.Store, lg *slog.Logger, jbt time.Duration, lt heavy.Lifter, msg message.Messenger) *Handler {
	return &Handler{
		db:  db,
		src: src,
		lg:  lg,
		jbt: jbt,
		lt:  lt,
		msg: msg,
	}
}

const (
	concurrency = 20
	maxFailures = 6
)

// these are job statuses.
const (
	initialized = "INITIALIZED"
	generated   = "GENERATED"
	sending     = "SENDING"
	successful  = "SUCCESSFUL"
	failed      = "FAILED"
)

// Handle is the main process for handling all jobs.
func (h *Handler) Handle() error {
	ctx := context.Background()
	year, month := time.Now().Year(), int(time.Now().Month()) // assume this is a monthly process

	// first of all, try to fill jobs ("publish").
	if err := h.fillJobs(ctx, year, month); err != nil {
		return fmt.Errorf("filling jobs: %v", err)
	}

	// enter main loop
	for {

		// check if we should continue
		remaining, err := h.remaining(ctx, year, month)
		if err != nil {
			return fmt.Errorf("checking should continue: %v", err)
		}
		if !remaining {
			break
		}

		// start multiple goroutines to handle one job each
		wg := sync.WaitGroup{}
		wg.Add(concurrency)
		for i := 0; i < concurrency; i++ {
			// fetch only one row, lock it, work on it concurrently in a separate goroutine
			tx, err := h.db.Begin(ctx)
			if err != nil {
				return fmt.Errorf("starting tx: %v", err)
			}

			ctx, cancel := context.WithTimeout(ctx, h.jbt)
			go func(ctx context.Context, tx pgx.Tx) {
				defer cancel()
				defer wg.Done()
				h.handleJob(ctx, tx, year, month)
			}(ctx, tx)
		}
		wg.Wait()
	}

	return nil
}

// handleJob handles one job.
func (h *Handler) handleJob(ctx context.Context, tx pgx.Tx, year, month int) {
	defer tx.Rollback(context.Background())

	type job struct {
		ID     string
		UserID string
		Status string
		Year   int
		Month  int
		MsgKey *string
	}
	// locks one row for this goroutine to process. due to "skip locked", there is zero lock contention and hence
	// minimal performance penalty.
	rows, err := tx.Query(ctx, `SELECT id, user_id, status, year, month, msg_key
FROM jobs
WHERE year = $1
  AND month = $2
  AND status = ANY($3)
LIMIT 1 FOR UPDATE SKIP LOCKED`, year, month, []string{initialized, generated, sending})
	if err != nil {
		h.lg.Error("locking one row", slog.String("err", err.Error()))
		return
	}

	j, err := pgx.CollectOneRow(rows, pgx.RowToStructByPos[job])
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		// when there is no row locked, it could mean that all remaining rows are locked by others
		return
	case err != nil:
		h.lg.Error("getting row", slog.String("err", err.Error()))
		return
	}

	lg := h.lg.With(
		slog.String("job_id", j.ID),
		slog.String("user_id", j.UserID),
		slog.String("status", j.Status),
		slog.Int("year", j.Year),
		slog.Int("month", j.Month),
	)

	// different handling based on status of the job. think of these as "savepoint"s.
	// SQL "savepoint" isn't appropriate here because this process needs to survive crashes.
	// if the status updates are implemented as SQL savepoints, upon crash they will not be persisted.
	switch j.Status {
	case initialized:
		// fake heavy lifting
		result, err := h.lt.Lift(ctx, j.UserID, j.Year, j.Month)
		if err != nil {
			lg.Error("lifting", slog.String("err", err.Error()))
			if err := failJob(ctx, tx, j.ID, err); err != nil { // only count failure in the heavy lifting
				lg.Error("failing job", slog.String("err", err.Error()))
			}
			return
		}
		// persists heavy lifting results
		if _, err := tx.Exec(ctx, `INSERT INTO results (user_id, year, month, result)
VALUES ($1, $2, $3, $4)`, j.UserID, j.Year, j.Month, result); err != nil {
			lg.Error("inserting result", slog.String("err", err.Error()))
			return
		}
		// status update
		if _, err := tx.Exec(ctx, `UPDATE jobs SET status = $2 WHERE id = $1`, j.ID, generated); err != nil {
			lg.Error("inserting result", slog.String("err", err.Error()))
			return
		}
		if err := tx.Commit(ctx); err != nil {
			lg.Error("committing tx", slog.String("err", err.Error()))
			return
		}
	case generated:
		// "write-ahead log": persist the idempotence key in DB first, perform the actual sending in the next step.
		// if this is combined with the next step, it could lead to duplicate calling of the external API:
		// if this process crashes after generating the key and making the external API call, but before receiving
		// the response, upon retry it will generate a different key for the same job and make the call again.
		msgID := uuid.NewString()
		if _, err := tx.Exec(ctx, `UPDATE jobs SET status = $2, msg_key = $3 WHERE id = $1`, j.ID, sending, msgID); err != nil {
			lg.Error("generating msg key", slog.String("err", err.Error()))
			return
		}
		if err := tx.Commit(ctx); err != nil {
			lg.Error("committing tx", slog.String("err", err.Error()))
			return
		}
	case sending:
		if j.MsgKey == nil {
			// since in the previous step the update is atomic this is impossible for this code, but we don't want to panic.
			lg.Error("data inconsistent")
			if err := failJobNow(ctx, tx, j.ID, errors.New(`data inconsistent`)); err != nil {
				lg.Error("failing job now", slog.String("err", err.Error()))
				return
			}
		}
		var res string
		if err := tx.QueryRow(ctx, `SELECT result FROM results WHERE user_id = $1 AND year = $2 AND month = $3`, j.UserID, j.Year, j.Month).Scan(&res); err != nil {
			lg.Error("querying results", slog.String("err", err.Error()))
			return
		}
		// makes the API call. note that since the key is retrieved from DB, even if the process crashes, upon retry it
		// will get the same key, i.e., the dependent API will receive the same idempotence key and should not perform
		// duplicate processing.
		if err := h.msg.Message(ctx, *j.MsgKey, j.UserID, res, j.Year, j.Month); err != nil {
			lg.Error("sending message", slog.String("err", err.Error()))
			if err := failJob(ctx, tx, j.ID, err); err != nil {
				lg.Error("failing job", slog.String("err", err.Error()))
				return
			}
			return
		}
		if _, err := tx.Exec(ctx, `UPDATE jobs SET status = $2 WHERE id = $1`, j.ID, successful); err != nil {
			return
		}

		if err := tx.Commit(ctx); err != nil {
			lg.Error("committing tx", slog.String("err", err.Error()))
			return
		}
	default:
		if err := failJobNow(ctx, tx, j.ID, errors.New("wrong status")); err != nil {
			lg.Error("failing job now", slog.String("err", err.Error()))
			return
		}
	}
}

// failJobNow marks the job failed immediately.
func failJobNow(ctx context.Context, tx pgx.Tx, id string, e error) error {
	_, err := tx.Exec(ctx, `UPDATE jobs
SET status   = $2,
    last_err = $3
WHERE id = $1`, id, failed, e.Error())
	if err != nil {
		return fmt.Errorf("setting failed: %v", err)
	}
	// committing here since it must be the end of one processing.
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("committing: %v", err)
	}
	return nil
}

// failJob increases the failure count for a job, records the last error, and sets the status to failed
// if maxFailures is reached.
func failJob(ctx context.Context, tx pgx.Tx, id string, e error) error {
	_, err := tx.Exec(ctx, `
UPDATE jobs
SET failures = failures + 1,
    status   = (CASE WHEN failures = $2 THEN $3 ELSE status END),
    last_err = $4
WHERE id = $1`, id, maxFailures-1, failed, e.Error()) // maxFailures-1 because it's before the update.
	if err != nil {
		return fmt.Errorf("updating failures: %v", err)
	}
	// committing here since it must be the end of one processing.
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("committing: %v", err)
	}
	return nil
}

// fillJobs is analogous to job "publishing". it utilizes table locks, hence there won't be
// duplicate pulling and insertions. While this means all instances of this "publishers" need
// to wait for one another, this delay should be negligible compared to the actual processing.
func (h *Handler) fillJobs(ctx context.Context, year, month int) error {

	// since this is done by locking the jobs table, even if multiple
	// processes are executing it, it'd be safe.
	tx, err := h.db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin: %v", err)
	}
	defer tx.Rollback(ctx)

	// lock table so that no one else can read/write this
	if _, err := tx.Exec(ctx, `LOCK TABLE jobs IN ACCESS EXCLUSIVE MODE`); err != nil {
		return fmt.Errorf("locking: %v", err)
	}

	// check if there's existing jobs for this month
	var filled bool
	if err := tx.QueryRow(
		ctx,
		`SELECT EXISTS(SELECT 1 FROM jobs WHERE year = $1 AND month = $2)`,
		year,
		month,
	).Scan(&filled); err != nil {
		return fmt.Errorf("get filled: %v", err)
	}

	if filled { // if filled, exit
		return nil
	}

	userRows, err := h.src.Get(ctx)
	if err != nil {
		return fmt.Errorf("get users: %v", err)
	}

	// use copyFrom to bulk insert.
	if _, err := tx.CopyFrom(
		ctx,
		pgx.Identifier{"jobs"},
		[]string{"user_id", "year", "month", "status"},
		pgx.CopyFromFunc(func() ([]any, error) {
			if !userRows.Next() {
				return nil, nil
			}
			values, err := userRows.Values()
			if err != nil {
				return nil, err
			}
			return append(values, year, month, initialized), nil
		}),
	); err != nil {
		return fmt.Errorf("copying: %v", err)
	}

	return tx.Commit(ctx)
}

// remaining tells us if there's more job to be processed. It is used to determine if the main loop should continue.
// Note that even when the last batch of jobs are being processed, because of transaction isolation,
// this query will still see those jobs' previous status and won't return false.
func (h *Handler) remaining(ctx context.Context, year, month int) (bool, error) {
	var remaining bool
	// check if any record with status not "final", either successful or failed, exists.
	if err := h.db.QueryRow(
		ctx,
		`SELECT EXISTS(SELECT 1 FROM jobs WHERE year = $1 AND month = $2 AND status = ANY($3))`,
		year,
		month,
		[]string{initialized, generated, sending},
	).Scan(&remaining); err != nil {
		return false, err
	}
	return remaining, nil
}
