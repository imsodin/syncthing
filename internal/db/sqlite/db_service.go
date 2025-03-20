package sqlite

import (
	"context"
	"time"

	"github.com/syncthing/syncthing/internal/db"
)

const (
	internalMetaPrefix = "dbsvc"
	lastMaintKey       = "lastMaint"
)

type Service struct {
	sdb                 *DB
	maintenanceInterval time.Duration
	internalMeta        *db.Typed
}

func newService(sdb *DB, maintenanceInterval time.Duration) *Service {
	return &Service{
		sdb:                 sdb,
		maintenanceInterval: maintenanceInterval,
		internalMeta:        db.NewTyped(sdb, internalMetaPrefix),
	}
}

func (s *Service) Serve(ctx context.Context) error {
	// Run periodic maintenance

	// Figure out when we last ran maintenance and schedule accordingly. If
	// it was never, do it now.
	lastMaint, _, _ := s.internalMeta.Time(lastMaintKey)
	nextMaint := lastMaint.Add(s.maintenanceInterval)
	wait := time.Until(nextMaint)
	if wait < 0 {
		wait = time.Minute
	}
	l.Debugln("Next periodic run in", wait)

	timer := time.NewTimer(wait)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
		}

		if err := s.periodic(ctx); err != nil {
			return wrap(err)
		}

		timer.Reset(s.maintenanceInterval)
		l.Debugln("Next periodic run in", s.maintenanceInterval)
		_ = s.internalMeta.PutTime(lastMaintKey, time.Now())
	}
}

func (s *Service) periodic(ctx context.Context) error {
	t0 := time.Now()
	l.Debugln("Periodic start")

	s.sdb.updateLock.Lock()
	defer s.sdb.updateLock.Unlock()

	t1 := time.Now()
	defer func() { l.Debugln("Periodic done in", time.Since(t1), "+", t1.Sub(t0)) }()

	if err := s.garbageCollectBlocklistsAndBlocksLocked(ctx); err != nil {
		return wrap(err)
	}

	_, _ = s.sdb.sql.ExecContext(ctx, `ANALYZE`)
	_, _ = s.sdb.sql.ExecContext(ctx, `PRAGMA optimize`)
	_, _ = s.sdb.sql.ExecContext(ctx, `PRAGMA incremental_vacuum`)
	_, _ = s.sdb.sql.ExecContext(ctx, `PRAGMA wal_checkpoint(TRUNCATE)`)

	return nil
}

func (s *Service) garbageCollectBlocklistsAndBlocksLocked(ctx context.Context) error {
	// Remove all blocklists not referred to by any files and, by extension,
	// any blocks not referred to by a blocklist. This is an expensive
	// operation when run normally, especially if there are a lot of blocks
	// to collect.
	//
	// We make this orders of magnitude faster by disabling foreign keys for
	// the transaction and doing the cleanup manually. This requires using
	// an explicit connection and disabling foreign keys before starting the
	// transaction. We make sure to clean up on the way out.

	conn, err := s.sdb.sql.Connx(ctx)
	if err != nil {
		return wrap(err)
	}
	defer conn.Close()

	if _, err := conn.ExecContext(ctx, `PRAGMA foreign_keys = 0`); err != nil {
		return wrap(err)
	}
	defer func() { //nolint:contextcheck
		_, _ = conn.ExecContext(context.Background(), `PRAGMA foreign_keys = 1`)
	}()

	tx, err := conn.BeginTxx(ctx, nil)
	if err != nil {
		return wrap(err)
	}
	defer tx.Rollback() //nolint:errcheck

	if res, err := tx.ExecContext(ctx, `
		DELETE FROM blocklists
		WHERE NOT EXISTS (
			SELECT 1 FROM files WHERE files.blocklist_hash = blocklists.blocklist_hash
		)`); err != nil {
		return wrap(err, "delete blocklists")
	} else if shouldDebug() {
		rows, err := res.RowsAffected()
		l.Debugln("Blocklist GC:", rows, err)
	}

	if res, err := tx.ExecContext(ctx, `
		DELETE FROM blocks
		WHERE NOT EXISTS (
			SELECT 1 FROM blocklists WHERE blocklists.blocklist_hash = blocks.blocklist_hash
		)`); err != nil {
		return wrap(err, "delete blocks")
	} else if shouldDebug() {
		rows, err := res.RowsAffected()
		l.Debugln("Blocks GC:", rows, err)
	}

	return wrap(tx.Commit())
}
