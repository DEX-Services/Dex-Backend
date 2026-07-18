package repo

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/dex/dex-backend/internal/engineclient"
	"github.com/jackc/pgx/v5"
)

type p2pOutboxEvent struct {
	ID        string
	Direction string
	UserID    string
	Asset     string
	AmountRaw string
	Attempts  int
}

func (r *P2PRepo) claimOutboxEvent(ctx context.Context) (*p2pOutboxEvent, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var event p2pOutboxEvent
	err = tx.QueryRow(ctx, `SELECT e.id::text,e.direction,e.user_id,e.asset,e.amount_raw::text,e.attempts
		FROM p2p_engine_outbox e
		WHERE (
			(e.status IN ('pending','failed') AND e.next_attempt_at<=now())
			OR (e.status='processing' AND e.updated_at<now()-interval '2 minutes')
		)
		AND NOT EXISTS (
			SELECT 1 FROM p2p_engine_outbox earlier
			WHERE earlier.order_id=e.order_id AND earlier.sequence<e.sequence AND earlier.status<>'synced'
		)
		ORDER BY e.created_at,e.sequence
		FOR UPDATE SKIP LOCKED LIMIT 1`).
		Scan(&event.ID, &event.Direction, &event.UserID, &event.Asset, &event.AmountRaw, &event.Attempts)
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if _, err = tx.Exec(ctx, `UPDATE p2p_engine_outbox
		SET status='processing',attempts=attempts+1,updated_at=now() WHERE id=$1`, event.ID); err != nil {
		return nil, err
	}
	event.Attempts++
	if err = tx.Commit(ctx); err != nil {
		return nil, err
	}
	return &event, nil
}

func (r *P2PRepo) ProcessOutbox(ctx context.Context, engine *engineclient.Client, limit int) error {
	if engine == nil || !engine.Enabled() {
		return nil
	}
	for i := 0; i < limit; i++ {
		event, err := r.claimOutboxEvent(ctx)
		if err != nil {
			return err
		}
		if event == nil {
			return nil
		}
		err = engine.Sync(ctx, event.ID, event.UserID, event.Asset, event.AmountRaw, event.Direction)
		if err == nil {
			if _, updateErr := r.pool.Exec(ctx, `UPDATE p2p_engine_outbox
				SET status='synced',last_error=NULL,updated_at=now() WHERE id=$1`, event.ID); updateErr != nil {
				return updateErr
			}
			continue
		}
		delay := time.Second * time.Duration(1<<min(event.Attempts, 8))
		if _, updateErr := r.pool.Exec(ctx, `UPDATE p2p_engine_outbox
			SET status='failed',last_error=$2,next_attempt_at=now()+$3::interval,updated_at=now()
			WHERE id=$1`, event.ID, err.Error(), fmt.Sprintf("%d seconds", int(delay.Seconds()))); updateErr != nil {
			return updateErr
		}
	}
	return nil
}

func (r *P2PRepo) RunMaintenance(ctx context.Context, engine *engineclient.Client) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		if err := r.ExpirePendingOrders(ctx, 100); err != nil && ctx.Err() == nil {
			slog.Error("p2p order expiry failed", "error", err)
		}
		if err := r.ProcessOutbox(ctx, engine, 25); err != nil && ctx.Err() == nil {
			slog.Error("p2p engine outbox failed", "error", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
