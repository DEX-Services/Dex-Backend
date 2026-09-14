package chain

import (
	"context"
	"fmt"
	"log/slog"
	"math/big"
	"time"

	"github.com/dex/dex-backend/internal/engineclient"
	"github.com/dex/dex-backend/internal/repo"
	ethereum "github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	cursorKey    = "dexvault_deposit"
	pollInterval = 15 * time.Second
	// tokenLabel is the real on-chain asset the vault contract actually
	// receives — kept for an honest ledger audit trail (see InsertDeposit).
	tokenLabel = "USDC"
	// creditTokenLabel is what the deposit actually credits: the platform's
	// internal stable quote currency, pegged 1:1 to tokenLabel. Every
	// market's quote leg trades in BI2XUSD, not the raw deposited asset.
	creditTokenLabel = "BI2XUSD"
	// maxBlockRange stays under Fuji's public RPC eth_getLogs cap (2048 blocks per call).
	maxBlockRange = 2000
)

type Listener struct {
	Client       *Client
	Pool         *pgxpool.Pool
	Users        *repo.UserRepo
	Ledger       *repo.LedgerRepo
	Log          *slog.Logger
	StartBlock   uint64 // used only to seed chain_cursor on first run
	EngineClient *engineclient.Client
}

func (l *Listener) Run(ctx context.Context) {
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	for {
		if err := l.poll(ctx); err != nil {
			l.Log.Error("deposit listener poll failed", "err", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (l *Listener) poll(ctx context.Context) error {
	from, err := l.cursor(ctx)
	if err != nil {
		return err
	}

	latest, err := l.Client.ETH.BlockNumber(ctx)
	if err != nil {
		return err
	}

	for from <= latest {
		to := from + maxBlockRange - 1
		if to > latest {
			to = latest
		}

		logs, err := l.Client.ETH.FilterLogs(ctx, ethereum.FilterQuery{
			FromBlock: new(big.Int).SetUint64(from),
			ToBlock:   new(big.Int).SetUint64(to),
			Addresses: []common.Address{l.Client.VaultAddress},
			Topics:    [][]common.Hash{{l.Client.DepositTopic}},
		})
		if err != nil {
			return err
		}

		for _, vLog := range logs {
			if err := l.handleDeposit(ctx, vLog); err != nil {
				l.Log.Error("failed to process deposit log", "err", err, "tx", vLog.TxHash.Hex())
				continue
			}
		}

		if err := l.setCursor(ctx, to+1); err != nil {
			return err
		}
		from = to + 1
	}

	return nil
}

func (l *Listener) handleDeposit(ctx context.Context, vLog types.Log) error {
	userAddr := common.HexToAddress(vLog.Topics[1].Hex())

	var event struct {
		Amount    *big.Int
		Timestamp *big.Int
	}
	if err := l.Client.VaultABI.UnpackIntoInterface(&event, "Deposit", vLog.Data); err != nil {
		return err
	}

	// A deposit event is never a signup flow — an on-chain depositor without
	// an existing account gets one created here with no referral/affiliate
	// code, same as before this feature existed.
	user, err := l.Users.FindOrCreate(ctx, userAddr.Hex(), "metamask", "")
	if err != nil {
		return err
	}

	if err := l.Ledger.InsertDeposit(ctx, user.ID, userAddr.Hex(), tokenLabel, creditTokenLabel, event.Amount.String(), vLog.TxHash.Hex()); err != nil {
		return err
	}

	// event.Amount is the raw on-chain amount (6-decimal scale, same as this
	// platform's Postgres raw-unit convention — see InsertDeposit above,
	// which correctly stores it as-is). EngineClient.Credit expects a
	// HUMAN-decimal amount, not raw — passing event.Amount.String() directly
	// here inflated every deposit's engine-side balance by 1,000,000× (a
	// real "31.00004 tokens" deposit became "31,000,040" in the engine's
	// in-memory ledger while Postgres correctly showed the real amount).
	// See engineclient.RawToHumanUnits' doc comment for the full incident.
	humanAmount, err := engineclient.RawToHumanUnits(event.Amount.String())
	if err != nil {
		return fmt.Errorf("convert deposit amount for engine credit: %w", err)
	}
	engineclient.Async("credit", func(ctx context.Context) error {
		return l.EngineClient.Credit(ctx, user.ID, creditTokenLabel, humanAmount)
	})
	return nil
}

func (l *Listener) cursor(ctx context.Context) (uint64, error) {
	var block uint64
	err := l.Pool.QueryRow(ctx, `SELECT block_number FROM chain_cursor WHERE key = $1`, cursorKey).Scan(&block)
	if err == pgx.ErrNoRows {
		return l.StartBlock, nil
	}
	if err != nil {
		return 0, err
	}
	return block, nil
}

func (l *Listener) setCursor(ctx context.Context, block uint64) error {
	_, err := l.Pool.Exec(ctx,
		`INSERT INTO chain_cursor (key, block_number) VALUES ($1, $2)
		 ON CONFLICT (key) DO UPDATE SET block_number = $2`,
		cursorKey, block,
	)
	return err
}
