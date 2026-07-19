package chain

import (
	"context"
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
	tokenLabel   = "USDC"
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
	// EngineOutbox durably queues engine credit syncs for deposits; nil falls
	// back to the non-durable async path.
	EngineOutbox *engineclient.Outbox
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
			Topics:    [][]common.Hash{{l.Client.DepositTopic, l.Client.LegacyDepositTopic}},
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

	// New-format events carry the token address as the second indexed topic;
	// legacy events don't, and are assumed to be the configured USDC token.
	label := tokenLabel
	if len(vLog.Topics) > 2 && vLog.Topics[0] == l.Client.DepositTopic {
		depositedToken := common.HexToAddress(vLog.Topics[2].Hex())
		if depositedToken != l.Client.TokenAddress {
			l.Log.Warn("deposit of unrecognized token ignored",
				"token", depositedToken.Hex(), "tx", vLog.TxHash.Hex(), "user", userAddr.Hex())
			return nil
		}
	}

	user, err := l.Users.FindOrCreate(ctx, userAddr.Hex(), "metamask")
	if err != nil {
		return err
	}

	if err := l.Ledger.InsertDeposit(ctx, user.ID, userAddr.Hex(), label, event.Amount.String(), vLog.TxHash.Hex()); err != nil {
		return err
	}

	if l.EngineOutbox != nil {
		l.EngineOutbox.EnqueueCredit(ctx, user.ID, label, event.Amount.String())
	} else {
		engineclient.Async("credit", func(ctx context.Context) error {
			return l.EngineClient.Credit(ctx, user.ID, label, event.Amount.String())
		})
	}
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
