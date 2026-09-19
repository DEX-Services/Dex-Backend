package chain

import (
	"context"
	"crypto/ecdsa"
	"errors"
	"fmt"
	"math/big"
	"strings"

	ethereum "github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
)

var ErrTxReverted = errors.New("vault transaction reverted")

type Signer struct {
	Client  *Client
	Auth    *bind.TransactOpts
	Address common.Address
	ChainID *big.Int
}

func NewSigner(client *Client, privateKeyHex string, chainID int64) (*Signer, error) {
	key, err := crypto.HexToECDSA(strings.TrimPrefix(privateKeyHex, "0x"))
	if err != nil {
		return nil, err
	}

	publicKey, ok := key.Public().(*ecdsa.PublicKey)
	if !ok {
		return nil, errors.New("invalid treasury private key")
	}
	address := crypto.PubkeyToAddress(*publicKey)

	cid := big.NewInt(chainID)
	auth, err := bind.NewKeyedTransactorWithChainID(key, cid)
	if err != nil {
		return nil, err
	}

	return &Signer{Client: client, Auth: auth, Address: address, ChainID: cid}, nil
}

// replaceFeeBumpPct is how much a replacement transaction's fee cap/tip must
// exceed the original by, per Geth's own txpool rule for accepting a
// same-nonce replacement (10%, rounded up here to 12.5% for headroom so a
// borderline bump isn't silently rejected by the node).
const replaceFeeBumpPct = 125

// priorSubmission carries a previously-submitted transaction's nonce and fee
// caps, when retrying a withdrawal whose transaction may still be pending —
// submitTx then reuses that SAME nonce with a bumped fee (a "speed up"/
// replace) instead of asking the chain for a fresh nonce, which would skip
// over the stuck one and leave it jammed forever (H4).
type priorSubmission struct {
	Nonce     uint64
	FeeCapWei *big.Int
	TipCapWei *big.Int
}

// onSubmit, when set, is called with the nonce/fee actually used for a
// transaction right before it's broadcast — the caller should persist these
// durably (see repo.LedgerRepo.SavePendingNonce) so a crash or timeout during
// the subsequent confirmation wait still leaves a record to replace from.
func (s *Signer) submitTx(ctx context.Context, to common.Address, data []byte, prior *priorSubmission, onSubmit func(nonce uint64, feeCapWei, tipCapWei string)) (string, error) {
	var nonce uint64
	if prior != nil {
		// Reuse the exact nonce already in flight rather than calling
		// PendingNonceAt, which counts the still-pending original as
		// occupying its slot and would return nonce+1 — advancing past the
		// stuck transaction instead of replacing it.
		nonce = prior.Nonce
	} else {
		pending, err := s.Client.ETH.PendingNonceAt(ctx, s.Address)
		if err != nil {
			return "", err
		}
		nonce = pending
	}

	msg := ethereum.CallMsg{From: s.Address, To: &to, Data: data}
	gasLimit, err := s.Client.ETH.EstimateGas(ctx, msg)
	if err != nil {
		return "", err
	}

	// DynamicFeeTx (EIP-1559) instead of LegacyTx: a fixed GasPrice from
	// SuggestGasPrice either overpays during calm conditions (that estimate
	// already bakes in a safety margin against being underpriced) or
	// underpays and gets stuck during a spike, with no way to bump a
	// LegacyTx's price after sending. GasFeeCap/GasTipCap let the network
	// charge only what's actually needed up to the cap, and — for the
	// nonce-replacement path below — resubmitting the SAME nonce with a
	// higher tip is the standard, wallet-compatible way to unstick a pending
	// tx, which a LegacyTx's single GasPrice field can't express as cleanly.
	tipCap, err := s.Client.ETH.SuggestGasTipCap(ctx)
	if err != nil {
		return "", err
	}
	head, err := s.Client.ETH.HeaderByNumber(ctx, nil)
	if err != nil {
		return "", err
	}
	feeCap := feeCapFor(head.BaseFee, tipCap)

	if prior != nil {
		// A replacement must strictly out-bid the original at the SAME nonce
		// (Geth's txpool rejects anything less than a ~10% bump) — take
		// whichever is higher: the network's current suggested fee, or the
		// prior fee bumped by replaceFeeBumpPct, so a replacement never gets
		// silently dropped by the node for being underpriced relative to
		// what's already sitting in the mempool.
		bumpedFeeCap := bumpFee(prior.FeeCapWei, replaceFeeBumpPct)
		bumpedTipCap := bumpFee(prior.TipCapWei, replaceFeeBumpPct)
		if bumpedFeeCap.Cmp(feeCap) > 0 {
			feeCap = bumpedFeeCap
		}
		if bumpedTipCap.Cmp(tipCap) > 0 {
			tipCap = bumpedTipCap
		}
	}

	tx := types.NewTx(&types.DynamicFeeTx{
		ChainID:   s.ChainID,
		Nonce:     nonce,
		To:        &to,
		Value:     big.NewInt(0),
		Gas:       gasLimit,
		GasTipCap: tipCap,
		GasFeeCap: feeCap,
		Data:      data,
	})

	signedTx, err := s.Auth.Signer(s.Auth.From, tx)
	if err != nil {
		return "", err
	}
	txHash := signedTx.Hash().Hex()

	if onSubmit != nil {
		onSubmit(nonce, feeCap.String(), tipCap.String())
	}

	if err := s.Client.ETH.SendTransaction(ctx, signedTx); err != nil {
		return "", err
	}

	receipt, err := bind.WaitMined(ctx, s.Client.ETH, signedTx)
	if err != nil {
		return txHash, err
	}
	if receipt.Status != types.ReceiptStatusSuccessful {
		return txHash, fmt.Errorf("%w: %s", ErrTxReverted, txHash)
	}
	return txHash, nil
}

// bumpFee returns wei*pct/100, used to compute a replacement transaction's
// higher fee cap/tip from the prior submission's.
func bumpFee(wei *big.Int, pct int64) *big.Int {
	bumped := new(big.Int).Mul(wei, big.NewInt(pct))
	return bumped.Div(bumped, big.NewInt(100))
}

// feeCapFor computes GasFeeCap = 2*baseFee + tipCap, the standard headroom
// formula (matches what go-ethereum's own transaction-pool helpers and most
// wallets use): base fee can at most 1.125x per block, so doubling it covers
// several blocks of increase before this tx would ever be underpriced,
// while the network still only ever actually charges baseFee+tip, not the
// cap itself.
func feeCapFor(baseFee, tipCap *big.Int) *big.Int {
	return new(big.Int).Add(new(big.Int).Mul(baseFee, big.NewInt(2)), tipCap)
}

// PendingTx is a previously-submitted, not-yet-confirmed transaction's nonce
// and fee, passed back into SubmitWithdrawal/SubmitWithdrawalApproval on a
// retry so the resubmission replaces it at the same nonce (H4) instead of
// skipping past it. Nil means "no prior submission — use a fresh nonce."
type PendingTx struct {
	Nonce     uint64
	FeeCapWei *big.Int
	TipCapWei *big.Int
}

// SubmitWithdrawal transfers USDC from the treasury signer wallet to the user
// and returns only after the transaction is mined successfully. If prior is
// non-nil (retrying a withdrawal whose earlier transaction may still be
// pending), the resubmission reuses prior's nonce with a bumped fee instead
// of deriving a fresh nonce. onSubmit, if set, is called with the nonce/fee
// actually used right before broadcast, so the caller can persist it durably
// before waiting for confirmation.
func (s *Signer) SubmitWithdrawal(ctx context.Context, userAddress string, amountRaw *big.Int, prior *PendingTx, onSubmit func(nonce uint64, feeCapWei, tipCapWei string)) (string, error) {
	data, err := s.Client.TokenABI.Pack("transfer", common.HexToAddress(userAddress), amountRaw)
	if err != nil {
		return "", err
	}
	return s.submitTx(ctx, s.Client.TokenAddress, data, toPrivate(prior), onSubmit)
}

func toPrivate(p *PendingTx) *priorSubmission {
	if p == nil {
		return nil
	}
	return &priorSubmission{Nonce: p.Nonce, FeeCapWei: p.FeeCapWei, TipCapWei: p.TipCapWei}
}

// SubmitWithdrawalApproval keeps the older audit-only path available for deployments
// whose vault contract has not yet been upgraded with withdrawToken.
//
// Removal criteria (Low-3): safe to delete once every deployed DexVault
// contract has the withdrawToken function this service actually uses
// (SubmitWithdrawal) — confirm no live deployment still relies on the
// record-approval-then-manual-execute flow this backs, then delete this
// function and the recordWithdrawalApproval ABI entry together. As of this
// writing it has no other caller anywhere in this codebase (grep-verified).
func (s *Signer) SubmitWithdrawalApproval(ctx context.Context, userAddress string, amountRaw *big.Int, prior *PendingTx, onSubmit func(nonce uint64, feeCapWei, tipCapWei string)) (string, error) {
	data, err := s.Client.VaultABI.Pack("recordWithdrawalApproval", common.HexToAddress(userAddress), amountRaw)
	if err != nil {
		return "", err
	}
	return s.submitTx(ctx, s.Client.VaultAddress, data, toPrivate(prior), onSubmit)
}
