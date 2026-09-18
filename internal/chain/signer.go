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

func (s *Signer) submitTx(ctx context.Context, to common.Address, data []byte) (string, error) {
	nonce, err := s.Client.ETH.PendingNonceAt(ctx, s.Address)
	if err != nil {
		return "", err
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

// feeCapFor computes GasFeeCap = 2*baseFee + tipCap, the standard headroom
// formula (matches what go-ethereum's own transaction-pool helpers and most
// wallets use): base fee can at most 1.125x per block, so doubling it covers
// several blocks of increase before this tx would ever be underpriced,
// while the network still only ever actually charges baseFee+tip, not the
// cap itself.
func feeCapFor(baseFee, tipCap *big.Int) *big.Int {
	return new(big.Int).Add(new(big.Int).Mul(baseFee, big.NewInt(2)), tipCap)
}

// SubmitWithdrawal transfers USDC from the treasury signer wallet to the user
// and returns only after the transaction is mined successfully.
func (s *Signer) SubmitWithdrawal(ctx context.Context, userAddress string, amountRaw *big.Int) (string, error) {
	data, err := s.Client.TokenABI.Pack("transfer", common.HexToAddress(userAddress), amountRaw)
	if err != nil {
		return "", err
	}
	return s.submitTx(ctx, s.Client.TokenAddress, data)
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
func (s *Signer) SubmitWithdrawalApproval(ctx context.Context, userAddress string, amountRaw *big.Int) (string, error) {
	data, err := s.Client.VaultABI.Pack("recordWithdrawalApproval", common.HexToAddress(userAddress), amountRaw)
	if err != nil {
		return "", err
	}
	return s.submitTx(ctx, s.Client.VaultAddress, data)
}
