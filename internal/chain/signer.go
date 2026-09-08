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

	gasPrice, err := s.Client.ETH.SuggestGasPrice(ctx)
	if err != nil {
		return "", err
	}

	msg := ethereum.CallMsg{From: s.Address, To: &to, Data: data}
	gasLimit, err := s.Client.ETH.EstimateGas(ctx, msg)
	if err != nil {
		return "", err
	}

	tx := types.NewTx(&types.LegacyTx{
		Nonce:    nonce,
		To:       &to,
		Value:    big.NewInt(0),
		Gas:      gasLimit,
		GasPrice: gasPrice,
		Data:     data,
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
func (s *Signer) SubmitWithdrawalApproval(ctx context.Context, userAddress string, amountRaw *big.Int) (string, error) {
	data, err := s.Client.VaultABI.Pack("recordWithdrawalApproval", common.HexToAddress(userAddress), amountRaw)
	if err != nil {
		return "", err
	}
	return s.submitTx(ctx, s.Client.VaultAddress, data)
}
