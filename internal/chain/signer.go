package chain

import (
	"context"
	"crypto/ecdsa"
	"errors"
	"math/big"
	"strings"

	ethereum "github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
)

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

// SubmitWithdrawalApproval sends a recordWithdrawalApproval(user, amount) tx and returns its hash.
func (s *Signer) SubmitWithdrawalApproval(ctx context.Context, userAddress string, amountRaw *big.Int) (string, error) {
	data, err := s.Client.VaultABI.Pack("recordWithdrawalApproval", common.HexToAddress(userAddress), amountRaw)
	if err != nil {
		return "", err
	}

	nonce, err := s.Client.ETH.PendingNonceAt(ctx, s.Address)
	if err != nil {
		return "", err
	}

	gasPrice, err := s.Client.ETH.SuggestGasPrice(ctx)
	if err != nil {
		return "", err
	}

	msg := ethereum.CallMsg{From: s.Address, To: &s.Client.VaultAddress, Data: data}
	gasLimit, err := s.Client.ETH.EstimateGas(ctx, msg)
	if err != nil {
		return "", err
	}

	tx := types.NewTx(&types.LegacyTx{
		Nonce:    nonce,
		To:       &s.Client.VaultAddress,
		Value:    big.NewInt(0),
		Gas:      gasLimit,
		GasPrice: gasPrice,
		Data:     data,
	})

	signedTx, err := s.Auth.Signer(s.Auth.From, tx)
	if err != nil {
		return "", err
	}

	if err := s.Client.ETH.SendTransaction(ctx, signedTx); err != nil {
		return "", err
	}

	return signedTx.Hash().Hex(), nil
}
