// Package chain talks to the Avalanche Fuji DexVault contract: it watches for Deposit events
// and submits recordWithdrawalApproval transactions signed by the treasury key.
package chain

import (
	"context"
	"strings"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"
)

const dexVaultABIJSON = `[
	{"type":"event","name":"Deposit","inputs":[
		{"name":"user","type":"address","indexed":true},
		{"name":"amount","type":"uint256","indexed":false},
		{"name":"timestamp","type":"uint256","indexed":false}
	]},
	{"type":"function","name":"recordWithdrawalApproval","stateMutability":"nonpayable","inputs":[
		{"name":"user","type":"address"},
		{"name":"amount","type":"uint256"}
	],"outputs":[]}
]`

type Client struct {
	ETH          *ethclient.Client
	VaultAddress common.Address
	VaultABI     abi.ABI
	DepositTopic common.Hash
}

func NewClient(ctx context.Context, rpcURL, vaultAddress string) (*Client, error) {
	eth, err := ethclient.DialContext(ctx, rpcURL)
	if err != nil {
		return nil, err
	}

	parsedABI, err := abi.JSON(strings.NewReader(dexVaultABIJSON))
	if err != nil {
		return nil, err
	}

	return &Client{
		ETH:          eth,
		VaultAddress: common.HexToAddress(vaultAddress),
		VaultABI:     parsedABI,
		DepositTopic: parsedABI.Events["Deposit"].ID,
	}, nil
}
