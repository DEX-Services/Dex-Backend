// Package chain talks to Avalanche Fuji contracts: it watches DexVault Deposit events
// and submits treasury-signed USDC withdrawal transactions.
package chain

import (
	"context"
	"strings"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"
)

const dexVaultABIJSON = `[
	{"type":"event","name":"Deposit","inputs":[
		{"name":"user","type":"address","indexed":true},
		{"name":"token","type":"address","indexed":true},
		{"name":"amount","type":"uint256","indexed":false},
		{"name":"timestamp","type":"uint256","indexed":false}
	]},
	{"type":"function","name":"depositToken","stateMutability":"nonpayable","inputs":[
		{"name":"token","type":"address"},
		{"name":"amount","type":"uint256"}
	],"outputs":[]},
	{"type":"function","name":"withdrawToken","stateMutability":"nonpayable","inputs":[
		{"name":"token","type":"address"},
		{"name":"to","type":"address"},
		{"name":"amount","type":"uint256"}
	],"outputs":[]},
	{"type":"function","name":"recordWithdrawalApproval","stateMutability":"nonpayable","inputs":[
		{"name":"user","type":"address"},
		{"name":"token","type":"address"},
		{"name":"amount","type":"uint256"}
	],"outputs":[]}
]`

const erc20ABIJSON = `[
	{"type":"function","name":"transfer","stateMutability":"nonpayable","inputs":[
		{"name":"to","type":"address"},
		{"name":"amount","type":"uint256"}
	],"outputs":[{"name":"","type":"bool"}]},
	{"type":"function","name":"balanceOf","stateMutability":"view","inputs":[
		{"name":"account","type":"address"}
	],"outputs":[{"name":"","type":"uint256"}]}
]`

type Client struct {
	ETH          *ethclient.Client
	VaultAddress common.Address
	TokenAddress common.Address
	VaultABI     abi.ABI
	TokenABI     abi.ABI
	DepositTopic common.Hash
	// LegacyDepositTopic matches Deposit events emitted by vaults deployed
	// before the token address was added to the event. Kept so historical
	// logs (and not-yet-redeployed vaults) still index.
	LegacyDepositTopic common.Hash
}

func NewClient(ctx context.Context, rpcURL, vaultAddress, tokenAddress string) (*Client, error) {
	eth, err := ethclient.DialContext(ctx, rpcURL)
	if err != nil {
		return nil, err
	}

	parsedVaultABI, err := abi.JSON(strings.NewReader(dexVaultABIJSON))
	if err != nil {
		return nil, err
	}
	parsedTokenABI, err := abi.JSON(strings.NewReader(erc20ABIJSON))
	if err != nil {
		return nil, err
	}

	return &Client{
		ETH:                eth,
		VaultAddress:       common.HexToAddress(vaultAddress),
		TokenAddress:       common.HexToAddress(tokenAddress),
		VaultABI:           parsedVaultABI,
		TokenABI:           parsedTokenABI,
		DepositTopic:       parsedVaultABI.Events["Deposit"].ID,
		LegacyDepositTopic: crypto.Keccak256Hash([]byte("Deposit(address,uint256,uint256)")),
	}, nil
}
