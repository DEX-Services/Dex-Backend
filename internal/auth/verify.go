package auth

import (
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/ethereum/go-ethereum/crypto"
)

// VerifySignature checks that signature (hex, 65 bytes, EIP-191 personal_sign)
// over message recovers to the claimed Ethereum address.
func VerifySignature(address, message, signatureHex string) error {
	sigBytes, err := decodeHex(signatureHex)
	if err != nil {
		return fmt.Errorf("invalid signature encoding: %w", err)
	}
	if len(sigBytes) != 65 {
		return fmt.Errorf("invalid signature length")
	}
	if sigBytes[64] >= 27 {
		sigBytes[64] -= 27
	}

	hash := accounts_TextHash([]byte(message))
	pubKey, err := crypto.SigToPub(hash, sigBytes)
	if err != nil {
		return fmt.Errorf("signature recovery failed: %w", err)
	}

	recovered := crypto.PubkeyToAddress(*pubKey).Hex()
	if !strings.EqualFold(recovered, address) {
		return fmt.Errorf("signature does not match address")
	}
	return nil
}

func decodeHex(s string) ([]byte, error) {
	s = strings.TrimPrefix(s, "0x")
	return hex.DecodeString(s)
}

// accounts_TextHash reproduces go-ethereum's accounts.TextHash without pulling
// in the full accounts package: keccak256("\x19Ethereum Signed Message:\n" + len(msg) + msg).
func accounts_TextHash(data []byte) []byte {
	msg := fmt.Sprintf("\x19Ethereum Signed Message:\n%d%s", len(data), data)
	return crypto.Keccak256([]byte(msg))
}
