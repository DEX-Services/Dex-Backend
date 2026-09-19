package auth

import (
	"fmt"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

type Claims struct {
	UserID        string `json:"uid"`
	WalletAddress string `json:"addr"`
	jwt.RegisteredClaims
}

type JWTIssuer struct {
	secret []byte
	ttl    time.Duration
}

func NewJWTIssuer(secret string, ttl time.Duration) *JWTIssuer {
	return &JWTIssuer{secret: []byte(secret), ttl: ttl}
}

// Issue mints a JWT whose jti is sessionID, so a session store keyed on the
// same id can later revoke this specific token (M8) instead of only being
// able to wait out its natural expiry.
func (j *JWTIssuer) Issue(userID, walletAddress, sessionID string) (string, time.Time, error) {
	expiresAt := time.Now().Add(j.ttl)
	claims := Claims{
		UserID:        userID,
		WalletAddress: walletAddress,
		RegisteredClaims: jwt.RegisteredClaims{
			ID:        sessionID,
			ExpiresAt: jwt.NewNumericDate(expiresAt),
			IssuedAt:  jwt.NewNumericDate(time.Now()),
		},
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	signed, err := token.SignedString(j.secret)
	return signed, expiresAt, err
}

// TTL exposes the issuer's configured token lifetime so callers can align a
// session store's TTL with it.
func (j *JWTIssuer) TTL() time.Duration {
	return j.ttl
}

func (j *JWTIssuer) Verify(tokenString string) (*Claims, error) {
	claims := &Claims{}
	token, err := jwt.ParseWithClaims(tokenString, claims, func(t *jwt.Token) (interface{}, error) {
		if _, ok := t.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method")
		}
		return j.secret, nil
	})
	if err != nil || !token.Valid {
		return nil, fmt.Errorf("invalid token")
	}
	return claims, nil
}
