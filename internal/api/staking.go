package api

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/dex/dex-backend/internal/repo"
)

// StakingServer is the authenticated user-facing BI2X staking API: 5% APR
// simple interest, no lock-up period, BI2X only. Derives the account from
// the wallet session exactly like TradeServer — the browser never supplies
// an account identifier.
type StakingServer struct {
	*Server
	Staking *repo.StakingRepo

	// acctLocks serializes stake/redeem per account — same reasoning and
	// shape as TradeServer.acctLocks (see that type's doc comment), kept as
	// a SEPARATE map here (not shared with TradeServer) so a burst of
	// staking requests from one account can never queue behind that
	// account's trading activity or vice versa — the two features have no
	// reason to serialize against each other. Strictly speaking,
	// StakingRepo's row-level locking (SELECT ... FOR UPDATE inside one
	// transaction) already prevents any double-spend on its own even
	// without this — this exists for the same fast-fail-429-instead-of-
	// queuing-behind-a-DB-lock UX reason TradeServer's does, not because
	// correctness strictly requires it.
	acctLocks   map[string]chan struct{}
	acctLocksMu sync.Mutex
}

func (s *StakingServer) claims(w http.ResponseWriter, r *http.Request) (string, bool) {
	claims, ok := s.authenticate(r)
	if !ok {
		writeError(w, http.StatusUnauthorized, "not authenticated")
		return "", false
	}
	return claims.UserID, true
}

func (s *StakingServer) acquireAccountSlot(ctx context.Context, accountID string) (release func(), ok bool) {
	s.acctLocksMu.Lock()
	ch, exists := s.acctLocks[accountID]
	if !exists {
		if s.acctLocks == nil {
			s.acctLocks = make(map[string]chan struct{})
		}
		ch = make(chan struct{}, 1)
		s.acctLocks[accountID] = ch
	}
	s.acctLocksMu.Unlock()

	timer := time.NewTimer(acctQueueWait)
	defer timer.Stop()
	select {
	case ch <- struct{}{}:
		return func() { <-ch }, true
	case <-timer.C:
		return nil, false
	case <-ctx.Done():
		return nil, false
	}
}

type stakeRequest struct {
	Amount string `json:"amount"`
}

// Stake handles POST /staking/stake: debits amount of BI2X from the
// caller's wallet balance and opens a new staking position for it.
func (s *StakingServer) Stake(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	accountID, ok := s.claims(w, r)
	if !ok {
		return
	}
	release, ok := s.acquireAccountSlot(r.Context(), accountID)
	if !ok {
		writeError(w, http.StatusTooManyRequests, "too many concurrent staking requests for this account; retry shortly")
		return
	}
	defer release()

	var req stakeRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	amountRaw, err := toRawUnits(strings.TrimSpace(req.Amount))
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	position, err := s.Staking.Stake(r.Context(), accountID, amountRaw)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"position": position})
}

type redeemRequest struct {
	PositionID string `json:"positionId"`
	Amount     string `json:"amount"` // human decimal; omit or leave empty to redeem the position's full remaining principal
}

// Redeem handles POST /staking/redeem: redeems all or part of an active
// position's principal, paying out that principal plus its authoritatively
// recalculated accrued interest to the caller's wallet balance.
func (s *StakingServer) Redeem(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	accountID, ok := s.claims(w, r)
	if !ok {
		return
	}
	release, ok := s.acquireAccountSlot(r.Context(), accountID)
	if !ok {
		writeError(w, http.StatusTooManyRequests, "too many concurrent staking requests for this account; retry shortly")
		return
	}
	defer release()

	var req redeemRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	req.PositionID = strings.TrimSpace(req.PositionID)
	if req.PositionID == "" {
		writeError(w, http.StatusBadRequest, "positionId is required")
		return
	}
	amount := strings.TrimSpace(req.Amount)
	var amountRaw string
	if amount == "" {
		// Full redeem: look up the position's current principal ourselves
		// rather than trusting a client-supplied "redeem everything" flag
		// that could race a concurrent partial redeem.
		positions, err := s.Staking.Positions(r.Context(), accountID)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "could not load staking position")
			return
		}
		found := false
		for _, p := range positions {
			if p.ID == req.PositionID {
				amountRaw = p.PrincipalRaw
				found = true
				break
			}
		}
		if !found {
			writeError(w, http.StatusNotFound, "staking position not found")
			return
		}
	} else {
		raw, err := toRawUnits(amount)
		if err != nil {
			writeError(w, http.StatusBadRequest, err.Error())
			return
		}
		amountRaw = raw
	}

	result, err := s.Staking.Redeem(r.Context(), accountID, req.PositionID, amountRaw)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"position":     result.Position,
		"principalRaw": result.PrincipalRaw,
		"interestRaw":  result.InterestRaw,
		"totalRaw":     result.TotalRaw,
	})
}

// Positions handles GET /staking/positions: every staking position (active
// and redeemed) for the caller.
func (s *StakingServer) Positions(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	accountID, ok := s.claims(w, r)
	if !ok {
		return
	}
	positions, err := s.Staking.Positions(r.Context(), accountID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "could not load staking positions")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"positions": positions})
}
