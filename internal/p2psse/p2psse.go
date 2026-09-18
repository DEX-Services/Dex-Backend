// Package p2psse pushes P2P order updates to subscribed clients over
// Server-Sent Events (SSE), replacing the frontend's previous 5-second
// setInterval poll of GET /p2p/order (P2P-L2). SSE rather than a full
// WebSocket hub: Dex-Backend has no existing WS infrastructure at all
// (unlike matching-engine), and order-status pushes are one-directional
// (server -> client only) — SSE covers that over plain HTTP with no new
// dependency and far less code than standing up a WS upgrade/hub/auth path
// for a single one-way event type.
package p2psse

import (
	"encoding/json"
	"net/http"
	"sync"
	"time"
)

// Hub holds one subscriber channel set per order ID. Subscribers are
// removed automatically when their HTTP request ends (client disconnect or
// page navigation), so this never accumulates stale connections.
type Hub struct {
	mu   sync.Mutex
	subs map[string]map[chan []byte]struct{}
}

func NewHub() *Hub {
	return &Hub{subs: make(map[string]map[chan []byte]struct{})}
}

// Publish sends payload (typically the updated order, JSON-marshaled by the
// caller) to every subscriber currently watching orderID. Non-blocking: a
// slow/stuck subscriber is dropped rather than blocking the publisher,
// since a missed push just means that one client falls back to its next
// poll/reconnect rather than corrupting state.
func (h *Hub) Publish(orderID string, payload []byte) {
	h.mu.Lock()
	subs := h.subs[orderID]
	chans := make([]chan []byte, 0, len(subs))
	for ch := range subs {
		chans = append(chans, ch)
	}
	h.mu.Unlock()
	for _, ch := range chans {
		select {
		case ch <- payload:
		default:
		}
	}
}

// PublishOrder is a convenience wrapper: marshals v (the order) and
// publishes it under orderID, swallowing a marshal error since a push
// failure should never fail the HTTP request that triggered it.
func (h *Hub) PublishOrder(orderID string, v any) {
	b, err := json.Marshal(v)
	if err != nil {
		return
	}
	h.Publish(orderID, b)
}

func (h *Hub) subscribe(orderID string) chan []byte {
	ch := make(chan []byte, 4)
	h.mu.Lock()
	if h.subs[orderID] == nil {
		h.subs[orderID] = make(map[chan []byte]struct{})
	}
	h.subs[orderID][ch] = struct{}{}
	h.mu.Unlock()
	return ch
}

func (h *Hub) unsubscribe(orderID string, ch chan []byte) {
	h.mu.Lock()
	delete(h.subs[orderID], ch)
	if len(h.subs[orderID]) == 0 {
		delete(h.subs, orderID)
	}
	h.mu.Unlock()
}

// ServeSSE streams every Publish for orderID to w as an SSE `data:` event,
// until the client disconnects (ctx.Done, tied to the request lifetime) or
// the response writer stops supporting flushing. Caller is responsible for
// auth (verifying the requester owns orderID) before calling this.
func (h *Hub) ServeSSE(w http.ResponseWriter, r *http.Request, orderID string) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	// The server sets a blanket WriteTimeout for ordinary request/response
	// handlers; applied to a long-lived SSE connection as-is, it would kill
	// this stream every WriteTimeout regardless of activity. Clearing the
	// deadline here scopes that back to just this one connection instead of
	// loosening it server-wide — the request's own context (tied to client
	// disconnect) is still what actually ends this loop.
	rc := http.NewResponseController(w)
	_ = rc.SetWriteDeadline(time.Time{})

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	ch := h.subscribe(orderID)
	defer h.unsubscribe(orderID, ch)

	// Periodic comment-only heartbeat: keeps intermediate proxies/load
	// balancers (which may have their own idle-connection timeouts
	// independent of this server's) from silently dropping a connection
	// that's simply waiting for the next real order update, which can be
	// minutes or longer between order-status changes.
	heartbeat := time.NewTicker(20 * time.Second)
	defer heartbeat.Stop()

	for {
		select {
		case <-r.Context().Done():
			return
		case <-heartbeat.C:
			if _, err := w.Write([]byte(": keep-alive\n\n")); err != nil {
				return
			}
			flusher.Flush()
		case payload := <-ch:
			if _, err := w.Write([]byte("data: ")); err != nil {
				return
			}
			if _, err := w.Write(payload); err != nil {
				return
			}
			if _, err := w.Write([]byte("\n\n")); err != nil {
				return
			}
			flusher.Flush()
		}
	}
}
