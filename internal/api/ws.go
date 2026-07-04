package api

import (
	"time"

	"github.com/gofiber/contrib/websocket"
)

// handleStatsWS streams a metrics snapshot once per second for the live
// dashboard. A background reader drains control frames so a client disconnect
// is detected promptly and the goroutine exits (no connection leak).
func (s *Server) handleStatsWS(c *websocket.Conn) {
	defer c.Close()

	closed := make(chan struct{})
	go func() {
		defer close(closed)
		for {
			// ReadMessage returns an error on close; that's our exit signal.
			if _, _, err := c.ReadMessage(); err != nil {
				return
			}
		}
	}()

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	// Push an immediate first frame so the UI populates without a 1s wait.
	if err := c.WriteJSON(s.metrics.Snapshot()); err != nil {
		return
	}
	for {
		select {
		case <-closed:
			return
		case <-ticker.C:
			_ = c.SetWriteDeadline(time.Now().Add(5 * time.Second))
			if err := c.WriteJSON(s.metrics.Snapshot()); err != nil {
				return
			}
		}
	}
}
