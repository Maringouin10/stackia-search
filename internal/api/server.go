// Package api is the embedded HTTP layer: a lightweight Fiber server that
// exposes the search API, a JSON stats endpoint, a live-stats WebSocket, and
// the two embedded HTML pages (search UI + admin dashboard). It holds no state
// of its own beyond references to the searcher and the metrics registry.
package api

import (
	"time"

	"github.com/gofiber/contrib/websocket"
	"github.com/gofiber/fiber/v2"

	"github.com/Maringouin10/stackia-search/internal/metrics"
	"github.com/Maringouin10/stackia-search/internal/search"
	"github.com/Maringouin10/stackia-search/web"
)

type Server struct {
	app      *fiber.App
	searcher *search.Searcher
	metrics  *metrics.Metrics

	indexHTML     []byte
	dashboardHTML []byte
}

func New(searcher *search.Searcher, m *metrics.Metrics) (*Server, error) {
	idx, err := web.FS.ReadFile("static/index.html")
	if err != nil {
		return nil, err
	}
	dash, err := web.FS.ReadFile("static/dashboard.html")
	if err != nil {
		return nil, err
	}

	app := fiber.New(fiber.Config{
		AppName:               "stackia-search",
		DisableStartupMessage: true,
		// Keep the front-end lean; search never needs large request bodies.
		BodyLimit:    16 << 10,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 15 * time.Second,
		// Reuse the standard JSON encoders; low overhead, no extra deps.
	})

	s := &Server{
		app:           app,
		searcher:      searcher,
		metrics:       m,
		indexHTML:     idx,
		dashboardHTML: dash,
	}
	s.routes()
	return s, nil
}

func (s *Server) routes() {
	s.app.Get("/", func(c *fiber.Ctx) error {
		c.Type("html")
		return c.Send(s.indexHTML)
	})
	s.app.Get("/dashboard", func(c *fiber.Ctx) error {
		c.Type("html")
		return c.Send(s.dashboardHTML)
	})

	s.app.Get("/healthz", func(c *fiber.Ctx) error { return c.SendString("ok") })

	api := s.app.Group("/api")
	api.Get("/search", s.handleSearch)
	api.Get("/stats", s.handleStats)

	// WebSocket upgrade gate + live stats stream.
	s.app.Use("/ws", func(c *fiber.Ctx) error {
		if websocket.IsWebSocketUpgrade(c) {
			return c.Next()
		}
		return fiber.ErrUpgradeRequired
	})
	s.app.Get("/ws/stats", websocket.New(s.handleStatsWS))
}

// Listen starts serving and blocks. Run it in its own goroutine.
func (s *Server) Listen(addr string) error {
	return s.app.Listen(addr)
}

// Shutdown gracefully drains in-flight requests.
func (s *Server) Shutdown() error {
	return s.app.ShutdownWithTimeout(5 * time.Second)
}
