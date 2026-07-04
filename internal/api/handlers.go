package api

import (
	"strconv"

	"github.com/gofiber/fiber/v2"
)

// handleSearch runs a query and returns the ranked results as JSON.
// GET /api/search?q=<query>&n=<count>
func (s *Server) handleSearch(c *fiber.Ctx) error {
	q := c.Query("q")
	if q == "" {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "missing q"})
	}
	n := 10
	if v := c.Query("n"); v != "" {
		if parsed, err := strconv.Atoi(v); err == nil && parsed > 0 {
			n = parsed
		}
	}

	resp, err := s.searcher.Query(c.UserContext(), q, n)
	if err != nil {
		return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
	}
	return c.JSON(resp)
}

// handleStats returns a one-shot metrics snapshot (the WebSocket streams the
// same shape continuously; this is the polling fallback).
func (s *Server) handleStats(c *fiber.Ctx) error {
	return c.JSON(s.metrics.Snapshot())
}
