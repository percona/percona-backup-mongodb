package api

import (
	"fmt"
	"net/http"
)

type Config struct {
	Port int
}

// Server is the ctrl-agent's web API server.
type Server struct {
	errCh chan error
}

// Start serves api handler in a background goroutine.
func Start(cfg Config, h http.Handler) *Server {
	s := &Server{
		errCh: make(chan error, 1),
	}

	addr := fmt.Sprintf("0.0.0.0:%d", cfg.Port)
	go func() {
		s.errCh <- http.ListenAndServe(addr, h)
	}()

	return s
}

// Err delivers a a bind failure or a runtime error.
func (s *Server) Err() <-chan error {
	return s.errCh
}
