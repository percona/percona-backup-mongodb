package api

import "net/http"

// newRouter builds the REST API routes.
func newRouter() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /status", handleStatus)
	return mux
}

func handleStatus(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	_, _ = w.Write([]byte("this is status"))
}
