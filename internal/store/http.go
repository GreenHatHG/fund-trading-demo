package store

import (
	"encoding/json"
	"net/http"
)

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}

// Exported aliases.
func WriteJSON(w http.ResponseWriter, code int, v any) { writeJSON(w, code, v) }
func WriteErr(w http.ResponseWriter, code int, msg string) { writeErr(w, code, msg) }
