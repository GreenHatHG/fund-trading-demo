package store

import (
	"crypto/rand"
	"encoding/hex"
)

func randomID() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}
