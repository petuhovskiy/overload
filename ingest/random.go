package ingest

import (
	"math/rand"
	"time"
)

// generateRandomRow creates a single row of random data for the table
func generateRandomRow() []interface{} {
	return []interface{}{
		rand.Intn(100000),           // tid
		rand.Intn(10000),            // bid
		rand.Intn(10000000),         // aid
		rand.Intn(1000000) - 500000, // delta (can be negative)
		time.Now().Add(-time.Duration(rand.Intn(30*24)) * time.Hour), // random timestamp within last 30 days
		randomString(22), // filler
	}
}

// randomString generates a random string of specified length
func randomString(length int) string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[rand.Intn(len(charset))]
	}
	return string(b)
}
