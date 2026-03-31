package domain

import "time"

// Status represents the result of a health check.
type Status string

const (
	StatusUp      Status = "UP"
	StatusDown    Status = "DOWN"
	StatusUnknown Status = "UNKNOWN"
)

// Check represents a single health-check task and its result.
type Check struct {
	// ID is a unique identifier for this check.
	ID string `json:"id"`

	// URL is the endpoint to be checked.
	URL string `json:"url"`

	// Status is the result of the last health check (UP, DOWN, UNKNOWN).
	Status Status `json:"status"`

	// StatusCode is the HTTP status code returned by the endpoint.
	StatusCode int `json:"status_code,omitempty"`

	// ResponseTimeMs is the round-trip latency in milliseconds.
	ResponseTimeMs int64 `json:"response_time_ms,omitempty"`

	// CheckedAt is the timestamp of when the check was performed.
	CheckedAt time.Time `json:"checked_at,omitempty"`

	// Error holds any error message returned during the check.
	Error string `json:"error,omitempty"`
}
