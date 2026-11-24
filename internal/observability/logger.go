package observability

import (
	"io"
	"log/slog"
)

// NewLogger creates a structured JSON slog.Logger.
func NewLogger(output io.Writer, level slog.Level) *slog.Logger {
	if output == nil {
		output = io.Discard
	}
	return slog.New(slog.NewJSONHandler(output, &slog.HandlerOptions{
		Level: level,
	}))
}
