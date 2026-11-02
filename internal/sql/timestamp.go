package sql

import (
	"fmt"
	"time"

	"github.com/vennictus/sceptre/internal/table"
)

// timestampLayouts are the accepted timestamp literal forms. Forms without a
// zone are UTC.
var timestampLayouts = []string{
	time.RFC3339Nano,
	"2006-01-02T15:04:05.999999999",
	"2006-01-02 15:04:05.999999999Z07:00",
	"2006-01-02 15:04:05.999999999",
	"2006-01-02",
}

func parseTimestamp(s string) (time.Time, error) {
	for _, layout := range timestampLayouts {
		if t, err := time.Parse(layout, s); err == nil {
			return t.UTC(), nil
		}
	}
	return time.Time{}, fmt.Errorf("%w: invalid timestamp %q (use RFC 3339 or YYYY-MM-DD [HH:MM:SS])", table.ErrInvalidValue, s)
}
