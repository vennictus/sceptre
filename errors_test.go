package sceptre

import (
	"errors"
	"fmt"
	"testing"
)

func TestReadOnlyErrorsHaveTheirOwnCode(t *testing.T) {
	err := executionError(fmt.Errorf("insert: %w", ErrReadOnly))
	var e *Error
	if !errors.As(err, &e) || e.Code != CodeReadOnly || !errors.Is(err, ErrReadOnly) {
		t.Fatalf("read-only error = %#v", err)
	}
}
