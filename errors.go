package config

import (
	"errors"
	"fmt"
)

var (
	// ErrNotFound indicates that the requested configuration was not found
	ErrNotFound = errors.New("configuration not found")

	// ErrInvalidPath indicates that the provided path is invalid
	ErrInvalidPath = errors.New("invalid configuration path")

	// ErrInvalidValue indicates that the provided value is invalid
	ErrInvalidValue = errors.New("invalid configuration value")

	// ErrNodeHasChildren indicates that the node cannot be deleted because it has children
	ErrNodeHasChildren = errors.New("cannot delete node with children")

	// ErrInvalidStructPtr indicates that the provided struct pointer is invalid
	ErrInvalidStructPtr = errors.New("invalid struct pointer")

	// ErrNilHandler indicates that a nil handler was provided
	ErrNilHandler = errors.New("handler cannot be nil")

	// ErrSubscriptionNotFound indicates that the subscription ID was not found
	ErrSubscriptionNotFound = errors.New("subscription not found")

	// ErrConnectionFailed indicates that the connection to ZooKeeper failed
	ErrConnectionFailed = errors.New("failed to connect to ZooKeeper")
)

// Error codes for better categorization of errors
const (
	// Error code prefixes
	ErrCodeNotFound     = "CFG-404" // Not found errors
	ErrCodeInvalidInput = "CFG-400" // Invalid input errors
	ErrCodeInternal     = "CFG-500" // Internal service errors
	ErrCodeConnection   = "CFG-503" // Connection-related errors
	ErrCodePermission   = "CFG-403" // Permission denied errors
)

// FormatError creates a standardized error message with a code prefix
func FormatError(code string, err error, context string) error {
	if err == nil {
		return nil
	}

	if context == "" {
		return fmt.Errorf("%s: %w", code, err)
	}

	return fmt.Errorf("%s: %s: %w", code, context, err)
}

// IsNotFound checks if an error is a not found error
func IsNotFound(err error) bool {
	return errors.Is(err, ErrNotFound)
}

// IsInvalidInput checks if an error is an invalid input error
func IsInvalidInput(err error) bool {
	return errors.Is(err, ErrInvalidPath) ||
		errors.Is(err, ErrInvalidValue) ||
		errors.Is(err, ErrInvalidStructPtr) ||
		errors.Is(err, ErrNilHandler)
}

// IsConnectionError checks if an error is a connection error
func IsConnectionError(err error) bool {
	return errors.Is(err, ErrConnectionFailed)
}
