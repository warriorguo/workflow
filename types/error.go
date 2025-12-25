package types

import (
	"time"

	"github.com/juju/errors"
)

var (
	_ error = &RetryError{}
	_ error = &FatalError{}
)

func NewRetryError(otherErr error, backoff time.Duration) error {
	return &RetryError{baseError: newBaseErr(otherErr), Backoff: backoff}
}

func NewRetryErrorf(backoff time.Duration, format string, args ...interface{}) error {
	return NewRetryError(errors.Errorf(format, args...), backoff)
}

func NewFatalError(otherErr error) error {
	return &FatalError{baseError: newBaseErr(otherErr)}
}

func NewFatalErrorf(format string, args ...interface{}) error {
	return NewFatalError(errors.Errorf(format, args...))
}

func newBaseErr(otherErr error) *baseError {
	return &baseError{unwrapErr(otherErr)}
}

func unwrapErr(err error) error {
	if err == nil {
		return nil
	}
	if ue, ok := err.(wrappedErr); ok {
		return unwrapErr(ue.UnwrapLocal())
	}
	return err
}

type wrappedErr interface {
	UnwrapLocal() error
}

type baseError struct {
	BaseErr error
}

func (e *baseError) Error() string {
	return e.BaseErr.Error()
}

func (e *baseError) UnwrapLocal() error {
	return e.BaseErr
}

type RetryError struct {
	*baseError
	Backoff time.Duration
}

type FatalError struct {
	*baseError
}

type PauseError struct {
	*baseError
}
