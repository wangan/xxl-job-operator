package xxlclient

import (
	"errors"
	"fmt"
	"strings"
)

// AdminError is a typed error returned when XXL-JOB Admin replies to a management or OpenAPI call
// with a non-success ReturnT (including the HTTP 200 + {"code":500} envelope produced by
// WebExceptionResolver for server-side exceptions such as DB timeouts) (fixes R6-P0-1, R6-P2-11).
type AdminError struct {
	// Path is the Admin endpoint that produced the error (e.g., "/jobgroup/remove").
	Path string
	// HTTPStatus is the HTTP status code of the response (200 for ReturnT envelopes).
	HTTPStatus int
	// Code is the ReturnT.code value (0 when the response was not a ReturnT envelope).
	Code int
	// Msg is the ReturnT.msg value (may be empty: e.g. /jobgroup/remove on a missing id returns FAIL with msg=null).
	Msg string
}

func (e *AdminError) Error() string {
	return fmt.Sprintf("xxl-job admin %s failed (http=%d, code=%d): %s", e.Path, e.HTTPStatus, e.Code, e.Msg)
}

// AsAdminError extracts an *AdminError from err if present.
func AsAdminError(err error) (*AdminError, bool) {
	var ae *AdminError
	if errors.As(err, &ae) {
		return ae, true
	}
	return nil, false
}

// IsBusinessRejection reports whether err is an explicit ReturnT rejection from a reachable Admin
// (HTTP 200 with code != 200), as opposed to a transport / HTTP-level failure.
func IsBusinessRejection(err error) bool {
	ae, ok := AsAdminError(err)
	return ok && ae.HTTPStatus == 200 && ae.Code != 0 && ae.Code != SuccessCode
}

// IsTerminalCallbackRejection reports whether err is a terminal business rejection from XXL-JOB Admin's
// /api/callback endpoint indicating that the callback log item was already completed ("repeate callback" /
// "repeat callback") or no longer exists ("log item not found") (fixes R10-P1-2).
func IsTerminalCallbackRejection(err error) bool {
	if err == nil || !IsBusinessRejection(err) {
		return false
	}
	lower := strings.ToLower(err.Error())
	return strings.Contains(lower, "repeate callback") ||
		strings.Contains(lower, "repeat callback") ||
		strings.Contains(lower, "log item not found")
}
