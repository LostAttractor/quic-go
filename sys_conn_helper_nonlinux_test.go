//go:build !linux

package quic

import "errors"

var (
	errGSO          = errors.New("fake GSO error")
	errGSOEINVAL    = errors.New("fake GSO EINVAL error")
	errNotPermitted = errors.New("fake not permitted error")
)
