package http3

import (
	"errors"
	"fmt"
	"testing"

	"github.com/daeuniverse/quic-go"
)

func TestErrorConversionPreservesScope(t *testing.T) {
	stream := &quic.StreamError{StreamID: 4, ErrorCode: quic.StreamErrorCode(ErrCodeRequestCanceled), Remote: true}
	connection := &quic.ApplicationError{ErrorCode: quic.ApplicationErrorCode(ErrCodeGeneralProtocolError), Remote: true}
	for _, original := range []error{stream, connection} {
		err := maybeReplaceError(fmt.Errorf("receive: %w", original))
		var h3err *Error
		if !errors.As(err, &h3err) || !errors.Is(err, original) {
			t.Fatalf("conversion lost original error: %v", err)
		}
		var gotStream *quic.StreamError
		var gotConnection *quic.ApplicationError
		if errors.As(err, &gotStream) != (original == stream) || errors.As(err, &gotConnection) != (original == connection) {
			t.Fatalf("conversion changed error scope: %v", err)
		}
	}
}
