package quic

import (
	"errors"
	"io"
	"testing"
	"time"

	"github.com/daeuniverse/quic-go/internal/monotime"
	"github.com/daeuniverse/quic-go/internal/protocol"
	"github.com/daeuniverse/quic-go/internal/wire"
	"go.uber.org/mock/gomock"
)

func TestReceiveStreamTryReadEOF(t *testing.T) {
	for _, kind := range []string{"open", "empty_fin", "unread_data", "gap", "late_fin", "local_cancel", "remote_reset", "connection_closed"} {
		t.Run(kind, func(t *testing.T) {
			var completed int
			sender := NewMockStreamSender(gomock.NewController(t))
			sender.EXPECT().onStreamCompleted(protocol.StreamID(42)).Do(func(protocol.StreamID) { completed++ }).AnyTimes()
			sender.EXPECT().onHasStreamControlFrame(gomock.Any(), gomock.Any()).AnyTimes()
			sender.EXPECT().onHasConnectionData().AnyTimes()
			stream := newReceiveStream(42, sender, newTestStreamFlowController(42))
			frame := func(f *wire.StreamFrame) {
				t.Helper()
				if err := stream.handleStreamFrame(f, monotime.Now()); err != nil {
					t.Fatal(err)
				}
			}
			switch kind {
			case "empty_fin":
				frame(&wire.StreamFrame{Fin: true})
			case "unread_data":
				frame(&wire.StreamFrame{Data: []byte("x"), Fin: true})
			case "gap":
				frame(&wire.StreamFrame{Offset: 1, Data: []byte("x"), Fin: true})
			case "late_fin":
				frame(&wire.StreamFrame{Data: []byte("x")})
				var data [1]byte
				if _, err := io.ReadFull(stream, data[:]); err != nil {
					t.Fatal(err)
				}
				if ended, err := stream.TryReadEOF(); ended || err != nil {
					t.Fatalf("missing FIN was guessed: %t, %v", ended, err)
				}
				frame(&wire.StreamFrame{Offset: 1, Fin: true})
			case "local_cancel":
				stream.CancelRead(17)
			case "remote_reset":
				if err := stream.handleResetStreamFrame(&wire.ResetStreamFrame{ErrorCode: 17, FinalSize: 0}, monotime.Now()); err != nil {
					t.Fatal(err)
				}
			case "connection_closed":
				stream.closeForShutdown(errors.New("connection closed"))
			}
			ended, err := stream.TryReadEOF()
			wantEnded := kind == "empty_fin" || kind == "late_fin"
			wantError := kind == "local_cancel" || kind == "remote_reset" || kind == "connection_closed"
			if ended != wantEnded || (err != nil) != wantError {
				t.Fatalf("end state: %t, %v; want ended=%t error=%t", ended, err, wantEnded, wantError)
			}
			if kind == "unread_data" {
				var data [1]byte
				if _, err := io.ReadFull(stream, data[:]); err != nil || data[0] != 'x' {
					t.Fatalf("probe consumed DATA: %q, %v", data, err)
				}
				wantEnded = true
			}
			if wantEnded {
				// A successful probe has the same stable EOF semantics as Read,
				// including a subsequent Close/CancelRead by an HTTP body owner.
				stream.CancelRead(17)
				_ = stream.SetReadDeadline(time.Now().Add(time.Second))
				var data [1]byte
				if n, err := stream.Read(data[:]); n != 0 || !errors.Is(err, io.EOF) {
					t.Fatalf("read after acknowledged EOF: %d, %v", n, err)
				}
				for range 2 {
					if ended, err := stream.TryReadEOF(); !ended || err != nil {
						t.Fatalf("EOF is not stable: %t, %v", ended, err)
					}
				}
				if completed != 1 {
					t.Fatalf("stream completion notifications: %d", completed)
				}
			} else if completed != 0 {
				t.Fatalf("incomplete stream released: %d", completed)
			}
		})
	}
}
