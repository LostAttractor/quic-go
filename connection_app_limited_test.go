package quic

import (
	"testing"
	"time"

	"github.com/daeuniverse/quic-go/internal/ackhandler"
	mockackhandler "github.com/daeuniverse/quic-go/internal/mocks/ackhandler"
	"github.com/daeuniverse/quic-go/internal/monotime"
	"github.com/daeuniverse/quic-go/internal/protocol"
	"go.uber.org/mock/gomock"
)

type appLimitedHandler struct {
	ackhandler.SentPacketHandler
	notifications int
}

func (h *appLimitedHandler) OnApplicationLimited() { h.notifications++ }

func TestConnectionNotifiesApplicationLimited(t *testing.T) {
	for _, gso := range []bool{false, true} {
		name := "without_gso"
		if gso {
			name = "with_gso"
		}
		t.Run(name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			mock := mockackhandler.NewMockSentPacketHandler(ctrl)
			h := &appLimitedHandler{SentPacketHandler: mock}
			tc := newServerTestConnection(t, ctrl, nil, gso, connectionOptHandshakeConfirmed(), connectionOptSentPacketHandler(h))
			mock.EXPECT().ECNMode(true).Return(protocol.ECNNon)
			tc.packer.EXPECT().AppendPacket(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(shortHeaderPacket{}, errNothingToPack)
			if err := tc.conn.sendPackets(monotime.Now()); err != nil {
				t.Fatal(err)
			}
			if h.notifications != 1 {
				t.Fatal("empty send opportunity not reported")
			}
		})
	}
}

func TestConnectionBlockedSenderIsNotApplicationLimited(t *testing.T) {
	for _, mode := range []ackhandler.SendMode{ackhandler.SendAck, ackhandler.SendPacingLimited, ackhandler.SendNone} {
		ctrl := gomock.NewController(t)
		mock := mockackhandler.NewMockSentPacketHandler(ctrl)
		h := &appLimitedHandler{SentPacketHandler: mock}
		tc := newServerTestConnection(t, ctrl, nil, false, connectionOptHandshakeConfirmed(), connectionOptSentPacketHandler(h))
		mock.EXPECT().SendMode(gomock.Any()).Return(mode)
		if mode == ackhandler.SendPacingLimited {
			mock.EXPECT().TimeUntilSend().Return(monotime.Now().Add(time.Millisecond))
		}
		if mode != ackhandler.SendNone {
			mock.EXPECT().ECNMode(true).Return(protocol.ECNNon)
			tc.packer.EXPECT().PackAckOnlyPacket(gomock.Any(), gomock.Any(), gomock.Any()).Return(shortHeaderPacket{}, nil, errNothingToPack)
		}
		if err := tc.conn.triggerSending(monotime.Now()); err != nil {
			t.Fatal(err)
		}
		if h.notifications != 0 {
			t.Fatalf("blocked send mode %v was reported as application limited", mode)
		}
	}
}
