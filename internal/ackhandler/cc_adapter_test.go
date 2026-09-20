package ackhandler

import (
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/daeuniverse/quic-go/congestion"
	"github.com/daeuniverse/quic-go/internal/monotime"
	"github.com/daeuniverse/quic-go/internal/protocol"
	"github.com/daeuniverse/quic-go/internal/utils"
	"github.com/daeuniverse/quic-go/internal/wire"
)

type sendStateRecorder struct {
	congestion.CongestionControl
	priorInFlight congestion.ByteCount
	appLimited    int
	discarded     []congestion.PacketNumber
}

type sampleRecorder struct {
	sendStateRecorder
	samples map[congestion.PacketNumber]congestion.ByteCount
	acked   []congestion.AckedPacketInfo
	lost    []congestion.LostPacketInfo
	flight  congestion.ByteCount
}

func (r *sampleRecorder) SetRTTStatsProvider(congestion.RTTStatsProvider) {}
func (r *sampleRecorder) MaybeExitSlowStart()                             {}
func (r *sampleRecorder) OnPacketAcked(congestion.PacketNumber, congestion.ByteCount, congestion.ByteCount, time.Time) {
}

func (r *sampleRecorder) OnCongestionEvent(congestion.PacketNumber, congestion.ByteCount, congestion.ByteCount) {
}

func (r *sampleRecorder) OnPacketSent(_ time.Time, flight congestion.ByteCount, number congestion.PacketNumber, size congestion.ByteCount, controlled bool) {
	if controlled {
		r.samples[number] = size
		r.flight = flight + size
	}
}

func (r *sampleRecorder) OnPacketDiscarded(number congestion.PacketNumber, flight congestion.ByteCount) {
	r.sendStateRecorder.OnPacketDiscarded(number, flight)
	delete(r.samples, number)
	r.flight = flight
}

func (r *sampleRecorder) OnCongestionEventEx(flight congestion.ByteCount, _ time.Time, acked []congestion.AckedPacketInfo, lost []congestion.LostPacketInfo) {
	r.acked, r.lost = slices.Clone(acked), slices.Clone(lost)
	for _, p := range acked {
		flight -= p.BytesAcked
		delete(r.samples, p.PacketNumber)
	}
	for _, p := range lost {
		flight -= p.BytesLost
		delete(r.samples, p.PacketNumber)
	}
	r.flight = flight
}

func TestExternalControllerPacketIdentityAcrossSpacesAndReplacement(t *testing.T) {
	for _, oldController := range []bool{false, true} {
		for _, finish := range []string{"discard", "ack", "loss"} {
			t.Run(fmt.Sprintf("old_external=%t/%s", oldController, finish), func(t *testing.T) {
				h := NewSentPacketHandler(0, 1200, utils.NewRTTStats(), &utils.ConnectionStats{}, true, false,
					nil, protocol.PerspectiveClient, nil, utils.DefaultLogger).(*sentPacketHandler)
				now := monotime.Now()
				send := func(level protocol.EncryptionLevel, size protocol.ByteCount) protocol.PacketNumber {
					pn := h.PopPacketNumber(level)
					h.SentPacket(now, pn, protocol.InvalidPacketNumber, nil, []Frame{{Frame: &wire.PingFrame{}}}, level, protocol.ECNNon, size, false, false)
					return pn
				}
				if oldController {
					h.SetCongestionControl(&sampleRecorder{samples: make(map[congestion.PacketNumber]congestion.ByteCount)})
				}
				handshake := send(protocol.EncryptionHandshake, 900)
				r := &sampleRecorder{samples: make(map[congestion.PacketNumber]congestion.ByteCount)}
				h.SetCongestionControl(r)
				app := send(protocol.Encryption1RTT, 1200)
				switch finish {
				case "discard":
					h.DropPackets(protocol.EncryptionHandshake, now.Add(time.Millisecond))
				case "ack":
					if _, err := h.ReceivedAck(&wire.AckFrame{AckRanges: ackRanges(handshake)}, protocol.EncryptionHandshake, now.Add(50*time.Millisecond)); err != nil {
						t.Fatal(err)
					}
				case "loss":
					var last protocol.PacketNumber
					for range 3 {
						last = send(protocol.EncryptionHandshake, 800)
					}
					if _, err := h.ReceivedAck(&wire.AckFrame{AckRanges: []wire.AckRange{{Smallest: 1, Largest: last}}}, protocol.EncryptionHandshake, now.Add(time.Second)); err != nil {
						t.Fatal(err)
					}
					if len(r.lost) != 1 || r.lost[0].PacketNumber != congestion.InvalidPacketNumber {
						t.Fatalf("old handshake loss acquired a sample ID: %v", r.lost)
					}
				}
				if len(r.samples) != 1 || r.samples[0] != 1200 || r.flight != 1200 {
					t.Fatalf("old handshake consumed app sample: samples=%v flight=%d", r.samples, r.flight)
				}
				if _, err := h.ReceivedAck(&wire.AckFrame{AckRanges: ackRanges(app)}, protocol.Encryption1RTT, now.Add(2*time.Second)); err != nil {
					t.Fatal(err)
				}
				if len(r.acked) != 1 || r.acked[0].PacketNumber != 0 || r.flight != 0 || len(r.samples) != 0 {
					t.Fatalf("app ACK lost its sample identity: %+v", r)
				}
			})
		}
	}
}

func TestExternalControllerInterleavedPacketSpaces(t *testing.T) {
	h := NewSentPacketHandler(0, 1200, utils.NewRTTStats(), &utils.ConnectionStats{}, true, false,
		nil, protocol.PerspectiveClient, nil, utils.DefaultLogger).(*sentPacketHandler)
	r := &sampleRecorder{samples: make(map[congestion.PacketNumber]congestion.ByteCount)}
	h.SetCongestionControl(r)
	now := monotime.Now()
	for _, level := range []protocol.EncryptionLevel{protocol.EncryptionInitial, protocol.EncryptionHandshake, protocol.Encryption1RTT} {
		pn := h.PopPacketNumber(level) // Each number space starts at zero.
		h.SentPacket(now, pn, protocol.InvalidPacketNumber, nil, []Frame{{Frame: &wire.PingFrame{}}}, level, protocol.ECNNon, 1200, false, false)
	}
	if len(r.samples) != 3 {
		t.Fatalf("number spaces alias: %v", r.samples)
	}
	h.DropPackets(protocol.EncryptionHandshake, now)
	if len(r.samples) != 2 || r.samples[0] != 1200 || r.samples[2] != 1200 || r.flight != 2400 {
		t.Fatalf("discard affected another space: %+v", r)
	}
}

func (r *sendStateRecorder) OnPacketSent(_ time.Time, flight congestion.ByteCount, _ congestion.PacketNumber, _ congestion.ByteCount, _ bool) {
	r.priorInFlight = flight
}
func (r *sendStateRecorder) OnApplicationLimited() { r.appLimited++ }
func (r *sendStateRecorder) OnPacketDiscarded(pn congestion.PacketNumber, _ congestion.ByteCount) {
	r.discarded = append(r.discarded, pn)
}

func TestExternalControllerSendState(t *testing.T) {
	r := &sendStateRecorder{}
	a := &ccAdapter{CC: r}
	a.OnPacketSent(monotime.Now(), 1200, 0, 1200, true)
	if r.priorInFlight != 0 {
		t.Fatal("first packet did not see an empty flight")
	}
	a.OnPacketSent(monotime.Now(), 3600, 1, 1200, true)
	if r.priorInFlight != 2400 {
		t.Fatal("current packet counted in prior flight")
	}
	a.OnPacketSent(monotime.Now(), 3600, 2, 40, false)
	if r.priorInFlight != 3600 {
		t.Fatal("ACK-only packet subtracted from flight")
	}
	h := &sentPacketHandler{congestion: a}
	h.OnApplicationLimited()
	if r.appLimited != 1 {
		t.Fatal("application-limited notification not forwarded")
	}
}

func TestExternalControllerRetiresDiscardedPackets(t *testing.T) {
	for _, reason := range []string{"pto", "initial_keys", "handshake_keys", "0rtt_rejected", "retry", "migration"} {
		t.Run(reason, func(t *testing.T) {
			h := NewSentPacketHandler(0, 1200, utils.NewRTTStats(), &utils.ConnectionStats{}, true, false,
				nil, protocol.PerspectiveServer, nil, utils.DefaultLogger).(*sentPacketHandler)
			r := &sendStateRecorder{}
			h.congestion = &ccAdapter{CC: r}
			level := protocol.Encryption1RTT
			switch reason {
			case "initial_keys", "retry":
				level = protocol.EncryptionInitial
			case "handshake_keys":
				level = protocol.EncryptionHandshake
			case "0rtt_rejected":
				level = protocol.Encryption0RTT
			}
			now := monotime.Now()
			var sent []congestion.PacketNumber
			for range 2 {
				pn := h.PopPacketNumber(level)
				h.SentPacket(now, pn, protocol.InvalidPacketNumber, nil,
					[]Frame{{Frame: &wire.PingFrame{}}}, level, protocol.ECNNon, 1200, false, false)
				sent = append(sent, congestion.PacketNumber(pn))
			}
			switch reason {
			case "pto":
				if !h.QueueProbePacket(level) || h.bytesInFlight != 1200 {
					t.Fatal("PTO did not retire exactly one outstanding packet")
				}
				sent = sent[:1]
			case "retry":
				h.ResetForRetry(now.Add(time.Millisecond))
			case "migration":
				h.MigratedPath(now, 1200)
			default:
				h.DropPackets(level, now)
			}
			if !slices.Equal(r.discarded, sent) {
				t.Fatalf("discard notifications = %v, want %v", r.discarded, sent)
			}
			if reason != "pto" && h.bytesInFlight != 0 {
				t.Fatalf("discard left %d bytes in flight", h.bytesInFlight)
			}
		})
	}
}

func TestExternalControllerIdentifiesLostMTUProbe(t *testing.T) {
	h := NewSentPacketHandler(0, 1200, utils.NewRTTStats(), &utils.ConnectionStats{}, true, false,
		nil, protocol.PerspectiveServer, nil, utils.DefaultLogger).(*sentPacketHandler)
	recorder := &congestionEventRecorder{SendAlgorithmWithDebugInfos: h.getCongestionControl()}
	h.congestion = recorder
	now := monotime.Now()
	var last protocol.PacketNumber
	for i := range 4 {
		last = h.PopPacketNumber(protocol.Encryption1RTT)
		h.SentPacket(now, last, protocol.InvalidPacketNumber, nil,
			[]Frame{{Frame: &wire.PingFrame{}}}, protocol.Encryption1RTT, protocol.ECNNon, 1400, i == 0, false)
	}
	if _, err := h.ReceivedAck(&wire.AckFrame{AckRanges: ackRanges(last)}, protocol.Encryption1RTT, now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if len(recorder.events) != 1 || len(recorder.events[0].lostPackets) == 0 || !recorder.events[0].lostPackets[0].IsPathMTUProbe {
		t.Fatal("MTU loss lost its probe annotation on the real loss-detection path")
	}
}
