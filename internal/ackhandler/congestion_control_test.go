package ackhandler

import (
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/daeuniverse/quic-go/congestion"
	"github.com/daeuniverse/quic-go/internal/monotime"
	"github.com/daeuniverse/quic-go/internal/protocol"
	"github.com/daeuniverse/quic-go/internal/utils"
	"github.com/daeuniverse/quic-go/internal/wire"
)

// fakeController implements only what sending needs; the embedded nil
// interface makes any other call panic.
type fakeController struct {
	congestion.CongestionControl
	rttStats congestion.RTTStatsProvider
	sizes    []congestion.ByteCount
}

func (f *fakeController) SetRTTStatsProvider(p congestion.RTTStatsProvider) { f.rttStats = p }
func (f *fakeController) SetMaxDatagramSize(s congestion.ByteCount)         { f.sizes = append(f.sizes, s) }
func (f *fakeController) CanSend(congestion.ByteCount) bool                 { return true }
func (f *fakeController) HasPacingBudget(time.Time) bool                    { return true }
func (f *fakeController) TimeUntilSend(congestion.ByteCount) time.Time      { return time.Time{} }
func (f *fakeController) GetCongestionWindow() congestion.ByteCount         { return 1 << 20 }
func (f *fakeController) OnPacketSent(time.Time, congestion.ByteCount, congestion.PacketNumber, congestion.ByteCount, bool) {
}

func newTestSentPacketHandler(initialMaxDatagramSize protocol.ByteCount, connStats *utils.ConnectionStats) *sentPacketHandler {
	return NewSentPacketHandler(0, initialMaxDatagramSize, utils.NewRTTStats(), connStats, true, false,
		nil, protocol.PerspectiveClient, nil, utils.DefaultLogger).(*sentPacketHandler)
}

func sendTestPacket(h *sentPacketHandler, now monotime.Time, size protocol.ByteCount) protocol.PacketNumber {
	pn := h.PopPacketNumber(protocol.PacketNumberSpaceAppData)
	h.SentPacket(now, pn, protocol.InvalidPacketNumber, nil, []Frame{{Frame: &wire.PingFrame{}}},
		protocol.Encryption1RTT, protocol.ECNNon, size, false, false)
	return pn
}

func TestSetCongestionControlInstallsOnConnectionLoop(t *testing.T) {
	h := newTestSentPacketHandler(1200, &utils.ConnectionStats{})
	cc := &fakeController{}
	h.SetCongestionControl(cc)
	if cc.rttStats != nil {
		t.Fatal("SetCongestionControl called into the controller on the caller's goroutine")
	}
	h.SendMode(monotime.Now())
	if cc.rttStats == nil {
		t.Fatal("controller not installed on its first use by the connection")
	}
}

func TestSetCongestionControlConcurrentWithConnection(t *testing.T) {
	h := newTestSentPacketHandler(1200, &utils.ConnectionStats{})
	var wg sync.WaitGroup
	wg.Go(func() {
		for range 100 {
			h.SetCongestionControl(&fakeController{})
		}
	})
	now := monotime.Now()
	for range 100 {
		h.SendMode(now)
		sendTestPacket(h, now, 1200)
	}
	wg.Wait()
}

func TestInstalledControllerLearnsMaxDatagramSize(t *testing.T) {
	// The first Initial failed with EMSGSIZE before a non-0-RTT dial installed the controller.
	h := newTestSentPacketHandler(protocol.InitialPacketSize, &utils.ConnectionStats{})
	h.SetMaxDatagramSize(protocol.MinInitialPacketSize)
	cc := &fakeController{}
	h.SetCongestionControl(cc)
	now := monotime.Now()
	h.SendMode(now)
	if len(cc.sizes) == 0 || cc.sizes[len(cc.sizes)-1] != protocol.MinInitialPacketSize {
		t.Fatalf("installed controller saw sizes %v, want the shrunk size %d", cc.sizes, protocol.MinInitialPacketSize)
	}

	h.SetMaxDatagramSize(1452)
	h.MigratedPath(now, protocol.InitialPacketSize)
	if last := cc.sizes[len(cc.sizes)-1]; last != protocol.InitialPacketSize {
		t.Fatalf("controller kept across migration has size %d, want %d", last, protocol.InitialPacketSize)
	}
}

type windowController struct {
	fakeController
	cwnd congestion.ByteCount
}

func (c *windowController) GetCongestionWindow() congestion.ByteCount { return c.cwnd }
func (c *windowController) MaybeExitSlowStart()                       {}
func (c *windowController) OnPacketAcked(congestion.PacketNumber, congestion.ByteCount, congestion.ByteCount, time.Time) {
}
func (c *windowController) OnCongestionEvent(congestion.PacketNumber, congestion.ByteCount, congestion.ByteCount) {
}
func (c *windowController) OnCongestionEventEx(congestion.ByteCount, time.Time, []congestion.AckedPacketInfo, []congestion.LostPacketInfo) {
	c.cwnd /= 2
}

func TestConnectionStatsWithExternalController(t *testing.T) {
	for _, external := range []bool{false, true} {
		t.Run(fmt.Sprintf("external=%t", external), func(t *testing.T) {
			stats := &utils.ConnectionStats{}
			h := newTestSentPacketHandler(1200, stats)
			cc := &windowController{cwnd: 1 << 20}
			if external {
				h.SetCongestionControl(cc)
			}
			now := monotime.Now()
			h.SendMode(now)
			if external && stats.CongestionWindow.Load() != uint64(cc.cwnd) {
				t.Fatalf("installed window not published: %d, want %d", stats.CongestionWindow.Load(), cc.cwnd)
			}
			var pns []protocol.PacketNumber
			for range 5 {
				pns = append(pns, sendTestPacket(h, now, 1200))
			}
			// Acknowledging the last three loses the first two by packet threshold.
			if _, err := h.ReceivedAck(&wire.AckFrame{AckRanges: ackRanges(pns[2:]...)}, protocol.Encryption1RTT, now.Add(10*time.Millisecond)); err != nil {
				t.Fatal(err)
			}
			if lost, bytes := stats.PacketsLost.Load(), stats.BytesLost.Load(); lost != 2 || bytes != 2400 {
				t.Fatalf("lost %d packets / %d bytes, want 2 / 2400", lost, bytes)
			}
			if external && stats.CongestionWindow.Load() != uint64(cc.cwnd) {
				t.Fatalf("window after the event = %d, controller has %d", stats.CongestionWindow.Load(), cc.cwnd)
			}
		})
	}
}

func TestDefaultControllerSkipsCongestionEventPacketInfo(t *testing.T) {
	h := newTestSentPacketHandler(1200, &utils.ConnectionStats{})
	now := monotime.Now()
	var pns []protocol.PacketNumber
	for range 5 {
		pns = append(pns, sendTestPacket(h, now, 1200))
	}
	if _, err := h.ReceivedAck(&wire.AckFrame{AckRanges: ackRanges(pns[2:]...)}, protocol.Encryption1RTT, now.Add(10*time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	if cap(h.ackedPacketsInfo) != 0 || cap(h.lostPacketsInfo) != 0 {
		t.Fatalf("default sender built event packet info: acked cap %d, lost cap %d", cap(h.ackedPacketsInfo), cap(h.lostPacketsInfo))
	}
}

func TestControllerGetsReadOnlyRTT(t *testing.T) {
	h := newTestSentPacketHandler(1200, &utils.ConnectionStats{})
	cc := &fakeController{}
	h.SetCongestionControl(cc)
	h.SendMode(monotime.Now())
	if _, ok := cc.rttStats.(interface {
		UpdateRTT(time.Duration, time.Duration)
	}); ok {
		t.Fatal("controller can update the RTT that loss recovery uses")
	}
	if cc.rttStats.SmoothedRTT() != h.rttStats.SmoothedRTT() {
		t.Fatal("RTT view does not reflect the connection's estimate")
	}
}

type eventController struct {
	windowController
	acked []congestion.AckedPacketInfo
}

func (c *eventController) OnCongestionEventEx(_ congestion.ByteCount, _ time.Time, acked []congestion.AckedPacketInfo, _ []congestion.LostPacketInfo) {
	c.acked = append(c.acked, acked...)
}

func TestAckedPacketInfoIsFilled(t *testing.T) {
	h := newTestSentPacketHandler(1200, &utils.ConnectionStats{})
	cc := &eventController{windowController: windowController{cwnd: 1 << 20}}
	h.SetCongestionControl(cc)
	now := monotime.Now()
	pns := []protocol.PacketNumber{sendTestPacket(h, now, 1200), sendTestPacket(h, now, 1200)}
	if _, err := h.ReceivedAck(&wire.AckFrame{AckRanges: ackRanges(pns...)}, protocol.Encryption1RTT, now.Add(10*time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	if len(cc.acked) != 2 {
		t.Fatalf("acked %v", cc.acked)
	}
	// The second packet has a non-zero number, so every field must be set.
	info := reflect.ValueOf(cc.acked[1])
	for i := range info.NumField() {
		if info.Field(i).IsZero() {
			t.Errorf("AckedPacketInfo.%s is never filled", info.Type().Field(i).Name)
		}
	}
}
