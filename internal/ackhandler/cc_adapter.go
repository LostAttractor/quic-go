package ackhandler

import (
	"time"

	"github.com/daeuniverse/quic-go/congestion"
	cgInternal "github.com/daeuniverse/quic-go/internal/congestion"
	"github.com/daeuniverse/quic-go/internal/monotime"
	"github.com/daeuniverse/quic-go/internal/protocol"
)

var (
	_ cgInternal.SendAlgorithmEx             = &ccAdapter{}
	_ cgInternal.SendAlgorithmWithDebugInfos = &ccAdapter{}
)

type ccAdapter struct {
	CC congestion.CongestionControl
	// Assigned on the connection loop, independently of QUIC packet-number
	// spaces. The adapter identity is also the controller installation epoch.
	nextPacketNumber protocol.PacketNumber
}

func (a *ccAdapter) sentPacket(t monotime.Time, flight protocol.ByteCount, p *packet) {
	p.congestionController, p.congestionNumber = a, a.nextPacketNumber
	a.nextPacketNumber++
	a.OnPacketSent(t, flight, p.congestionNumber, p.Length, p.IsAckEliciting())
}

// Old-controller packets still contribute to transport flight accounting, but
// have no sample in the current controller. Never alias them to a new sample.
func (p *packet) congestionID(cc cgInternal.SendAlgorithmWithDebugInfos, wireNumber protocol.PacketNumber) protocol.PacketNumber {
	if adapter, ok := cc.(*ccAdapter); ok {
		if p.congestionController != adapter {
			return protocol.InvalidPacketNumber
		}
		return p.congestionNumber
	}
	return wireNumber
}

func (a *ccAdapter) TimeUntilSend(bytesInFlight protocol.ByteCount) monotime.Time {
	return monotime.FromTime(a.CC.TimeUntilSend(congestion.ByteCount(bytesInFlight)))
}

func (a *ccAdapter) HasPacingBudget(now monotime.Time) bool {
	return a.CC.HasPacingBudget(now.ToTime())
}

func (a *ccAdapter) OnPacketSent(sentTime monotime.Time, bytesInFlight protocol.ByteCount, packetNumber protocol.PacketNumber, bytes protocol.ByteCount, isRetransmittable bool) {
	// The internal handler counts this packet before notifying the controller;
	// external delivery-rate samplers need the flight size before transmission.
	if isRetransmittable {
		bytesInFlight -= bytes
	}
	a.CC.OnPacketSent(sentTime.ToTime(), congestion.ByteCount(bytesInFlight), congestion.PacketNumber(packetNumber), congestion.ByteCount(bytes), isRetransmittable)
}

func (a *ccAdapter) CanSend(bytesInFlight protocol.ByteCount) bool {
	return a.CC.CanSend(congestion.ByteCount(bytesInFlight))
}

func (a *ccAdapter) MaybeExitSlowStart() {
	a.CC.MaybeExitSlowStart()
}

func (a *ccAdapter) OnPacketAcked(number protocol.PacketNumber, ackedBytes protocol.ByteCount, priorInFlight protocol.ByteCount, eventTime monotime.Time) {
	a.CC.OnPacketAcked(congestion.PacketNumber(number), congestion.ByteCount(ackedBytes), congestion.ByteCount(priorInFlight), eventTime.ToTime())
}

func (a *ccAdapter) OnCongestionEvent(number protocol.PacketNumber, lostBytes protocol.ByteCount, priorInFlight protocol.ByteCount) {
	a.CC.OnCongestionEvent(congestion.PacketNumber(number), congestion.ByteCount(lostBytes), congestion.ByteCount(priorInFlight))
}

func (a *ccAdapter) OnCongestionEventEx(priorInFlight protocol.ByteCount, eventTime time.Time, ackedPackets []congestion.AckedPacketInfo, lostPackets []congestion.LostPacketInfo) {
	a.CC.OnCongestionEventEx(congestion.ByteCount(priorInFlight), eventTime, ackedPackets, lostPackets)
}

func (a *ccAdapter) OnRetransmissionTimeout(packetsRetransmitted bool) {
	a.CC.OnRetransmissionTimeout(packetsRetransmitted)
}

func (a *ccAdapter) SetMaxDatagramSize(size protocol.ByteCount) {
	a.CC.SetMaxDatagramSize(congestion.ByteCount(size))
}

func (a *ccAdapter) InSlowStart() bool {
	return a.CC.InSlowStart()
}

func (a *ccAdapter) InRecovery() bool {
	return a.CC.InRecovery()
}

func (a *ccAdapter) GetCongestionWindow() protocol.ByteCount {
	return protocol.ByteCount(a.CC.GetCongestionWindow())
}
