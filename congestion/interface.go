package congestion

import (
	"time"

	"github.com/daeuniverse/quic-go/internal/protocol"
)

type (
	ByteCount    protocol.ByteCount
	PacketNumber protocol.PacketNumber
)

// Expose some constants from protocol that congestion control algorithms may need.
const (
	InitialPacketSizeIPv4                   = protocol.InitialPacketSize
	InitialPacketSizeIPv6                   = protocol.InitialPacketSize
	MinPacingDelay                          = protocol.MinPacingDelay
	MaxPacketBufferSize                     = protocol.MaxPacketBufferSize
	MinInitialPacketSize                    = protocol.MinInitialPacketSize
	MaxCongestionWindowPackets              = protocol.MaxCongestionWindowPackets
	PacketsPerConnectionID                  = protocol.PacketsPerConnectionID
	InvalidPacketNumber        PacketNumber = -1
)

type AckedPacketInfo struct {
	PacketNumber PacketNumber
	BytesAcked   ByteCount
	ReceivedTime time.Time
}

type LostPacketInfo struct {
	PacketNumber PacketNumber
	BytesLost    ByteCount
	// MTU probe loss removes bytes from flight but is not congestion loss.
	IsPathMTUProbe bool
}

// Packet numbers identify transmissions monotonically across all QUIC number
// spaces, within this controller's installation. They are not wire packet numbers.
type CongestionControl interface {
	SetRTTStatsProvider(provider RTTStatsProvider)
	TimeUntilSend(bytesInFlight ByteCount) time.Time
	HasPacingBudget(now time.Time) bool
	// bytesInFlight excludes the packet being sent.
	OnPacketSent(sentTime time.Time, bytesInFlight ByteCount, packetNumber PacketNumber, bytes ByteCount, isRetransmittable bool)
	CanSend(bytesInFlight ByteCount) bool
	MaybeExitSlowStart()
	OnPacketAcked(number PacketNumber, ackedBytes ByteCount, priorInFlight ByteCount, eventTime time.Time)
	OnCongestionEvent(number PacketNumber, lostBytes ByteCount, priorInFlight ByteCount)
	// ackedPackets and lostPackets are read-only borrowed slices, sorted by increasing packet number.
	// They are valid only until OnCongestionEventEx returns and must be copied before being retained
	// or used asynchronously.
	// Packets sent before installation have InvalidPacketNumber; their byte
	// counts update flight accounting but must not create delivery/loss samples.
	OnCongestionEventEx(priorInFlight ByteCount, eventTime time.Time, ackedPackets []AckedPacketInfo, lostPackets []LostPacketInfo)
	OnRetransmissionTimeout(packetsRetransmitted bool)
	SetMaxDatagramSize(size ByteCount)
	InSlowStart() bool
	InRecovery() bool
	GetCongestionWindow() ByteCount
}

type RTTStatsProvider interface {
	MinRTT() time.Duration
	LatestRTT() time.Duration
	SmoothedRTT() time.Duration
	MeanDeviation() time.Duration
	MaxAckDelay() time.Duration
	PTO(includeMaxAckDelay bool) time.Duration
	UpdateRTT(sendDelta, ackDelay time.Duration)
	SetMaxAckDelay(mad time.Duration)
	SetInitialRTT(t time.Duration)
}

// ApplicationLimitedCongestionControl is an optional notification for rate
// samplers. It is called when the sender can transmit but has no data to pack,
// never when the sender is blocked by pacing, cwnd, or the send queue.
type ApplicationLimitedCongestionControl interface {
	OnApplicationLimited()
}

// PacketDiscardedCongestionControl is an optional notification for packets
// removed from flight without an ACK or congestion loss (PTO retransmission,
// discarded keys, Retry, or path migration). Rate samplers must retire these
// packets without treating them as delivered or lost. bytesInFlight is the
// transport's remaining flight after removal; InvalidPacketNumber indicates a
// packet sent before this controller was installed.
type PacketDiscardedCongestionControl interface {
	OnPacketDiscarded(packetNumber PacketNumber, bytesInFlight ByteCount)
}
