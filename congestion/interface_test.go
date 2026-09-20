package congestion

import (
	"testing"
	"time"
)

// minimalController implements exactly the methods quic-go calls.
type minimalController struct{}

func (minimalController) SetRTTStatsProvider(RTTStatsProvider)                             {}
func (minimalController) TimeUntilSend(ByteCount) time.Time                                { return time.Time{} }
func (minimalController) HasPacingBudget(time.Time) bool                                   { return true }
func (minimalController) OnPacketSent(time.Time, ByteCount, PacketNumber, ByteCount, bool) {}
func (minimalController) CanSend(ByteCount) bool                                           { return true }
func (minimalController) MaybeExitSlowStart()                                              {}
func (minimalController) OnPacketAcked(PacketNumber, ByteCount, ByteCount, time.Time)      {}
func (minimalController) OnCongestionEvent(PacketNumber, ByteCount, ByteCount)             {}
func (minimalController) OnCongestionEventEx(ByteCount, time.Time, []AckedPacketInfo, []LostPacketInfo) {
}
func (minimalController) SetMaxDatagramSize(ByteCount)   {}
func (minimalController) InSlowStart() bool              { return false }
func (minimalController) InRecovery() bool               { return false }
func (minimalController) GetCongestionWindow() ByteCount { return 0 }

func TestControllerNeedsNoUncalledMethods(t *testing.T) {
	var _ CongestionControl = minimalController{}
}
