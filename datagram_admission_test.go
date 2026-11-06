package quic

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"

	"github.com/daeuniverse/quic-go/internal/protocol"
	"github.com/daeuniverse/quic-go/internal/utils"
	"github.com/daeuniverse/quic-go/internal/wire"
	"github.com/stretchr/testify/require"
)

func datagramAdmissionConn() *Conn {
	c := &Conn{
		version:       protocol.Version1,
		datagramQueue: newDatagramQueue(func() {}, utils.DefaultLogger, nil),
	}
	c.peerParams.Store(&wire.TransportParameters{MaxDatagramFrameSize: 1200})
	c.maxPayloadSizeEstimate.Store(1200)
	return c
}

func TestTrySendDatagramAdmission(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c := datagramAdmissionConn()
		payload := []byte("original")
		require.NoError(t, c.TrySendDatagram(payload))
		payload[0] = 'X'
		require.Equal(t, "original", string(c.datagramQueue.Peek().Data))
		for range maxDatagramSendQueueLen - 1 {
			require.NoError(t, c.TrySendDatagram(payload))
		}
		require.ErrorIs(t, c.TrySendDatagram(payload), ErrDatagramQueueFull)
		allocs := testing.AllocsPerRun(100, func() {
			if c.TrySendDatagram(payload) != ErrDatagramQueueFull {
				panic("full queue accepted a datagram")
			}
		})
		require.Zero(t, allocs, "full admission copied or allocated a datagram")
		c.datagramQueue.Pop()
		require.NoError(t, c.TrySendDatagram(payload))
		closed := errors.New("test connection closed")
		c.datagramQueue.CloseWithError(closed)
		require.ErrorIs(t, c.TrySendDatagram(payload), closed)
	})
}

func TestTrySendDatagramChecksSizeAndNegotiation(t *testing.T) {
	c := datagramAdmissionConn()
	_, tooLarge := errors.AsType[*DatagramTooLargeError](c.TrySendDatagram(make([]byte, 1200)))
	require.True(t, tooLarge)
	require.Nil(t, c.datagramQueue.Peek())
	c.peerParams.Store(&wire.TransportParameters{})
	require.Error(t, c.TrySendDatagram([]byte("disabled")))
	require.Nil(t, c.datagramQueue.Peek())
}

func TestTrySendDatagramConcurrentAdmission(t *testing.T) {
	c := datagramAdmissionConn()
	var accepted, rejected atomic.Int32
	var workers sync.WaitGroup
	for i := range 2 * maxDatagramSendQueueLen {
		workers.Go(func() {
			err := c.TrySendDatagram([]byte{byte(i)})
			switch {
			case err == nil:
				accepted.Add(1)
			case errors.Is(err, ErrDatagramQueueFull):
				rejected.Add(1)
			default:
				t.Errorf("unexpected admission error: %v", err)
			}
		})
	}
	workers.Wait()
	require.EqualValues(t, maxDatagramSendQueueLen, accepted.Load())
	require.EqualValues(t, maxDatagramSendQueueLen, rejected.Load())
	require.Equal(t, maxDatagramSendQueueLen, c.datagramQueue.sendQueue.Len())
}
