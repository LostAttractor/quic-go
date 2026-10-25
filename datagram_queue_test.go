package quic

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/daeuniverse/quic-go/internal/utils"
	"github.com/daeuniverse/quic-go/internal/wire"
	"github.com/daeuniverse/quic-go/qlog"
	"github.com/daeuniverse/quic-go/qlogwriter"
	"github.com/daeuniverse/quic-go/testutils/events"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDatagramQueuePeekAndPop(t *testing.T) {
	var queued []struct{}
	queue := newDatagramQueue(func() { queued = append(queued, struct{}{}) }, utils.DefaultLogger, nil)
	require.Nil(t, queue.Peek())
	require.Empty(t, queued)
	require.NoError(t, queue.Add(&wire.DatagramFrame{Data: []byte("foo")}))
	require.Len(t, queued, 1)
	require.Equal(t, &wire.DatagramFrame{Data: []byte("foo")}, queue.Peek())
	// calling peek again returns the same datagram
	require.Equal(t, &wire.DatagramFrame{Data: []byte("foo")}, queue.Peek())
	queue.Pop()
	require.Nil(t, queue.Peek())
}

func TestDatagramQueueSendQueueLength(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		queue := newDatagramQueue(func() {}, utils.DefaultLogger, nil)

		for range maxDatagramSendQueueLen {
			require.NoError(t, queue.Add(&wire.DatagramFrame{Data: []byte{0}}))
		}
		errChan := make(chan error, 1)
		go func() { errChan <- queue.Add(&wire.DatagramFrame{Data: []byte("foobar")}) }()

		synctest.Wait()

		select {
		case <-errChan:
			t.Fatal("expected to not receive error")
		default:
		}

		// peeking doesn't remove the datagram from the queue...
		require.NotNil(t, queue.Peek())
		synctest.Wait()
		select {
		case <-errChan:
			t.Fatal("expected to not receive error")
		default:
		}

		// ...but popping does
		queue.Pop()
		synctest.Wait()
		select {
		case err := <-errChan:
			require.NoError(t, err)
		default:
			t.Fatal("timeout")
		}
		// pop all the remaining datagrams
		for range maxDatagramSendQueueLen - 1 {
			queue.Pop()
		}
		f := queue.Peek()
		require.NotNil(t, f)
		require.Equal(t, &wire.DatagramFrame{Data: []byte("foobar")}, f)
	})
}

func TestDatagramQueueReceive(t *testing.T) {
	queue := newDatagramQueue(func() {}, utils.DefaultLogger, nil)

	// receive frames that were received earlier
	queue.HandleDatagramFrame(&wire.DatagramFrame{Data: []byte("foo")})
	queue.HandleDatagramFrame(&wire.DatagramFrame{Data: []byte("bar")})
	data, err := queue.Receive(context.Background())
	require.NoError(t, err)
	require.Equal(t, []byte("foo"), data)
	data, err = queue.Receive(context.Background())
	require.NoError(t, err)
	require.Equal(t, []byte("bar"), data)
}

func TestDatagramQueueReceiveCanceledContext(t *testing.T) {
	queue := newDatagramQueue(func() {}, utils.DefaultLogger, nil)
	queue.HandleDatagramFrame(&wire.DatagramFrame{Data: []byte("foo")})

	// a canceled context takes precedence over queued datagrams
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := queue.Receive(ctx)
	require.ErrorIs(t, err, context.Canceled)

	// the datagram is still queued
	data, err := queue.Receive(context.Background())
	require.NoError(t, err)
	require.Equal(t, []byte("foo"), data)
}

type datagramDrop struct {
	reason DatagramDropReason
	size   int
}

// recordDatagramDrops records the queue's drop reports.
func recordDatagramDrops(queue *datagramQueue) *[]datagramDrop {
	var dropped []datagramDrop
	queue.dropped = func(reason DatagramDropReason, size int) { dropped = append(dropped, datagramDrop{reason, size}) }
	return &dropped
}

func TestDatagramQueueReceiveOverflow(t *testing.T) {
	qlogger := &events.Recorder{}
	queue := newDatagramQueue(func() {}, utils.DefaultLogger, qlogger)
	dropped := recordDatagramDrops(queue)
	for range maxDatagramRcvQueueLen {
		queue.HandleDatagramFrame(&wire.DatagramFrame{Data: []byte("queued")})
	}
	require.Empty(t, qlogger.Events())

	f := &wire.DatagramFrame{Data: []byte("dropped"), DataLenPresent: true}
	queue.HandleDatagramFrame(f)
	require.Equal(t, []qlogwriter.Event{qlog.DatagramDropped{
		Direction: qlog.DirectionReceiving,
		Raw:       qlog.RawInfo{Length: len(f.Data), PayloadLength: qlog.OmitLength},
		Trigger:   "buffer_full",
	}}, qlogger.Events())
	require.Equal(t, []datagramDrop{{DatagramDropReceiveQueueFull, len(f.Data)}}, *dropped)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	for range maxDatagramRcvQueueLen {
		data, err := queue.Receive(ctx)
		require.NoError(t, err)
		require.Equal(t, []byte("queued"), data)
	}
	queue.HandleDatagramFrame(&wire.DatagramFrame{Data: []byte("next")})
	data, err := queue.Receive(ctx)
	require.NoError(t, err)
	require.Equal(t, []byte("next"), data)
	require.Len(t, qlogger.Events(), 1)
}

func TestDatagramQueueReceiveBlocking(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		queue := newDatagramQueue(func() {}, utils.DefaultLogger, nil)

		// block until a new frame is received
		type result struct {
			data []byte
			err  error
		}
		resultChan := make(chan result, 1)
		go func() {
			data, err := queue.Receive(context.Background())
			resultChan <- result{data, err}
		}()

		synctest.Wait()

		select {
		case <-resultChan:
			t.Fatal("expected to not receive result")
		default:
		}
		queue.HandleDatagramFrame(&wire.DatagramFrame{Data: []byte("foobar")})
		synctest.Wait()
		select {
		case result := <-resultChan:
			require.NoError(t, result.err)
			require.Equal(t, []byte("foobar"), result.data)
		default:
			t.Fatal("should have received a datagram frame")
		}

		// unblock when the context is canceled
		ctx, cancel := context.WithCancel(context.Background())
		errChan := make(chan error, 1)
		go func() {
			_, err := queue.Receive(ctx)
			errChan <- err
		}()

		synctest.Wait()
		select {
		case <-errChan:
			t.Fatal("expected to not receive error")
		default:
		}

		cancel()
		synctest.Wait()

		select {
		case err := <-errChan:
			require.ErrorIs(t, err, context.Canceled)
		default:
			t.Fatal("should have received a context canceled error")
		}
	})
}

func TestDatagramQueueClose(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		queue := newDatagramQueue(func() {}, utils.DefaultLogger, nil)

		for range maxDatagramSendQueueLen {
			require.NoError(t, queue.Add(&wire.DatagramFrame{Data: []byte{0}}))
		}
		errChan1 := make(chan error, 1)
		go func() { errChan1 <- queue.Add(&wire.DatagramFrame{Data: []byte("foobar")}) }()
		errChan2 := make(chan error, 1)
		go func() {
			_, err := queue.Receive(context.Background())
			errChan2 <- err
		}()

		queue.CloseWithError(assert.AnError)
		synctest.Wait()

		select {
		case err := <-errChan1:
			require.ErrorIs(t, err, assert.AnError)
		default:
			t.Fatal("should have received an error")
		}

		select {
		case err := <-errChan2:
			require.ErrorIs(t, err, assert.AnError)
		default:
			t.Fatal("should have received an error")
		}
	})
}

func TestDatagramQueueAddAfterClose(t *testing.T) {
	queue := newDatagramQueue(func() {}, utils.DefaultLogger, nil)
	queue.CloseWithError(assert.AnError)
	require.ErrorIs(t, queue.Add(&wire.DatagramFrame{Data: []byte("foo")}), assert.AnError)
	require.Nil(t, queue.Peek())
}

func TestDatagramQueueCloseReportsUnsent(t *testing.T) {
	queue := newDatagramQueue(func() {}, utils.DefaultLogger, nil)
	dropped := recordDatagramDrops(queue)
	require.NoError(t, queue.Add(&wire.DatagramFrame{Data: []byte("foo")}))
	require.NoError(t, queue.Add(&wire.DatagramFrame{Data: []byte("foobar")}))
	queue.CloseWithError(assert.AnError)
	require.Equal(t, []datagramDrop{{DatagramDropClosed, 3}, {DatagramDropClosed, 6}}, *dropped)
	require.Nil(t, queue.Peek())
}

// Dial, DialEarly and Listen populate their configs; each connection reports
// its own drops to the callback of its config.
func TestConnectionReportsDatagramDrops(t *testing.T) {
	var dropped []datagramDrop
	tc := newClientTestConnection(t, nil, &Config{
		DisablePathMTUDiscovery: true,
		EnableDatagrams:         true,
		DatagramDropped: func(reason DatagramDropReason, size int) {
			dropped = append(dropped, datagramDrop{reason, size})
		},
	}, false)
	for range maxDatagramRcvQueueLen + 1 {
		tc.conn.datagramQueue.HandleDatagramFrame(&wire.DatagramFrame{Data: []byte("foo")})
	}
	require.Equal(t, []datagramDrop{{DatagramDropReceiveQueueFull, 3}}, dropped)
}
