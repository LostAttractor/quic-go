package quic

import (
	"slices"
	"testing"

	"github.com/daeuniverse/quic-go/internal/protocol"
	"github.com/daeuniverse/quic-go/internal/wire"
)

func TestOutgoingStreamCapacity(t *testing.T) {
	for _, perspective := range []protocol.Perspective{protocol.PerspectiveClient, protocol.PerspectiveServer} {
		for _, kind := range []StreamType{StreamTypeBidi, StreamTypeUni} {
			var counts []int64
			m := newOutgoingStreamsMap(kind,
				func(id protocol.StreamID) *mockStream { return &mockStream{id: id} },
				func(wire.Frame) {}, perspective,
				func(got StreamType, count int64) {
					if got != kind {
						t.Fatalf("stream type = %v, want %v", got, kind)
					}
					counts = append(counts, count)
				})
			m.SetMaxStream(protocol.StreamNum(2).StreamID(kind, perspective))
			for range 2 {
				if _, err := m.OpenStream(); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := m.OpenStream(); err == nil {
				t.Fatal("opened beyond the peer limit")
			}
			m.SetMaxStream(protocol.StreamNum(3).StreamID(kind, perspective))
			if _, err := m.OpenStreamSync(t.Context()); err != nil {
				t.Fatal(err)
			}
			if want := []int64{0, 2, 1, 0, 1, 0}; !slices.Equal(counts, want) {
				t.Fatalf("perspective=%v kind=%v: capacity=%v, want %v", perspective, kind, counts, want)
			}
		}
	}
}

func TestStreamCapacitiesAreIndependent(t *testing.T) {
	capacity := make(map[StreamType]int64)
	m := newStreamsMap(t.Context(), nil, func(wire.Frame) {}, newTestStreamFlowController, 1, 1,
		protocol.PerspectiveClient, func(kind StreamType, n int64) { capacity[kind] = n })
	m.HandleTransportParameters(&wire.TransportParameters{MaxBidiStreamNum: 100, MaxUniStreamNum: 3})
	if _, err := m.OpenStream(); err != nil {
		t.Fatal(err)
	}
	if _, err := m.OpenUniStream(); err != nil {
		t.Fatal(err)
	}
	if capacity[StreamTypeBidi] != 99 || capacity[StreamTypeUni] != 2 {
		t.Fatalf("capacities overwritten: %v", capacity)
	}
}
