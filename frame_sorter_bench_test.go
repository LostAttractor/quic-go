package quic

import (
	"fmt"
	"math/rand/v2"
	"testing"

	"github.com/daeuniverse/quic-go/internal/protocol"
)

func BenchmarkFrameSorter(b *testing.B) {
	data := make([]byte, 1200)
	b.Run("in_order", func(b *testing.B) {
		s := newFrameSorter()
		var offset protocol.ByteCount
		b.ReportAllocs()
		for b.Loop() {
			if err := s.Push(data, offset, nil); err != nil {
				b.Fatal(err)
			}
			_, got, _ := s.Pop()
			if len(got) != len(data) {
				b.Fatal("lost data")
			}
			offset += protocol.ByteCount(len(data))
		}
	})
	for _, n := range []int{64, 1024, 3072} {
		b.Run(fmt.Sprintf("reordered_%d", n), func(b *testing.B) {
			order := rand.New(rand.NewPCG(1, 2)).Perm(n)
			s := newFrameSorter()
			var offset protocol.ByteCount
			b.ReportAllocs()
			for b.Loop() {
				for _, i := range order {
					if err := s.Push(data, offset+protocol.ByteCount(i*len(data)), nil); err != nil {
						b.Fatal(err)
					}
				}
				for range n {
					_, got, _ := s.Pop()
					if len(got) != len(data) {
						b.Fatal("lost data")
					}
				}
				offset += protocol.ByteCount(n * len(data))
			}
		})
	}
}
