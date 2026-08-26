package quic

import (
	"math/rand/v2"
	"testing"

	"github.com/daeuniverse/quic-go/internal/protocol"
)

func TestFrameSorterGapBalanceAndReuse(t *testing.T) {
	var gaps frameSorterGaps
	var check func(*frameSorterGap) (int, int)
	check = func(n *frameSorterGap) (int, int) {
		if n == nil {
			return 0, 0
		}
		lh, lc := check(n.left)
		rh, rc := check(n.right)
		if lh-rh < -1 || lh-rh > 1 || int(n.height) != max(lh, rh)+1 {
			t.Fatalf("unbalanced gap %v: left=%d right=%d height=%d", n.interval, lh, rh, n.height)
		}
		return int(n.height), lc + rc + 1
	}
	random := rand.New(rand.NewPCG(1, 2))
	for range 2 {
		for _, i := range random.Perm(1000) {
			gaps.insert(byteInterval{Start: protocol.ByteCount(4 * i), End: protocol.ByteCount(4*i + 3)})
		}
		for _, i := range random.Perm(1000) {
			start := protocol.ByteCount(4 * i)
			gaps.shrink(byteInterval{Start: start, End: start + 3}, byteInterval{Start: start + 1, End: start + 2})
		}
		for _, i := range random.Perm(1000) {
			gaps.remove(byteInterval{Start: protocol.ByteCount(4*i + 1)})
			_, count := check(gaps.root)
			if count != gaps.len {
				t.Fatalf("gap count=%d, want %d", count, gaps.len)
			}
			previous := protocol.ByteCount(-1)
			for _, gap := range gaps.match(byteInterval{End: protocol.MaxByteCount}) {
				if gap.Start <= previous {
					t.Fatal("shrinking a gap changed stream order")
				}
				previous = gap.End
			}
		}
		if gaps.root != nil || gaps.len != 0 {
			t.Fatal("removed gaps retained")
		}
	}
}
