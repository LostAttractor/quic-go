// AVL balancing adapted from the previously vendored go-tree implementation.
/*
Copyright (c) 2017 Ross Oreto

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/

package quic

import "github.com/daeuniverse/quic-go/internal/protocol"

// frameSorterGaps stores disjoint missing byte intervals in an AVL tree.
// Shrinking an interval preserves its order, so it doesn't need reinsertion.
// Nodes and match storage are reused within one stream, bounded by its gap limit.
type frameSorterGaps struct {
	root, free *frameSorterGap
	len        int
	matches    []byteInterval
}

type frameSorterGap struct {
	interval    byteInterval
	left, right *frameSorterGap
	height      int8
}

func (g *frameSorterGaps) insert(interval byteInterval) {
	g.root = g.insertAt(g.root, interval)
}

func (g *frameSorterGaps) insertAt(n *frameSorterGap, interval byteInterval) *frameSorterGap {
	if n == nil {
		n = g.free
		if n == nil {
			n = new(frameSorterGap)
		} else {
			g.free = n.left
		}
		*n = frameSorterGap{interval: interval, height: 1}
		g.len++
		return n
	}
	switch {
	case interval.Start < n.interval.Start:
		n.left = g.insertAt(n.left, interval)
	case interval.Start > n.interval.Start:
		n.right = g.insertAt(n.right, interval)
	default:
		n.interval = interval
		return n
	}
	return n.rebalance()
}

func (g *frameSorterGaps) remove(interval byteInterval) {
	g.root = g.removeAt(g.root, interval.Start)
}

func (g *frameSorterGaps) removeAt(n *frameSorterGap, start protocol.ByteCount) *frameSorterGap {
	if n == nil {
		return nil
	}
	switch {
	case start < n.interval.Start:
		n.left = g.removeAt(n.left, start)
	case start > n.interval.Start:
		n.right = g.removeAt(n.right, start)
	default:
		if n.left == nil || n.right == nil {
			child := n.left
			if child == nil {
				child = n.right
			}
			*n = frameSorterGap{left: g.free}
			g.free = n
			g.len--
			return child
		}
		next := n.right
		for next.left != nil {
			next = next.left
		}
		n.interval = next.interval
		n.right = g.removeAt(n.right, next.interval.Start)
	}
	return n.rebalance()
}

// shrink only replaces an interval with a subset of itself. Neighboring gaps
// cannot cross it, so both the search order and AVL balance remain unchanged.
func (g *frameSorterGaps) shrink(old, interval byteInterval) {
	for n := g.root; n != nil; {
		switch {
		case old.Start < n.interval.Start:
			n = n.left
		case old.Start > n.interval.Start:
			n = n.right
		default:
			n.interval = interval
			return
		}
	}
	panic("shrinking missing frame sorter gap")
}

// match returns snapshots, valid until the next call. Tree mutations don't
// invalidate them while frameSorter replaces overlapping queued frames.
func (g *frameSorterGaps) match(interval byteInterval) []byteInterval {
	g.matches = g.root.appendMatches(g.matches[:0], interval)
	return g.matches
}

func (n *frameSorterGap) appendMatches(dst []byteInterval, interval byteInterval) []byteInterval {
	if n == nil {
		return dst
	}
	if n.interval.Start > interval.End {
		return n.left.appendMatches(dst, interval)
	}
	if n.interval.End < interval.Start {
		return n.right.appendMatches(dst, interval)
	}
	dst = n.left.appendMatches(dst, interval)
	dst = append(dst, n.interval)
	return n.right.appendMatches(dst, interval)
}

func (n *frameSorterGap) getHeight() int8 {
	if n == nil {
		return 0
	}
	return n.height
}

func (n *frameSorterGap) updateHeight() {
	n.height = max(n.left.getHeight(), n.right.getHeight()) + 1
}

func (n *frameSorterGap) balance() int8 { return n.left.getHeight() - n.right.getHeight() }

func (n *frameSorterGap) rebalance() *frameSorterGap {
	n.updateHeight()
	switch {
	case n.balance() > 1:
		if n.left.balance() < 0 {
			n.left = n.left.rotateLeft()
		}
		return n.rotateRight()
	case n.balance() < -1:
		if n.right.balance() > 0 {
			n.right = n.right.rotateRight()
		}
		return n.rotateLeft()
	}
	return n
}

func (n *frameSorterGap) rotateRight() *frameSorterGap {
	left := n.left
	left.right, n.left = n, left.right
	n.updateHeight()
	left.updateHeight()
	return left
}

func (n *frameSorterGap) rotateLeft() *frameSorterGap {
	right := n.right
	right.left, n.right = n, right.left
	n.updateHeight()
	right.updateHeight()
	return right
}
