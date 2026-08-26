package quic

import (
	"errors"

	"github.com/daeuniverse/quic-go/internal/protocol"
)

// byteInterval is an interval from one ByteCount to the other
type byteInterval struct {
	Start protocol.ByteCount
	End   protocol.ByteCount
}

type frameSorterEntry struct {
	Data   []byte
	DoneCb func()
}

type frameSorter struct {
	queue   map[protocol.ByteCount]frameSorterEntry
	readPos protocol.ByteCount
	gaps    frameSorterGaps
}

var errDuplicateStreamData = errors.New("duplicate stream data")

func newFrameSorter() *frameSorter {
	s := frameSorter{
		queue: make(map[protocol.ByteCount]frameSorterEntry),
	}
	s.gaps.insert(byteInterval{Start: 0, End: protocol.MaxByteCount})
	return &s
}

func (s *frameSorter) Push(data []byte, offset protocol.ByteCount, doneCb func()) error {
	err := s.push(data, offset, doneCb)
	if err == errDuplicateStreamData {
		if doneCb != nil {
			doneCb()
		}
		return nil
	}
	return err
}

func (s *frameSorter) push(data []byte, offset protocol.ByteCount, doneCb func()) error {
	if len(data) == 0 {
		return errDuplicateStreamData
	}

	start := offset
	end := offset + protocol.ByteCount(len(data))
	// In-order data only advances the one remaining gap. No overlap search or
	// queued-frame replacement is needed, even before the reader catches up.
	if gap := s.gaps.root; s.gaps.len == 1 && start == gap.interval.Start && end < gap.interval.End {
		gap.interval.Start = end
		s.queue[start] = frameSorterEntry{Data: data, DoneCb: doneCb}
		return nil
	}
	covInterval := byteInterval{Start: start, End: end}

	gaps := s.gaps.match(covInterval)

	if len(gaps) == 0 {
		// No overlap with any existing gap
		return errDuplicateStreamData
	}

	startGap := gaps[0]
	endGap := gaps[len(gaps)-1]
	startGapEqualsEndGap := len(gaps) == 1

	if startGapEqualsEndGap && end <= startGap.Start {
		return errDuplicateStreamData
	}

	startsInGap := covInterval.Start >= startGap.Start && covInterval.Start <= startGap.End
	endsInGap := covInterval.End >= endGap.Start && covInterval.End < endGap.End

	startGapEnd := startGap.End // save it, in case startGap is modified
	endGapStart := endGap.Start // save it, in case endGap is modified
	endGapEnd := endGap.End     // save it, in case endGap is modified

	var adjustedStartGapEnd bool
	var wasCut bool

	pos := start
	var hasReplacedAtLeastOne bool
	for {
		oldEntry, ok := s.queue[pos]
		if !ok {
			break
		}
		oldEntryLen := protocol.ByteCount(len(oldEntry.Data))
		if end-pos > oldEntryLen || (hasReplacedAtLeastOne && end-pos == oldEntryLen) {
			// The existing frame is shorter than the new frame. Replace it.
			delete(s.queue, pos)
			pos += oldEntryLen
			hasReplacedAtLeastOne = true
			if oldEntry.DoneCb != nil {
				oldEntry.DoneCb()
			}
		} else {
			if !hasReplacedAtLeastOne {
				return errDuplicateStreamData
			}
			// The existing frame is longer than the new frame.
			// Cut the new frame such that the end aligns with the start of the existing frame.
			data = data[:pos-start]
			end = pos
			wasCut = true
			break
		}
	}

	if !startsInGap && !hasReplacedAtLeastOne {
		// cut the frame, such that it starts at the start of the gap
		data = data[startGap.Start-start:]
		start = startGap.Start
		wasCut = true
	}
	if start <= startGap.Start {
		if end >= startGap.End {
			// The frame covers the whole startGap. Delete the gap.
			s.gaps.remove(startGap)
		} else {
			s.gaps.shrink(startGap, byteInterval{Start: end, End: startGap.End})
		}
	} else if !hasReplacedAtLeastOne {
		s.gaps.shrink(startGap, byteInterval{Start: startGap.Start, End: start})
		adjustedStartGapEnd = true
	}

	if !startGapEqualsEndGap {
		s.deleteConsecutive(startGapEnd)
		for _, g := range gaps[1:] {
			if g.End >= endGapStart {
				break
			}
			s.deleteConsecutive(g.End)
			s.gaps.remove(g)
		}
	}

	if !endsInGap && start != endGapEnd && end > endGapEnd {
		// cut the frame, such that it ends at the end of the gap
		data = data[:endGapEnd-start]
		end = endGapEnd
		wasCut = true
	}
	if end == endGapEnd {
		if !startGapEqualsEndGap {
			// The frame covers the whole endGap. Delete the gap.
			s.gaps.remove(endGap)
		}
	} else {
		if startGapEqualsEndGap && adjustedStartGapEnd {
			// The frame split the existing gap into two.
			s.gaps.insert(byteInterval{Start: end, End: startGapEnd})
		} else if !startGapEqualsEndGap {
			s.gaps.shrink(endGap, byteInterval{Start: end, End: endGap.End})
		}
	}

	if wasCut && len(data) < protocol.MinStreamFrameBufferSize {
		newData := make([]byte, len(data))
		copy(newData, data)
		data = newData
		if doneCb != nil {
			doneCb()
			doneCb = nil
		}
	}

	if s.gaps.len > protocol.MaxStreamFrameSorterGaps {
		return errors.New("too many gaps in received data")
	}

	s.queue[start] = frameSorterEntry{Data: data, DoneCb: doneCb}
	return nil
}

// deleteConsecutive deletes consecutive frames from the queue, starting at pos
func (s *frameSorter) deleteConsecutive(pos protocol.ByteCount) {
	for {
		oldEntry, ok := s.queue[pos]
		if !ok {
			break
		}
		oldEntryLen := protocol.ByteCount(len(oldEntry.Data))
		delete(s.queue, pos)
		if oldEntry.DoneCb != nil {
			oldEntry.DoneCb()
		}
		pos += oldEntryLen
	}
}

func (s *frameSorter) Pop() (protocol.ByteCount, []byte, func()) {
	entry, ok := s.queue[s.readPos]
	if !ok {
		return s.readPos, nil, nil
	}
	delete(s.queue, s.readPos)
	offset := s.readPos
	s.readPos += protocol.ByteCount(len(entry.Data))
	return offset, entry.Data, entry.DoneCb
}

var errTooLittleData = errors.New("too little data")

// Peek returns the next consecutive data without removing it from the sorter.
// If there isn't enough consecutive data available, errTooLittleData is returned.
// Peek is not thread-safe, and must not be called concurrently with Push or Pop.
func (s *frameSorter) Peek(offset protocol.ByteCount, p []byte) error {
	if len(p) == 0 {
		return nil
	}

	// first, check if we have enough consecutive data available
	pos := offset
	remaining := len(p)
	for remaining > 0 {
		entry, ok := s.queue[pos]
		if !ok {
			return errTooLittleData
		}
		entryLen := len(entry.Data)
		if remaining <= entryLen {
			break // enough data available
		}
		remaining -= entryLen
		pos += protocol.ByteCount(entryLen)
	}

	pos = offset
	var copied int
	for copied < len(p) {
		entry := s.queue[pos] // the entry is guaranteed to exist from the check above
		copied += copy(p[copied:], entry.Data)
		pos += protocol.ByteCount(len(entry.Data))
	}
	return nil
}

// HasMoreData says if there is any more data queued at *any* offset.
func (s *frameSorter) HasMoreData() bool {
	return len(s.queue) > 0
}
