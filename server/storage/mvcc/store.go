package mvcc

import "sync"

type store struct {
	Mu             sync.Mutex
	compactMainRev int64
	currentRev     int64
	onCompact      func(rev int64)
}

func (s *store) compact(rev int64) {
	s.Mu.Lock()
	s.compactMainRev = rev
	onCompact := s.onCompact
	s.Mu.Unlock()
	if onCompact != nil {
		onCompact(rev)
	}
}
