package mvcc

import "sync"

type store struct {
	Mu             sync.Mutex
	compactMainRev int64
	currentRev     int64
}

func (s *store) compact(rev int64) {
	s.Mu.Lock()
	defer s.Mu.Unlock()
	s.compactMainRev = rev
}
