package mvcc

import (
	"errors"
	"sync"
)

var ErrCompacted = errors.New("mvcc: required revision has been compacted")

type WatchID int64

type FilterFunc func(e Event) bool

type Event struct {
	Kv  KeyValue
	PrevKv *KeyValue
}

type KeyValue struct {
	Key      []byte
	CreateRevision int64
	ModRevision    int64
	Version        int64
	Value          []byte
}

type WatchResponse struct {
	WatchID         WatchID
	Events          []Event
	CompactRevision int64
}

type watcher struct {
	key    []byte
	end    []byte
	minRev int64
	id     WatchID
	ch     chan<- WatchResponse
	fcs    []FilterFunc
}

type watchableStore struct {
	Mu    sync.Mutex
	store *store

	synced   watcherGroup
	unsynced watcherGroup
}

type watcherGroup struct {
	watchers map[*watcher]struct{}
}

func (s *watchableStore) watch(key, end []byte, startRev int64, id WatchID, ch chan<- WatchResponse, fcs ...FilterFunc) (*watcher, error) {
	s.Mu.Lock()
	defer s.Mu.Unlock()

	wa := &watcher{
		key:    key,
		end:    end,
		minRev: startRev,
		id:     id,
		ch:     ch,
		fcs:    fcs,
	}

	s.store.Mu.Lock()
	defer s.store.Mu.Unlock()
	if startRev != 0 && startRev <= s.store.compactMainRev {
		return nil, ErrCompacted
	}

	if startRev == 0 || startRev > s.store.currentRev {
		s.synced.watchers[wa] = struct{}{}
	} else {
		s.unsynced.watchers[wa] = struct{}{}
	}

	return wa, nil
}

func (s *watchableStore) syncWatchers() int {
	s.Mu.Lock()
	defer s.Mu.Unlock()

	if len(s.unsynced.watchers) == 0 {
		return 0
	}

	s.store.Mu.Lock()
	defer s.store.Mu.Unlock()

	// In a real implementation, we would acquire a read transaction here.
	// For the purpose of this fix, we check if the watcher's revision has been compacted.
	for w := range s.unsynced.watchers {
		if w.minRev <= s.store.compactMainRev {
			select {
			case w.ch <- WatchResponse{WatchID: w.id, CompactRevision: s.store.compactMainRev}:
			default:
			}
			delete(s.unsynced.watchers, w)
			continue
		}
	}

	return len(s.unsynced.watchers)
}
