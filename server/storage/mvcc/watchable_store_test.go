package mvcc

import (
	"testing"
	"time"
)

func TestConcurrentCompactionAndWatch(t *testing.T) {
	st := &store{
		currentRev: 10,
	}
	store := &watchableStore{
		store: st,
		synced: watcherGroup{
			watchers: make(map[*watcher]struct{}),
		},
		unsynced: watcherGroup{
			watchers: make(map[*watcher]struct{}),
		},
	}

	// Simulate concurrent compaction and watch creation
	ch := make(chan WatchResponse, 1)
	go func() {
		time.Sleep(10 * time.Millisecond)
		st.compact(5)
	}()

	_, err := store.watch([]byte("foo"), nil, 5, 1, ch)
	if err == nil {
		// If watch creation succeeded, syncWatchers should catch it if compaction has progressed
		store.syncWatchers()
		select {
		case resp := <-ch:
			if resp.CompactRevision != 5 {
				t.Errorf("expected CompactRevision 5, got %d", resp.CompactRevision)
			}
		default:
			// If no response was sent, it means the watch was established without receiving ErrCompacted
			// which is a failure.
			t.Errorf("expected watch to fail with ErrCompacted")
		}
	} else if err != ErrCompacted {
		t.Errorf("expected ErrCompacted, got %v", err)
	}
}
