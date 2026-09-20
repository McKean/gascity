package beads

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
)

// directWriteRecorder collects onChange emissions for the direct-bd-write tests.
type directWriteRecorder struct {
	mu     sync.Mutex
	events []directWriteEvent
}

type directWriteEvent struct {
	eventType string
	beadID    string
	payload   json.RawMessage
}

func (r *directWriteRecorder) record(eventType, beadID string, payload json.RawMessage) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, directWriteEvent{eventType, beadID, payload})
}

func (r *directWriteRecorder) reset() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = nil
}

func (r *directWriteRecorder) snapshot() []directWriteEvent {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]directWriteEvent, len(r.events))
	copy(out, r.events)
	return out
}

// findOne returns the single emission of eventType for beadID, and fails when
// none or more than one landed: a lost event and a duplicated event are both
// defects here.
func (r *directWriteRecorder) findOne(t *testing.T, eventType, beadID string) directWriteEvent {
	t.Helper()
	var hits []directWriteEvent
	for _, e := range r.snapshot() {
		if e.eventType == eventType && e.beadID == beadID {
			hits = append(hits, e)
		}
	}
	switch len(hits) {
	case 1:
		return hits[0]
	case 0:
		t.Fatalf("no %s reached onChange for %s; got %v", eventType, beadID, r.snapshot())
	default:
		t.Fatalf("%s reached onChange %d times for %s; want exactly 1: %v", eventType, len(hits), beadID, hits)
	}
	return directWriteEvent{}
}

func (r *directWriteRecorder) decode(t *testing.T, e directWriteEvent) Bead {
	t.Helper()
	var b Bead
	if err := json.Unmarshal(e.payload, &b); err != nil {
		t.Fatalf("decode payload: %v: %s", err, e.payload)
	}
	return b
}

// liveRead is the read shape a controller serves continuously: ListQuery.Live
// forces the backing store's authoritative rows and routes them through
// refreshCachedBeads. It stands in for every live read a live city makes
// between two reconcile passes.
func liveRead(t *testing.T, cs *CachingStore) {
	t.Helper()
	if _, err := cs.List(ListQuery{AllowScan: true, Live: true}); err != nil {
		t.Fatalf("live list: %v", err)
	}
}

// TestDirectBackingWriteReachesOnChangeEvenWhenAReadObservesItFirst pins the
// emission a refinery close and a reviewer route depend on.
//
// Production shape (gt-48f, HQ gc-fdxn4b): the reviewer and the refinery write
// through `bd` straight against Dolt. Such a write fires no gc event and no
// cache notification, so the city event stream learns of it only from the diff
// the cache synthesizes between its own copy and the backing store's. The
// reconciler is the documented synthesizer, on a 30-120 s cadence.
//
// But a live controller serves reads continuously, and the live-list refresh
// path absorbs whatever the backing store returns. Absorbing without announcing
// consumes the diff: the reconcile pass that follows compares fresh against a
// cache that already agrees, finds nothing, and emits nothing. The route never
// reaches bead.updated; the close is worse, because the cached row is by then
// already status=closed and the eviction arm's `cached.Status != "closed"`
// guard suppresses bead.closed too. The change lands in the cache and never
// reaches the event stream at all.
//
// These cases therefore assert on the WHOLE window, the read plus the
// reconcile pass, rather than on which of the two announced. A consumer of the
// event stream does not care which path observed the change; it cares that
// exactly one event carries it.
func TestDirectBackingWriteReachesOnChangeEvenWhenAReadObservesItFirst(t *testing.T) {
	t.Parallel()

	t.Run("reviewer route", func(t *testing.T) {
		t.Parallel()

		mem := NewMemStore()
		seed, err := mem.Create(Bead{Title: "reviewed work", Status: "open"})
		if err != nil {
			t.Fatalf("Create: %v", err)
		}

		rec := &directWriteRecorder{}
		cs := NewCachingStoreForTest(mem, rec.record)
		if err := cs.Prime(context.Background()); err != nil {
			t.Fatalf("Prime: %v", err)
		}
		rec.reset()

		// The reviewer's route: assignee plus metadata, written straight to the
		// backing store the way `bd update` writes to Dolt.
		reviewer := "webapp/gastown.refinery"
		if err := mem.Update(seed.ID, UpdateOpts{Assignee: &reviewer}); err != nil {
			t.Fatalf("external route: %v", err)
		}
		if err := mem.SetMetadata(seed.ID, "review_status", "approved"); err != nil {
			t.Fatalf("external metadata: %v", err)
		}

		liveRead(t, cs)
		cs.runReconciliation()

		b := rec.decode(t, rec.findOne(t, "bead.updated", seed.ID))
		if b.Assignee != reviewer {
			t.Fatalf("bead.updated payload does not carry the route: assignee=%q", b.Assignee)
		}
		if b.Metadata["review_status"] != "approved" {
			t.Fatalf("bead.updated payload does not carry the review metadata: %v", b.Metadata)
		}
	})

	t.Run("refinery close", func(t *testing.T) {
		t.Parallel()

		mem := NewMemStore()
		seed, err := mem.Create(Bead{Title: "merged work", Status: "open"})
		if err != nil {
			t.Fatalf("Create: %v", err)
		}

		rec := &directWriteRecorder{}
		cs := NewCachingStoreForTest(mem, rec.record)
		if err := cs.Prime(context.Background()); err != nil {
			t.Fatalf("Prime: %v", err)
		}
		rec.reset()

		if err := mem.Close(seed.ID); err != nil {
			t.Fatalf("external close: %v", err)
		}

		liveRead(t, cs)
		cs.runReconciliation()

		b := rec.decode(t, rec.findOne(t, "bead.closed", seed.ID))
		if b.Status != "closed" {
			t.Fatalf("bead.closed payload does not carry the close: status=%q", b.Status)
		}
	})

	t.Run("bead created outside this process", func(t *testing.T) {
		t.Parallel()

		mem := NewMemStore()
		rec := &directWriteRecorder{}
		cs := NewCachingStoreForTest(mem, rec.record)
		if err := cs.Prime(context.Background()); err != nil {
			t.Fatalf("Prime: %v", err)
		}
		rec.reset()

		seed, err := mem.Create(Bead{Title: "filed by an agent", Status: "open"})
		if err != nil {
			t.Fatalf("external create: %v", err)
		}

		liveRead(t, cs)
		cs.runReconciliation()

		b := rec.decode(t, rec.findOne(t, "bead.created", seed.ID))
		if b.Title != "filed by an agent" {
			t.Fatalf("bead.created payload does not carry the bead: title=%q", b.Title)
		}
	})

	t.Run("the announced payload carries the dependency set the absorb kept", func(t *testing.T) {
		t.Parallel()

		// The controller records a cache emission and feeds it straight back in
		// through ApplyEventSnapshot, which reads the payload's dependency set
		// as authoritative coverage. A payload that omitted the edges the absorb
		// kept would wipe them from the cache, which is the ga-yoix1 loop.
		mem := NewMemStore()
		blocker, err := mem.Create(Bead{Title: "blocker", Status: "open"})
		if err != nil {
			t.Fatalf("Create blocker: %v", err)
		}
		seed, err := mem.Create(Bead{Title: "blocked work", Status: "open"})
		if err != nil {
			t.Fatalf("Create: %v", err)
		}
		if err := mem.DepAdd(seed.ID, blocker.ID, "blocks"); err != nil {
			t.Fatalf("DepAdd: %v", err)
		}

		rec := &directWriteRecorder{}
		cs := NewCachingStoreForTest(mem, rec.record)
		if err := cs.Prime(context.Background()); err != nil {
			t.Fatalf("Prime: %v", err)
		}
		rec.reset()

		reviewer := "webapp/gastown.refinery"
		if err := mem.Update(seed.ID, UpdateOpts{Assignee: &reviewer}); err != nil {
			t.Fatalf("external route: %v", err)
		}

		liveRead(t, cs)

		b := rec.decode(t, rec.findOne(t, "bead.updated", seed.ID))
		if len(depsFromBeadFields(b)) == 0 {
			t.Fatalf("announced payload dropped the dependency set: %+v", b)
		}
	})

	t.Run("a read that follows the reconcile pass does not announce it twice", func(t *testing.T) {
		t.Parallel()

		// The other arm of the race. When the pass wins, it announces and the
		// read that follows must find cache and backing already in agreement.
		// Two observers of one transition must still produce one event.
		mem := NewMemStore()
		seed, err := mem.Create(Bead{Title: "reviewed work", Status: "open"})
		if err != nil {
			t.Fatalf("Create: %v", err)
		}

		rec := &directWriteRecorder{}
		cs := NewCachingStoreForTest(mem, rec.record)
		if err := cs.Prime(context.Background()); err != nil {
			t.Fatalf("Prime: %v", err)
		}
		rec.reset()

		reviewer := "webapp/gastown.refinery"
		if err := mem.Update(seed.ID, UpdateOpts{Assignee: &reviewer}); err != nil {
			t.Fatalf("external route: %v", err)
		}

		cs.runReconciliation()
		liveRead(t, cs)

		rec.findOne(t, "bead.updated", seed.ID)
	})

	t.Run("a live read over an unchanged backing announces nothing", func(t *testing.T) {
		t.Parallel()

		mem := NewMemStore()
		if _, err := mem.Create(Bead{Title: "steady work", Status: "open"}); err != nil {
			t.Fatalf("Create: %v", err)
		}

		rec := &directWriteRecorder{}
		cs := NewCachingStoreForTest(mem, rec.record)
		if err := cs.Prime(context.Background()); err != nil {
			t.Fatalf("Prime: %v", err)
		}
		rec.reset()

		// Reads are the hot path. Announcing on every one of them would flood
		// the event stream, so the refresh must announce only real transitions.
		for range 5 {
			liveRead(t, cs)
		}
		cs.runReconciliation()

		if got := rec.snapshot(); len(got) != 0 {
			t.Fatalf("live reads over an unchanged backing emitted %d events: %v", len(got), got)
		}
	})
}
