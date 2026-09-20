package beads

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/gastownhall/gascity/internal/beadmeta"
)

// List returns beads matching the query. Active-bead queries are served from
// cache when available. IncludeClosed queries merge cached active results with
// backing-store history when possible, preserving partial backing rows when bd
// reports corrupt entries and returning partial-result errors when backing
// history cannot be fully read.
func (c *CachingStore) List(query ListQuery) ([]Bead, error) {
	if !query.HasFilter() && !query.AllowScan {
		return nil, fmt.Errorf("listing beads: %w", ErrQueryRequiresScan)
	}
	if query.Live || query.ParentID != "" {
		c.mu.RLock()
		startSeq := c.mutationSeq
		c.mu.RUnlock()
		items, err := c.backing.List(query)
		if err == nil {
			items = c.refreshCachedBeads(query, startSeq, items)
		}
		return items, err
	}

	// Active-bead path: serve from cache after a bounded per-ID refresh of any
	// dirty rows. PrimeActive loads the open + in_progress subset, so queries
	// explicitly filtered to either status are complete before the full prime;
	// broader nonclosed queries require cacheLive. On overlay error the read
	// takes the old full-scan fallback.
	var cached []Bead
	if err := c.readCacheWithOverlay(func() bool {
		return c.cacheServableForListQueryLocked(query)
	}, func(suppressed map[string]struct{}) {
		cached = make([]Bead, 0, len(c.beads))
		for _, b := range c.beads {
			if _, gone := suppressed[b.ID]; gone {
				continue
			}
			if !query.Matches(b) {
				continue
			}
			cached = append(cached, cloneBead(b))
		}
	}); err == nil {
		finish := func(items []Bead, err error) ([]Bead, error) {
			sortBeadsForQuery(items, query.Sort)
			if query.Limit > 0 && len(items) > query.Limit {
				items = items[:query.Limit]
			}
			return items, err
		}

		if !query.IncludesClosed() {
			return finish(cached, nil)
		}

		// The cache never has a complete closed-only or parent-history view, so
		// preserve the old backing-store behavior for those query shapes.
		if query.Status == "closed" || query.ParentID != "" {
			return c.backing.List(liveListQuery(query))
		}

		all, err := c.backing.List(liveListQuery(query))
		if err != nil {
			if !IsPartialResult(err) {
				c.recordProblem("list include closed backing failure", err)
				return finish(cached, &PartialResultError{
					Op:  "cache list include closed",
					Err: err,
				})
			}
		}

		seen := make(map[string]bool, len(cached))
		for _, b := range cached {
			seen[b.ID] = true
		}
		for _, b := range all {
			if seen[b.ID] {
				continue
			}
			cached = append(cached, b)
			seen[b.ID] = true
		}
		return finish(cached, err)
	}
	return c.backing.List(liveListQuery(query))
}

func liveListQuery(query ListQuery) ListQuery {
	query.Live = true
	return query
}

// Count returns the number of beads List would return for query, minus
// beads whose Type is in excludeTypes. Active-bead queries are answered
// from the in-memory cache when it is live and clean; everything else
// (Live queries, ParentID lookups, closed history, dirty/unprimed cache)
// delegates to the backing store's Counter. Backing stores without a
// Counter return ErrCountUnsupported so callers can fall back to List. Limited
// queries are unsupported because Count must match List cardinality, including
// List's post-sort limit cap.
func (c *CachingStore) Count(ctx context.Context, query ListQuery, excludeTypes ...string) (int, error) {
	if !query.HasFilter() && !query.AllowScan {
		return 0, fmt.Errorf("counting beads: %w", ErrQueryRequiresScan)
	}
	if query.Limit > 0 {
		return 0, fmt.Errorf("counting beads: %w", ErrCountUnsupported)
	}
	if !query.Live && query.ParentID == "" && !query.IncludesClosed() {
		n, ok, err := c.cachedCountContext(ctx, query, excludeTypes)
		if err != nil {
			return 0, err
		}
		if ok {
			return n, nil
		}
	}
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
	}
	counter, ok := c.backing.(Counter)
	if !ok {
		return 0, fmt.Errorf("counting beads: backing store: %w", ErrCountUnsupported)
	}
	return counter.Count(ctx, liveListQuery(query), excludeTypes...)
}

// SawRows implements RowWitness for a cached store, which is the shape the
// API server actually holds: the witness has to answer for the wrapper, not
// only for whatever sits behind it.
//
// A populated cache is proof on its own — the rows in it came from this
// ledger — and it is the only evidence available when the backing store
// cannot witness itself, which is every backend but the bd CLI one. Falling
// through to the backing store covers the opposite case, a cache that is cold
// or unprimed on a scope bd has already answered with rows.
//
// Both arms only ever prove rows are present, matching the one-directional
// contract on RowWitness: false here means no evidence, never an empty ledger.
func (c *CachingStore) SawRows() bool {
	c.mu.RLock()
	cached := len(c.beads)
	c.mu.RUnlock()
	if cached > 0 {
		return true
	}
	witness, ok := c.backing.(RowWitness)
	return ok && witness.SawRows()
}

// cachedCountContext serves only a clean active snapshot. Dirty overlays use
// context-blind Store.Get calls, so a deadline-sensitive Count delegates those
// cases to the backing Counter instead. Lock acquisition and the scan both
// observe ctx, ensuring a cache writer cannot strand the caller's goroutine.
func (c *CachingStore) cachedCountContext(ctx context.Context, query ListQuery, excludeTypes []string) (int, bool, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return 0, false, err
	}
	if !c.mu.TryRLock() {
		ticker := time.NewTicker(time.Millisecond)
		defer ticker.Stop()
		for !c.mu.TryRLock() {
			select {
			case <-ctx.Done():
				return 0, false, ctx.Err()
			case <-ticker.C:
			}
		}
	}
	defer c.mu.RUnlock()

	if !c.cacheServableForListQueryLocked(query) || len(c.dirty) > 0 {
		return 0, false, nil
	}
	var n int
	for _, b := range c.beads {
		if err := ctx.Err(); err != nil {
			return 0, false, err
		}
		if query.Matches(b) && !slices.Contains(excludeTypes, b.Type) {
			n++
		}
	}
	if err := ctx.Err(); err != nil {
		return 0, false, err
	}
	return n, true, nil
}

// CachedList returns query results from the in-memory cache only. The boolean
// reports whether the cache was initialized and clean enough to answer without
// touching the backing store.
//
// This strict cache-only handle intentionally keeps the conservative
// "dirty ⇒ decline" contract: it must answer without any backing I/O and
// without serving a row it is not certain matches the backing. The bounded
// per-ID dirty overlay (readCacheWithOverlay) applies only to the read paths
// that already fall back to the backing store (List/Count/Ready), where a
// refresh-and-serve is invisible to callers.
func (c *CachingStore) CachedList(query ListQuery) ([]Bead, bool) {
	if query.IncludesClosed() {
		return nil, false
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	if !c.cacheServableForListQueryLocked(query) || len(c.dirty) > 0 {
		return nil, false
	}
	return c.collectCachedListLocked(query), true
}

// ObservedList returns a detached active-only cache census and an opaque stamp
// that may be conditionally consumed with WithCurrentObservation. It never
// performs backing-store I/O. The stamp fences only this process's cache
// projection; it does not certify durable-store lineage or event delivery.
func (c *CachingStore) ObservedList(query ListQuery) ([]Bead, CacheObservation, bool) {
	if query.Validate() != nil ||
		(!query.HasFilter() && !query.AllowScan) ||
		query.Live ||
		query.IncludesClosed() ||
		query.ParentID != "" ||
		len(query.ParentIDs) > 0 {
		return nil, CacheObservation{}, false
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	if !c.observationAdmissibleLocked() {
		return nil, CacheObservation{}, false
	}
	return c.collectCachedListLocked(query), CacheObservation{owner: c, revision: c.observationRevision}, true
}

// WithCurrentObservation runs publish while holding the originating cache's
// read lock only when observation still describes a clean active cache. The
// callback must perform bounded in-memory work and must not call the cache,
// backing store, or wait for other work.
func (c *CachingStore) WithCurrentObservation(observation CacheObservation, publish func() error) (bool, error) {
	if publish == nil {
		return false, fmt.Errorf("using cache observation: nil callback")
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	if observation.owner != c || observation.revision == 0 || observation.revision != c.observationRevision ||
		!c.observationAdmissibleLocked() {
		return false, nil
	}
	return true, publish()
}

// observationAdmissibleLocked reports whether an active-only cache census can
// be observed and conditionally used. Caller must hold c.mu.
func (c *CachingStore) observationAdmissibleLocked() bool {
	return (c.state == cacheLive || c.state == cachePartial) &&
		c.primePartialErr == nil &&
		len(c.dirty) == 0 &&
		c.observationRevision != 0
}

// collectCachedListLocked materializes CachedList and ObservedList results.
// Caller must hold c.mu for reading or writing.
func (c *CachingStore) collectCachedListLocked(query ListQuery) []Bead {
	cached := make([]Bead, 0, len(c.beads))
	for _, b := range c.beads {
		if !query.Matches(b) {
			continue
		}
		cached = append(cached, cloneBead(b))
	}
	sortBeadsForQuery(cached, query.Sort)
	if query.Limit > 0 && len(cached) > query.Limit {
		cached = cached[:query.Limit]
	}
	return cached
}

// cacheServableForListQueryLocked refuses to treat PrimeActive's open and
// in-progress subset as a complete answer to a broader nonclosed query. A full
// prime may answer every nonclosed status; a partial prime may answer only the
// two status filters it actually loaded. Caller must hold c.mu.
func (c *CachingStore) cacheServableForListQueryLocked(query ListQuery) bool {
	if !c.cacheServableLocked() {
		return false
	}
	if c.state == cacheLive {
		return true
	}
	return slices.Contains(partialPrimeStatuses, query.Status)
}

// refreshWorkSet is the backing-store state refreshCachedBeads gathers before
// taking the write lock: the rows the live list returned, plus the per-ID Get
// results for cached rows that list did not cover.
type refreshWorkSet struct {
	items                []Bead
	refreshedParents     map[string]Bead
	removedParents       map[string]struct{}
	refreshedLiveMissing map[string]Bead
	removedLiveMissing   map[string]struct{}
}

func (w refreshWorkSet) empty() bool {
	return len(w.items) == 0 && len(w.refreshedParents) == 0 && len(w.removedParents) == 0 &&
		len(w.refreshedLiveMissing) == 0 && len(w.removedLiveMissing) == 0
}

func (c *CachingStore) refreshCachedBeads(query ListQuery, startSeq uint64, items []Bead) []Bead {
	ws := refreshWorkSet{
		items:                items,
		refreshedParents:     make(map[string]Bead),
		removedParents:       make(map[string]struct{}),
		refreshedLiveMissing: make(map[string]Bead),
		removedLiveMissing:   make(map[string]struct{}),
	}
	for _, id := range c.staleParentCacheIDs(query.ParentID, items) {
		fresh, err := c.backing.Get(id)
		switch {
		case err == nil:
			ws.refreshedParents[id] = cloneBead(fresh)
		case errors.Is(err, ErrNotFound):
			ws.removedParents[id] = struct{}{}
		default:
			c.recordProblem("refresh parent cache during list", fmt.Errorf("%s: %w", id, err))
		}
	}
	for _, id := range c.staleLiveCacheIDs(query, items) {
		fresh, err := c.backing.Get(id)
		switch {
		case err == nil:
			ws.refreshedLiveMissing[id] = cloneBead(fresh)
		case errors.Is(err, ErrNotFound):
			ws.removedLiveMissing[id] = struct{}{}
		default:
			c.recordProblem("refresh live cache during list", fmt.Errorf("%s: %w", id, err))
		}
	}
	if ws.empty() {
		return items
	}
	refreshed, notifications := c.applyRefreshWorkSetLocked(query, startSeq, ws)
	// Emitted after the write lock is released: onChange is a caller-supplied
	// callback (the controller records an event from it) and must never run
	// under c.mu, the same discipline runReconciliation follows.
	c.notifyChanges(notifications)
	return refreshed
}

// applyRefreshWorkSetLocked installs a live read's backing state into the cache
// and returns both the rows the read should serve and the transitions the read
// observed. It takes c.mu itself.
//
// Announcing here is what keeps a foreign write from vanishing: a `bd` close
// or route written straight against Dolt fires no gc event and no cache
// notification. The reconcile pass is the documented synthesizer of those
// events, but it synthesizes them from the diff between the cache and the
// backing store, and a live read that absorbs first consumes that diff: the
// next pass compares fresh against a cache that already agrees and emits
// nothing. Worse for a close, because the cached row is closed by then and
// mergeSnapshotLocked's eviction arm suppresses bead.closed for an
// already-closed cached row, so the close never reaches the stream at all
// (gt-48f: a refinery close and a reviewer route stayed invisible for hours on
// a live city, and surfaced only when a pass happened to win the race).
//
// Whichever observer sees the change first announces it, and only one of them
// can: after this absorbs, the reconcile diff is empty; if the pass wins
// instead, this read finds cache and backing already in agreement and stays
// quiet. refreshAbsorbNotification holds the shared classification so the two
// observers cannot drift.
func (c *CachingStore) applyRefreshWorkSetLocked(query ListQuery, startSeq uint64, ws refreshWorkSet) ([]Bead, []cacheNotification) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.state != cacheLive && c.state != cachePartial {
		return ws.items, nil
	}
	fullyPrimed := c.state == cacheLive
	now := time.Now()
	var notifications []cacheNotification
	// Must run BEFORE the absorb: it classifies against the cached row the
	// absorb is about to replace, and reads the dependency set the absorb is
	// about to keep.
	announceAbsorb := func(id string, fresh Bead) {
		cached, cachedExists := c.beads[id]
		if eventType := refreshAbsorbNotification(cached, cachedExists, fresh, fullyPrimed); eventType != "" {
			notifications = append(notifications, cacheNotification{
				eventType: eventType,
				bead:      c.announcedBeadLocked(id, fresh),
			})
		}
	}
	announceEvict := func(id string) {
		cached, ok := c.beads[id]
		if !ok {
			return
		}
		if n, emit := refreshEvictNotification(cached); emit {
			notifications = append(notifications, n)
		}
	}

	refreshed := make([]Bead, 0, len(ws.items))
	for _, item := range ws.items {
		if c.deletedSeq[item.ID] > startSeq {
			continue
		}
		if c.beadSeq[item.ID] > startSeq {
			current, ok := c.beads[item.ID]
			if ok && query.Matches(current) {
				refreshed = append(refreshed, cloneBead(current))
			}
			continue
		}
		if current, keep := c.recentLocalBeadConflictLocked(item.ID, item, now, false); keep {
			if query.Matches(current) {
				refreshed = append(refreshed, current)
			}
			continue
		}
		if c.beadSeq[item.ID] == startSeq {
			current, ok := c.beads[item.ID]
			if ok && current.Status == "closed" && item.Status != "closed" {
				continue
			}
		}
		announceAbsorb(item.ID, item)
		c.absorbFreshLocked(item.ID, item, now, absorbOpts{
			depsMode:   depsFromFieldsIfCarried,
			seqMode:    seqClearGuarded,
			clearDirty: true,
		})
		if query.Matches(item) {
			refreshed = append(refreshed, cloneBead(item))
		}
	}
	for id, bead := range ws.refreshedParents {
		if c.deletedSeq[id] > startSeq || c.beadSeq[id] > startSeq {
			continue
		}
		if _, keep := c.recentLocalBeadConflictLocked(id, bead, now, false); keep {
			continue
		}
		announceAbsorb(id, bead)
		c.absorbFreshLocked(id, bead, now, absorbOpts{
			depsMode:   depsFromFieldsIfCarried,
			seqMode:    seqClearGuarded,
			clearDirty: true,
		})
	}
	for id := range ws.removedParents {
		if c.deletedSeq[id] > startSeq || c.beadSeq[id] > startSeq {
			continue
		}
		if current, ok := c.beads[id]; ok && current.Status != "closed" && recentLocalMutation(c.localBeadAt[id], now) {
			continue
		}
		announceEvict(id)
		c.evictLocked(id)
	}
	for id, bead := range ws.refreshedLiveMissing {
		if c.deletedSeq[id] > startSeq || c.beadSeq[id] > startSeq {
			continue
		}
		if _, keep := c.recentLocalBeadConflictLocked(id, bead, now, false); keep {
			continue
		}
		announceAbsorb(id, bead)
		c.absorbFreshLocked(id, bead, now, absorbOpts{
			depsMode:   depsFromFieldsIfCarried,
			seqMode:    seqClearGuarded,
			clearDirty: true,
		})
	}
	for id := range ws.removedLiveMissing {
		if c.deletedSeq[id] > startSeq || c.beadSeq[id] > startSeq {
			continue
		}
		if current, ok := c.beads[id]; ok && current.Status != "closed" && recentLocalMutation(c.localBeadAt[id], now) {
			continue
		}
		announceEvict(id)
		c.evictLocked(id)
	}
	c.markFreshLocked(time.Now())
	c.updateStatsLocked()
	return refreshed, notifications
}

// announcedBeadLocked builds the payload for a read-path emission: the fresh
// row, with the cache's dependency set filled in when the row carries none.
//
// The controller feeds a cache emission straight back into the cache through
// ApplyEventSnapshot, which takes the payload's dependency set as authoritative
// coverage. A live list row need not carry dependency fields, and the absorb's
// depsFromFieldsIfCarried keeps the cached set in exactly that case, so
// announcing the bare row would assert an empty dependency set the absorb never
// installed and wipe live edges out of the cache. That is the ga-yoix1 failure
// mode: the cleared coverage fences the row out of the next reconcile pass, and
// the pass after that reads the cleared state as a change and emits again.
//
// Filling from c.deps makes the payload describe the state the absorb leaves
// behind, which is what an authoritative snapshot has to mean. Caller must hold
// c.mu and call this BEFORE the absorb, while c.deps still holds the set the
// absorb is about to keep.
func (c *CachingStore) announcedBeadLocked(id string, fresh Bead) Bead {
	b := cloneBead(fresh)
	if beadCarriesDependencyFields(b) {
		return b
	}
	if cachedDeps, ok := c.deps[id]; ok && len(cachedDeps) > 0 {
		b.Dependencies = cloneDeps(cachedDeps)
	}
	return b
}

func (c *CachingStore) staleParentCacheIDs(parentID string, fresh []Bead) []string {
	if parentID == "" {
		return nil
	}

	freshIDs := make(map[string]struct{}, len(fresh))
	for _, item := range fresh {
		freshIDs[item.ID] = struct{}{}
	}

	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.state != cacheLive && c.state != cachePartial {
		return nil
	}

	var stale []string
	for id, bead := range c.beads {
		if bead.ParentID != parentID {
			continue
		}
		if _, ok := freshIDs[id]; ok {
			continue
		}
		stale = append(stale, id)
	}
	return stale
}

func (c *CachingStore) staleLiveCacheIDs(query ListQuery, fresh []Bead) []string {
	if !query.Live || query.Limit > 0 || query.IncludesClosed() {
		return nil
	}

	freshIDs := make(map[string]struct{}, len(fresh))
	for _, item := range fresh {
		freshIDs[item.ID] = struct{}{}
	}

	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.state != cacheLive && c.state != cachePartial {
		return nil
	}

	var stale []string
	for id, bead := range c.beads {
		if _, ok := freshIDs[id]; ok {
			continue
		}
		if !query.Matches(bead) {
			continue
		}
		stale = append(stale, id)
	}
	return stale
}

// ListOpen returns all cached beads, optionally filtered by status.
func (c *CachingStore) ListOpen(status ...string) ([]Bead, error) {
	query := ListQuery{AllowScan: true}
	if len(status) > 0 {
		query.Status = status[0]
	}
	return c.List(query)
}

// Get returns a single bead by ID from the cache or backing store.
func (c *CachingStore) Get(id string) (Bead, error) {
	c.mu.RLock()
	if _, deleted := c.deletedSeq[id]; deleted {
		c.mu.RUnlock()
		return Bead{}, ErrNotFound
	}
	if _, mutated := c.beadSeq[id]; mutated {
		if _, dirty := c.dirty[id]; !dirty {
			if b, ok := c.beads[id]; ok {
				c.mu.RUnlock()
				return cloneBead(b), nil
			}
		}
	}
	if c.state == cacheLive || c.state == cachePartial {
		if _, ok := c.dirty[id]; ok {
			startSeq := c.mutationSeq
			c.mu.RUnlock()
			fresh, err := c.backing.Get(id)
			if err != nil {
				return Bead{}, err
			}
			c.mu.Lock()
			if c.state != cacheLive && c.state != cachePartial {
				c.mu.Unlock()
				return fresh, nil
			}
			switch {
			case c.deletedSeq[id] > startSeq:
				c.mu.Unlock()
				return Bead{}, ErrNotFound
			case c.beadSeq[id] > startSeq:
				if _, stillDirty := c.dirty[id]; stillDirty {
					c.mu.Unlock()
					return c.backing.Get(id)
				}
				if current, ok := c.beads[id]; ok {
					c.mu.Unlock()
					return cloneBead(current), nil
				}
				c.mu.Unlock()
				return Bead{}, ErrNotFound
			}
			// Absorbs without announcing, unlike the live-list refresh above.
			// This arm is reached only for a DIRTY id, and dirty is set solely
			// by a local write whose post-write Get failed, a write that has
			// already emitted its own bead.updated (CachingStore.Update) or had
			// no cached row to emit from. Confirming it here repairs this
			// process's own write; announcing again would duplicate the event.
			c.absorbFreshLocked(id, fresh, time.Now(), absorbOpts{
				depsMode:   depsFromFields,
				seqMode:    seqClearBeadSeqOnly,
				clearDirty: true,
			})
			c.markFreshLocked(time.Now())
			c.updateStatsLocked()
			c.mu.Unlock()
			return fresh, nil
		}
		if b, ok := c.beads[id]; ok {
			c.mu.RUnlock()
			return cloneBead(b), nil
		}
		c.mu.RUnlock()
		return c.backing.Get(id)
	}
	c.mu.RUnlock()
	return c.backing.Get(id)
}

// Ready returns open beads whose blocking deps are all closed.
func (c *CachingStore) Ready(query ...ReadyQuery) ([]Bead, error) {
	if readyQueryFromArgs(query) != (ReadyQuery{}) {
		return c.backing.Ready(query...)
	}
	var (
		statusByID      map[string]string
		workOutcomeByID map[string]string
		depsByID        map[string][]Dep
		openBeads       []Bead
		unanswerable    bool
	)
	// Ready requires a fully live cache with complete dependency coverage and a
	// ready projection the backing store can actually serve; the overlay
	// refreshes any dirty rows first, then computes readiness from the cache.
	// On overlay error the read takes the old full backing.Ready scan.
	if err := c.readCacheWithOverlay(
		func() bool {
			return c.state == cacheLive && c.depsComplete && c.primePartialErr == nil &&
				!c.readyReadsMustGoLive()
		},
		func(suppressed map[string]struct{}) {
			statusByID = make(map[string]string, len(c.beads))
			workOutcomeByID = make(map[string]string, len(c.beads))
			openBeads = make([]Bead, 0, len(c.beads))
			now := time.Now().UTC()
			for _, b := range c.beads {
				if _, gone := suppressed[b.ID]; gone {
					continue
				}
				statusByID[b.ID] = b.Status
				workOutcomeByID[b.ID] = b.Metadata[beadmeta.WorkOutcomeMetadataKey]
				if IsReadyCandidate(b, now) {
					if c.readyProjectionUnknownLocked(b.ID) {
						unanswerable = true
						return
					}
					openBeads = append(openBeads, cloneBead(b))
				}
			}
			depsByID = make(map[string][]Dep, len(openBeads))
			for _, b := range openBeads {
				depsByID[b.ID] = cloneDeps(c.deps[b.ID])
			}
		},
	); err != nil {
		return c.backing.Ready(query...)
	}
	if unanswerable {
		// One candidate whose verdict the cache cannot vouch for costs this
		// read the cache, not correctness: the live scan is slower and right.
		return c.backing.Ready(query...)
	}

	var result []Bead
	for _, b := range openBeads {
		if cachedBeadReady(b, statusByID, workOutcomeByID, depsByID[b.ID]) {
			result = append(result, cloneBead(b))
		}
	}
	// c.beads is a map, so the scan above yields a different order per
	// call; impose the canonical ready order so cache-served results
	// match the SQL-backed ready readers (#3208).
	sortBeadsReadyOrder(result)
	return result, nil
}

// ReadyContext answers only from the dependency-complete active cache. It
// deliberately does not fall back to the context-blind backing Ready method:
// deadline-sensitive callers must receive ErrCacheUnavailable instead of
// abandoning database work after their context expires.
func (c *CachingStore) ReadyContext(ctx context.Context, query ...ReadyQuery) ([]Bead, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	rows, err := c.cachedReadyCompleteOnly(ctx, readyQueryFromArgs(query))
	if err != nil {
		return rows, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return rows, nil
}

// CachedReady returns ready beads from the in-memory active read model.
// The boolean reports whether the cache was initialized enough to answer
// without touching the backing store. Unlike Ready, this can answer from a
// partial active cache only when each open bead has known dependency coverage.
//
// Like CachedList, this strict cache-only handle keeps the conservative
// "dirty ⇒ decline" contract so a caller relying on cache-only semantics never
// observes a row refreshed behind its back or a stale ready candidate (#2210).
func (c *CachingStore) CachedReady() ([]Bead, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.state != cacheLive && c.state != cachePartial {
		return nil, false
	}
	if c.primePartialErr != nil || len(c.dirty) > 0 || c.readyReadsMustGoLive() {
		return nil, false
	}

	statusByID := make(map[string]string, len(c.beads))
	workOutcomeByID := make(map[string]string, len(c.beads))
	openBeads := make([]Bead, 0, len(c.beads))
	now := time.Now().UTC()
	for _, b := range c.beads {
		statusByID[b.ID] = b.Status
		workOutcomeByID[b.ID] = b.Metadata[beadmeta.WorkOutcomeMetadataKey]
		if IsReadyCandidate(b, now) {
			if c.readyProjectionUnknownLocked(b.ID) {
				return nil, false
			}
			openBeads = append(openBeads, cloneBead(b))
		}
	}

	result := make([]Bead, 0, len(openBeads))
	for _, b := range openBeads {
		deps, ok := c.deps[b.ID]
		switch {
		case ok:
		case c.depsComplete:
			deps = nil
		default:
			return nil, false
		}
		if c.state == cachePartial && !cachedReadyDependencyStatusesKnown(b, statusByID, deps) {
			return nil, false
		}
		if cachedBeadReady(b, statusByID, workOutcomeByID, deps) {
			result = append(result, cloneBead(b))
		}
	}
	// Map-scan order is nondeterministic; match the canonical ready order of
	// the SQL-backed ready readers (#3208).
	sortBeadsReadyOrder(result)
	return result, true
}

func cachedReadyDependencyStatusesKnown(b Bead, statusByID map[string]string, deps []Dep) bool {
	if b.IsBlocked != nil {
		// bd's own projection (true or false) is trusted outright and needs
		// no per-dependency status data to back it up — see cachedBeadReady.
		// The narrow gc.work_outcome override layered on a false verdict
		// there degrades safely when a dependency is absent from a partial
		// cache snapshot (it simply can't add an override for that one), the
		// same eventual-consistency gap already accepted by trusting a true
		// verdict without re-checking dependency completeness.
		return true
	}
	missing := false
	for _, dep := range deps {
		if !isReadyBlockingDependencyType(dep.Type) {
			continue
		}
		status, ok := statusByID[dep.DependsOnID]
		if ok && status != "closed" {
			// One observed live blocker settles the verdict even if another
			// dependency target is outside the partial status snapshot.
			return true
		}
		if !ok {
			missing = true
		}
	}
	return !missing
}

func cachedBeadReady(b Bead, statusByID, workOutcomeByID map[string]string, deps []Dep) bool {
	if b.IsBlocked != nil {
		if *b.IsBlocked {
			return false
		}
		// bd's own false verdict already reflects its richer native gating
		// semantics (e.g. a waits-for gate opened through bd-native state,
		// not just a closed target) and is trusted outright — falling
		// through to the naive per-dependency scan below would second-guess
		// a verdict this cache cannot reproduce (see the bdReadyDisagreementLedger
		// fixture). The one gap a false verdict can still hide is
		// gc.work_outcome, which predates bd entirely: a blocking dependency
		// that closed with an unsatisfying outcome. Layer just that one
		// narrow check on top of the trusted false rather than reopening the
		// whole scan.
		for _, dep := range deps {
			if !isReadyBlockingDependencyType(dep.Type) {
				continue
			}
			status, ok := statusByID[dep.DependsOnID]
			if !ok {
				continue
			}
			if status == "closed" && workOutcomeByID[dep.DependsOnID] == beadmeta.WorkOutcomeBlocked {
				return false
			}
		}
		return true
	}
	for _, dep := range deps {
		if !isReadyBlockingDependencyType(dep.Type) {
			continue
		}
		status, ok := statusByID[dep.DependsOnID]
		if !ok {
			continue
		}
		if !DependencySatisfied(status, workOutcomeByID[dep.DependsOnID]) {
			return false
		}
	}
	return true
}

// Children returns beads with the given parent ID.
func (c *CachingStore) Children(parentID string, opts ...QueryOpt) ([]Bead, error) {
	return c.List(ListQuery{
		ParentID:      parentID,
		IncludeClosed: HasOpt(opts, IncludeClosed),
		Sort:          SortCreatedAsc,
	})
}

// ListByLabel returns beads matching the given label. By default, serves from
// cache only (non-closed beads). Pass IncludeClosed to also query the backing
// store for closed beads and merge results.
func (c *CachingStore) ListByLabel(label string, limit int, opts ...QueryOpt) ([]Bead, error) {
	return c.List(ListQuery{
		Label:         label,
		Limit:         limit,
		IncludeClosed: HasOpt(opts, IncludeClosed),
		Sort:          SortCreatedDesc,
		TierMode:      TierModeFromOpts(opts),
	})
}

// ListByAssignee returns beads assigned to the given agent with matching status.
func (c *CachingStore) ListByAssignee(assignee, status string, limit int) ([]Bead, error) {
	return c.List(ListQuery{
		Assignee: assignee,
		Status:   status,
		Limit:    limit,
		Sort:     SortCreatedDesc,
	})
}

// ListByMetadata filters beads by metadata key-value pairs. By default, serves
// from cache only (non-closed beads). Pass IncludeClosed to also query the
// backing store for closed beads and merge results.
func (c *CachingStore) ListByMetadata(filters map[string]string, limit int, opts ...QueryOpt) ([]Bead, error) {
	return c.List(ListQuery{
		Metadata:      filters,
		Limit:         limit,
		IncludeClosed: HasOpt(opts, IncludeClosed),
		Sort:          SortCreatedDesc,
		TierMode:      TierModeFromOpts(opts),
	})
}

func matchesMetadata(b Bead, filters map[string]string) bool {
	for k, v := range filters {
		if b.Metadata[k] != v {
			return false
		}
	}
	return true
}

// DepList returns dependencies for a bead in the given direction.
func (c *CachingStore) DepList(id, direction string) ([]Dep, error) {
	c.mu.RLock()
	if c.state == cacheLive {
		if direction == "down" || direction == "" {
			if !c.depsComplete {
				c.mu.RUnlock()
				return c.backing.DepList(id, direction)
			}
			if deps, ok := c.deps[id]; ok {
				c.mu.RUnlock()
				return cloneDeps(deps), nil
			}
			// Dep not cached yet - fetch from backing and cache it.
			c.mu.RUnlock()
			deps, err := c.backing.DepList(id, direction)
			if err != nil {
				return nil, err
			}
			c.mu.Lock()
			c.deps[id] = cloneDeps(deps)
			c.mu.Unlock()
			return deps, nil
		}
		// Reverse lookups are only partially cached; defer to the backing
		// store so callers do not observe incomplete results.
		c.mu.RUnlock()
		return c.backing.DepList(id, direction)
	}
	c.mu.RUnlock()
	return c.backing.DepList(id, direction)
}

// DepMetadata reads the edge payload straight from the backing store. The
// cache holds Dep values, which carry the pair and the type alone, so there is
// no cached form of this answer to serve and nothing to invalidate.
//
// Forwarded explicitly because the capability is discovered by type-assertion
// and this wrapper is not an interface embed: a caller that refuses on
// uncertainty — the infra-class migration is one — would read the cache as
// UNABLE TO ANSWER and refuse a city whose backing store answers fine. A
// backing store without the read gets an error rather than ("", false, nil),
// because "cannot be asked" and "carries nothing" are different answers.
func (c *CachingStore) DepMetadata(issueID, dependsOnID string) (string, bool, error) {
	reader, ok := c.backing.(DepMetadataReader)
	if !ok {
		return "", false, fmt.Errorf("reading dependency metadata %s -> %s: backing store %T exposes no edge-payload read", issueID, dependsOnID, c.backing)
	}
	return reader.DepMetadata(issueID, dependsOnID)
}

// Ping delegates to the backing store.
func (c *CachingStore) Ping() error {
	return c.backing.Ping()
}
