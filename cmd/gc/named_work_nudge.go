package main

import (
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/gastownhall/gascity/internal/beads"
	"github.com/gastownhall/gascity/internal/runtime"
	"github.com/gastownhall/gascity/internal/session"
)

// NamedWorkMatch captures the concrete work bead behind a namedWorkReady
// demand match (build_desired_state), so the named-work delivery pass can
// nudge the running session with its actual assignment.
type NamedWorkMatch struct {
	BeadID   string
	StoreRef string // "" = city store; non-empty = rig name
	Title    string
	Branch   string // work bead metadata["branch"], "" when absent
}

// namedWorkNudgedForBeadKey marks, on a named session's bead, the work bead
// id the named-work nudge was last delivered for. Persisted (not in-memory)
// for the same reason as the warm-bind marker: it survives controller
// restarts and cannot replay. New work writes a different bead id, so the
// marker mismatches and the nudge fires again: exactly once per work item.
const namedWorkNudgedForBeadKey = "gc.named_work_nudged_for"

// namedWorkNudgeIdleTimeout mirrors warmBindNudgeIdleTimeout: never inject
// mid-turn; a promptless named session is normally already idle, so the wait
// returns at once.
const namedWorkNudgeIdleTimeout = 30 * time.Second

// composeNamedWorkNudgeText renders the work assignment as a self-contained
// kick. It deliberately points at the bead + the session's own role formula
// rather than encoding role-specific steps here: the formula/prime is the
// authoritative protocol, the nudge only closes the delivery gap.
func composeNamedWorkNudgeText(m NamedWorkMatch) string {
	branchLine := ""
	if m.Branch != "" {
		branchLine = fmt.Sprintf(" Branch: %s.", m.Branch)
	}
	title := m.Title
	if title != "" {
		title = " (" + title + ")"
	}
	return fmt.Sprintf(
		"[gc] work nudge: bead %s%s is assigned to you.%s Run `bd show %s`, then execute it per your role formula (`gc prime` restores it). When done the bead MUST move — hand off or close per your protocol; do not idle.",
		m.BeadID, title, branchLine, m.BeadID,
	)
}

// namedWorkBeadStillAssigned reports whether the matched work bead is still
// open/in_progress and assigned to identity, read live from its owning
// store. Any unresolved store or read error yields false: never nudge on
// uncertainty (same fail-closed stance as the warm-bind trigger probe).
func namedWorkBeadStillAssigned(identity string, m NamedWorkMatch, cityStore beads.Store, rigStores map[string]beads.Store) bool {
	store := cityStore
	if ref := strings.TrimSpace(m.StoreRef); ref != "" && !strings.HasPrefix(ref, "city:") {
		store = rigStores[ref]
	}
	if store == nil {
		return false
	}
	wb, err := store.Get(m.BeadID)
	if err != nil {
		return false
	}
	if wb.Status != "open" && wb.Status != "in_progress" {
		return false
	}
	return strings.TrimSpace(wb.Assignee) == identity
}

// deliverNamedWorkNudges is the named-session counterpart of the warm-bind
// claim nudge: for every named identity with matched direct work, deliver
// the assignment into the already-running session exactly once per work
// item. Named sessions have no trigger binding, so the warm-bind retry never
// covers them — a startup prompt lost to a TUI boot race otherwise leaves
// the session idle-from-birth, where idle-sleep reaps it and the demand
// loop respawns it forever (gc-89jx3s). Gates, in cheap-first order:
//   - session bead resolves for the identity and is awake;
//   - persisted once-per-work marker mismatches;
//   - the work bead is still open/in_progress and assigned (fail-closed);
//   - bounded idle wait, then provider Nudge; marker set only on success so
//     a failed delivery retries next tick.
//
// Best-effort throughout; it never fails the tick.
func deliverNamedWorkNudges(
	ctx context.Context,
	sp runtime.Provider,
	sessStore beads.Store,
	cityStore beads.Store,
	rigStores map[string]beads.Store,
	sessionInfos []session.Info,
	matches map[string]NamedWorkMatch,
	stderr io.Writer,
) {
	if sp == nil || sessStore == nil || len(matches) == 0 {
		return
	}
	for identity, m := range matches {
		if strings.TrimSpace(m.BeadID) == "" {
			continue
		}
		var info *session.Info
		for i := range sessionInfos {
			si := &sessionInfos[i]
			if si.Closed || si.State != session.StateAwake {
				continue
			}
			if strings.TrimSpace(si.AgentName) == identity || strings.TrimSpace(si.Alias) == identity {
				info = si
				break
			}
		}
		if info == nil || strings.TrimSpace(info.SessionName) == "" {
			continue // not running (yet) — the start path owns cold delivery
		}
		raw, err := sessStore.Get(info.ID)
		if err != nil {
			continue
		}
		// Marker is per (work bead, continuation epoch): a session that slept
		// and woke is a fresh incarnation with an empty context — the same
		// work item must be delivered again or the incarnation idles
		// promptless and the sleep/respawn loop never converges.
		markerWant := m.BeadID + "@" + strings.TrimSpace(raw.Metadata["continuation_epoch"])
		if strings.TrimSpace(raw.Metadata[namedWorkNudgedForBeadKey]) == markerWant {
			continue // delivered to this incarnation already
		}
		if !namedWorkBeadStillAssigned(identity, m, cityStore, rigStores) {
			continue
		}
		if waiter, ok := sp.(runtime.IdleWaitProvider); ok {
			_ = waiter.WaitForIdle(ctx, info.SessionName, namedWorkNudgeIdleTimeout)
		}
		if err := sp.Nudge(info.SessionName, runtime.TextContent(composeNamedWorkNudgeText(m))); err != nil {
			fmt.Fprintf(stderr, "named-work nudge: %s failed for bead %s: %v\n", info.SessionName, m.BeadID, err) //nolint:errcheck
			continue // marker unset → retried next tick
		}
		if err := sessionFrontDoor(sessStore).SetMarker(info.ID, namedWorkNudgedForBeadKey, markerWant); err != nil {
			fmt.Fprintf(stderr, "named-work nudge: marking %s failed: %v\n", info.ID, err) //nolint:errcheck
			continue
		}
		fmt.Fprintf(stderr, "named-work nudge: nudged %s for bead %s\n", info.SessionName, m.BeadID) //nolint:errcheck
	}
}
