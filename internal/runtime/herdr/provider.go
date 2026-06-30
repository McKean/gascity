package herdr

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/gastownhall/gascity/internal/runtime"
)

// Provider implements runtime.Provider (and ServerLifecycleProvider) backed by
// herdr. Model: one shared herdr session (server) per city; within it, one
// workspace per rig (or per town) and one tab per agent, so each gascity session
// is its own switchable "space" rather than a tiled pane. Agents are addressable
// by name, 1:1 with gascity session names. Opt-in via the "herdr" runtime
// selector; tmux default. See herdr-provider-design.md.
type Provider struct {
	c       *client
	metaDir string     // sidecar KV root (herdr has no per-session metadata store)
	mu      sync.Mutex // serializes workspace/tab find-or-create across concurrent Starts
}

var (
	_ runtime.Provider                = (*Provider)(nil)
	_ runtime.ServerLifecycleProvider = (*Provider)(nil)
)

// New builds a herdr Provider. herdrSession is the shared per-city herdr session
// name; metaDir is a writable directory for sidecar session metadata (a temp
// fallback is used when empty, e.g. a city-less standalone construction).
func New(herdrSession, metaDir string) *Provider {
	if metaDir == "" {
		metaDir = filepath.Join(os.TempDir(), "gc-herdr-meta", sanitize(herdrSession))
	}
	return &Provider{c: newClient(herdrSession), metaDir: metaDir}
}

// ── ServerLifecycleProvider: own the shared herdr session-server ─────────────

// ConfigureServer ensures the shared herdr session-server is running. A named
// session's socket does not exist until its server starts, so this must run
// before any agent op. Idempotent.
func (p *Provider) ConfigureServer() error { return p.c.startServer() }

// TeardownServer stops the shared herdr session-server after sessions drain.
func (p *Provider) TeardownServer() error { return p.c.stopServer() }

// ── Provider core ────────────────────────────────────────────────────────────

func (p *Provider) Start(ctx context.Context, name string, cfg runtime.Config) error {
	if err := p.ConfigureServer(); err != nil {
		return fmt.Errorf("herdr: configure server: %w", err)
	}
	if p.IsRunning(name) {
		return runtime.ErrSessionExists
	}
	// Place the agent in its own tab under a per-rig (per-town) workspace, so
	// agents are separate switchable spaces rather than tiled panes. The
	// find-or-create is serialized so concurrent same-rig Starts share one
	// workspace instead of racing to create duplicates.
	wsLabel, tabLabel := placementFor(name, cfg.Env)
	p.mu.Lock()
	tabID, strayPane, err := p.c.ensurePlacement(ctx, wsLabel, tabLabel)
	p.mu.Unlock()
	if err != nil {
		return fmt.Errorf("herdr: place %q: %w", name, err)
	}
	info, err := p.c.startAgent(ctx, name, tabID, effectiveWorkDir(cfg), cfg.Env, shellArgv(cfg.Command))
	if err != nil {
		return fmt.Errorf("herdr: start %q: %w", name, err)
	}
	// herdr auto-spawns a stray shell pane when it creates a workspace/tab; close
	// it so the tab holds only the agent.
	if strayPane != "" && strayPane != info.PaneID {
		_ = p.c.closePane(ctx, strayPane)
	}
	if cfg.Nudge != "" && info.PaneID != "" {
		_ = p.c.deliverNudge(ctx, info.PaneID, cfg.Nudge)
	}
	return nil
}

func (p *Provider) Stop(name string) error {
	ctx := context.Background()
	pid, err := p.paneID(ctx, name)
	if err != nil || pid == "" {
		return nil // idempotent
	}
	_ = p.c.closePane(ctx, pid)
	_ = p.clearMeta(name)
	return nil
}

func (p *Provider) Interrupt(name string) error {
	ctx := context.Background()
	pid, err := p.paneID(ctx, name)
	if err != nil || pid == "" {
		return nil
	}
	return p.c.sendKeys(ctx, pid, "ctrl+c") // herdr has no signal API; ctrl+c is the soft interrupt
}

func (p *Provider) IsRunning(name string) bool {
	agents, err := p.c.listAgents(context.Background())
	if err != nil {
		return false
	}
	for _, a := range agents {
		if a.Name == name {
			return true
		}
	}
	return false
}

// IsAttached: herdr 0.7.1 exposes no clean attach-state query — report false.
func (p *Provider) IsAttached(name string) bool { return false }

func (p *Provider) Attach(name string) error {
	cmd := exec.Command(p.c.bin, "--session", p.c.session, "agent", "attach", name)
	cmd.Stdin, cmd.Stdout, cmd.Stderr = os.Stdin, os.Stdout, os.Stderr
	return cmd.Run() // blocks until the user detaches
}

func (p *Provider) ProcessAlive(name string, processNames []string) bool {
	ctx := context.Background()
	pid, err := p.paneID(ctx, name)
	if err != nil || pid == "" {
		return false
	}
	shellPID, fg, err := p.c.processInfo(ctx, pid)
	if err != nil || shellPID == 0 {
		return false
	}
	if len(processNames) == 0 {
		return true // per contract
	}
	for _, pr := range fg {
		for _, want := range processNames {
			if pr.Name == want {
				return true
			}
		}
	}
	return false
}

func (p *Provider) Nudge(name string, content []runtime.ContentBlock) error {
	ctx := context.Background()
	pid, err := p.paneID(ctx, name)
	if err != nil || pid == "" {
		return runtime.ErrSessionNotFound
	}
	return p.c.deliverNudge(ctx, pid, runtime.FlattenText(content))
}

// Peek reads the current rendered screen ("visible") — the liveness/fingerprint
// snapshot. recent*/scrollback is empty until lines scroll off.
func (p *Provider) Peek(name string, lines int) (string, error) {
	return p.c.read(context.Background(), name, "visible", lines)
}

func (p *Provider) ListRunning(prefix string) ([]string, error) {
	agents, err := p.c.listAgents(context.Background())
	if err != nil {
		return nil, err
	}
	var out []string
	for _, a := range agents {
		if strings.HasPrefix(a.Name, prefix) {
			out = append(out, a.Name)
		}
	}
	return out, nil
}

func (p *Provider) SendKeys(name string, keys ...string) error {
	ctx := context.Background()
	pid, err := p.paneID(ctx, name)
	if err != nil || pid == "" {
		return nil
	}
	hk := make([]string, len(keys))
	for i, k := range keys {
		hk[i] = translateKey(k)
	}
	return p.c.sendKeys(ctx, pid, hk...)
}

func (p *Provider) Capabilities() runtime.ProviderCapabilities {
	return runtime.ProviderCapabilities{
		CanReportAttachment: false, // no clean IsAttached query
		CanReportActivity:   false, // no GetLastActivity
		CanStream:           false, // socket-event streaming is a later optimization
		CanAttachTTY:        true,  // agent attach
	}
}

// ── best-effort / unsupported (the contract permits these) ───────────────────

func (p *Provider) GetLastActivity(name string) (time.Time, error) { return time.Time{}, nil }
func (p *Provider) ClearScrollback(name string) error              { return nil }
func (p *Provider) RunLive(name string, cfg runtime.Config) error  { return nil }

func (p *Provider) CopyTo(name, src, relDst string) error {
	if _, err := os.Stat(src); err != nil {
		return nil // best-effort: missing src
	}
	a, ok, err := p.c.getAgent(context.Background(), name)
	if err != nil || !ok || a.Cwd == "" {
		return nil
	}
	return copyPath(src, filepath.Join(a.Cwd, relDst))
}

// ── metadata sidecar (herdr has no per-session KV) ───────────────────────────

func (p *Provider) SetMeta(name, key, value string) error {
	dir := filepath.Join(p.metaDir, sanitize(name))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, sanitize(key)), []byte(value), 0o644)
}

func (p *Provider) GetMeta(name, key string) (string, error) {
	b, err := os.ReadFile(filepath.Join(p.metaDir, sanitize(name), sanitize(key)))
	if errors.Is(err, os.ErrNotExist) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func (p *Provider) RemoveMeta(name, key string) error {
	err := os.Remove(filepath.Join(p.metaDir, sanitize(name), sanitize(key)))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}

func (p *Provider) clearMeta(name string) error {
	return os.RemoveAll(filepath.Join(p.metaDir, sanitize(name)))
}

// ── helpers ──────────────────────────────────────────────────────────────────

// paneID resolves a gascity session name to its herdr pane id (or "" if absent).
func (p *Provider) paneID(ctx context.Context, name string) (string, error) {
	a, ok, err := p.c.getAgent(ctx, name)
	if err != nil {
		return "", err
	}
	if !ok {
		return "", nil
	}
	return a.PaneID, nil
}

// shellArgv wraps a shell command string as argv for `herdr agent start -- …`.
func shellArgv(command string) []string {
	if strings.TrimSpace(command) == "" {
		return []string{"/bin/sh"}
	}
	return []string{"/bin/sh", "-c", command}
}

// workspaceTabFor maps a gascity runtime session name to its herdr placement: a
// per-rig (or per-town) workspace label and a per-agent tab label. Runtime names
// are "<rig>--<town>__<agent>" (rig-qualified; citylayout maps "/" → "--") or
// "<town>__<agent>" (town-level). Workspace = the rig when present, else the
// town; tab = the agent (the segment after the last "__"). Falls back to the
// whole name when those separators are absent (defensive for non-gc names).
func workspaceTabFor(name string) (workspace, tab string) {
	rest := name
	if i := strings.Index(name, "--"); i >= 0 {
		workspace, rest = name[:i], name[i+2:]
	} else if j := strings.Index(name, "__"); j >= 0 {
		workspace = name[:j]
	} else {
		workspace = name
	}
	if k := strings.LastIndex(rest, "__"); k >= 0 {
		tab = rest[k+2:]
	} else {
		tab = rest
	}
	if workspace == "" {
		workspace = name
	}
	if tab == "" {
		tab = name
	}
	return workspace, tab
}

// placementFor decides a session's herdr workspace and tab. It starts from the
// structural runtime name (workspaceTabFor) and then refines it with the richer
// identity the reconciler injects into the environment — the same GC_RIG /
// GC_ALIAS convention the t3bridge and k8s providers use (session/manager.go
// populates these via RuntimeEnvWithSessionContext).
//
// This matters for ephemeral pool wisps: their runtime name is town-qualified
// (e.g. "gastown__polecat-gc-wisp-3nvj3yx"), so workspaceTabFor alone drops them
// in the town workspace under an opaque wisp-id tab. GC_RIG restores the
// originating rig workspace (webapp/mobile), and GC_ALIAS swaps the wisp id for
// the themed instance name, yielding e.g. workspace "webapp", tab
// "polecat-furiosa". Persistent and town-level sessions are unaffected: they
// either carry no GC_RIG (town agents) or already resolve to the same labels.
func placementFor(name string, env map[string]string) (workspace, tab string) {
	workspace, tab = workspaceTabFor(name)
	if len(env) == 0 {
		return workspace, tab
	}
	// Group under the originating rig when known. Town-level agents (mayor,
	// deacon, …) have no GC_RIG and keep their town workspace.
	if rig := strings.TrimSpace(env["GC_RIG"]); rig != "" {
		workspace = rig
	}
	// Replace a wisp id with the themed instance alias so tabs read e.g.
	// "polecat-furiosa" rather than "polecat-gc-wisp-3nvj3yx". The role prefix
	// (everything before the wisp id) is preserved. Falls through unchanged when
	// no alias is available yet, or when the alias is itself the wisp identity.
	if i := strings.Index(tab, "gc-wisp-"); i >= 0 {
		alias := strings.TrimSpace(env["GC_ALIAS"])
		if alias == "" {
			alias = strings.TrimSpace(env["GC_AGENT"])
		}
		if leaf := lastSegment(alias); leaf != "" && !strings.Contains(leaf, "gc-wisp-") {
			tab = tab[:i] + leaf
		}
	}
	return workspace, tab
}

// lastSegment returns the trailing identity segment after the final "/" or ".",
// reducing a possibly-qualified alias ("webapp/gastown.furiosa") to its bare
// instance name ("furiosa").
func lastSegment(s string) string {
	if i := strings.LastIndexAny(s, "/."); i >= 0 {
		return s[i+1:]
	}
	return s
}

// effectiveWorkDir picks the directory the agent should launch in. herdr falls
// back to $HOME when --cwd points at a path that does not exist, and Claude Code
// never persists trust acceptance from $HOME — so it re-prompts "trust this
// folder?" on every launch. Ephemeral pool wisps are started before their
// per-bead worktree is created, so cfg.WorkDir may not exist yet at launch;
// fall back to the city root (a stable project dir where trust is saved once)
// rather than let herdr land the session in $HOME. An empty result lets herdr
// use its own server cwd (also the city root).
func effectiveWorkDir(cfg runtime.Config) string {
	if cfg.WorkDir != "" {
		if _, err := os.Stat(cfg.WorkDir); err == nil {
			return cfg.WorkDir
		}
	}
	return cfg.Env["GC_CITY_ROOT"]
}

// translateKey maps tmux-style key names (SendKeys uses "Enter"/"C-c"/"Down")
// to herdr key-combo strings ("enter"/"ctrl+c"/"down").
func translateKey(k string) string {
	switch k {
	case "Enter":
		return "enter"
	case "Escape", "Esc":
		return "esc"
	case "Tab":
		return "tab"
	case "Up":
		return "up"
	case "Down":
		return "down"
	case "Left":
		return "left"
	case "Right":
		return "right"
	case "Space":
		return "space"
	case "BSpace":
		return "backspace"
	}
	if len(k) > 2 && k[1] == '-' { // C-x / M-x / S-x
		switch k[0] {
		case 'C':
			return "ctrl+" + strings.ToLower(k[2:])
		case 'M':
			return "alt+" + strings.ToLower(k[2:])
		case 'S':
			return "shift+" + strings.ToLower(k[2:])
		}
	}
	return k
}

// sanitize makes a string safe as a single path segment.
func sanitize(s string) string {
	return strings.NewReplacer("/", "_", " ", "_", ":", "_", "..", "_").Replace(s)
}

// copyPath copies a file or directory tree from src to dst.
func copyPath(src, dst string) error {
	info, err := os.Stat(src)
	if err != nil {
		return err
	}
	if info.IsDir() {
		entries, err := os.ReadDir(src)
		if err != nil {
			return err
		}
		if err := os.MkdirAll(dst, 0o755); err != nil {
			return err
		}
		for _, e := range entries {
			if err := copyPath(filepath.Join(src, e.Name()), filepath.Join(dst, e.Name())); err != nil {
				return err
			}
		}
		return nil
	}
	b, err := os.ReadFile(src)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return err
	}
	return os.WriteFile(dst, b, info.Mode().Perm())
}
