package herdr

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/gastownhall/gascity/internal/runtime"
)

// Provider implements runtime.Provider (and ServerLifecycleProvider) backed by
// herdr. Model: one shared herdr session (server) per city, one named herdr
// agent (pane) per gascity session — agents are addressable by name, 1:1 with
// gascity session names. Opt-in via the "herdr" runtime selector; tmux default.
// See herdr-provider-design.md for the validated interface mapping.
type Provider struct {
	c       *client
	metaDir string // sidecar KV root (herdr has no per-session metadata store)
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
	if _, err := p.c.startAgent(ctx, name, cfg.WorkDir, cfg.Env, shellArgv(cfg.Command)); err != nil {
		return fmt.Errorf("herdr: start %q: %w", name, err)
	}
	if cfg.Nudge != "" {
		if pid, err := p.paneID(ctx, name); err == nil && pid != "" {
			_ = p.c.paneRun(ctx, pid, cfg.Nudge)
		}
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
	return p.c.paneRun(ctx, pid, runtime.FlattenText(content)) // text + Enter
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
func (p *Provider) ClearScrollback(name string) error             { return nil }
func (p *Provider) RunLive(name string, cfg runtime.Config) error { return nil }

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
