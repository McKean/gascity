package herdr

import (
	"reflect"
	"testing"
)

// launchSpecFor decides how Start launches a session's command under herdr
// ≥0.7.5, whose `agent start` no longer execs arbitrary argv: it launches a
// supported agent *kind*'s canonical executable into an existing shell pane
// and waits for TUI detection. Clean invocations of a supported kind take
// that path (registered agent: native detection, prompt, wait, status);
// everything else is typed into the pane shell as `exec /bin/sh -c <cmd>` so
// the pane still dies with the command (tmux parity).

func TestLaunchSpecForCleanClaudeCommandUsesKind(t *testing.T) {
	got := launchSpecFor(`claude --dangerously-skip-permissions --effort max --settings "/city root/.gc/settings.json"`)
	if got.Kind != "claude" {
		t.Fatalf("Kind = %q; want claude", got.Kind)
	}
	want := []string{"--dangerously-skip-permissions", "--effort", "max", "--settings", "/city root/.gc/settings.json"}
	if !reflect.DeepEqual(got.Args, want) {
		t.Errorf("Args = %q; want %q", got.Args, want)
	}
	if got.Raw != "" {
		t.Errorf("Raw = %q; want empty on the kind path", got.Raw)
	}
}

func TestLaunchSpecForPathQualifiedKind(t *testing.T) {
	got := launchSpecFor("/usr/local/bin/claude --resume abc123")
	if got.Kind != "claude" || got.Raw != "" {
		t.Fatalf("spec = %+v; want kind claude via basename", got)
	}
}

// Shell metachars mean the command needs a real shell: fall back to raw even
// when it mentions a known kind. Conservative is correct — the raw path still
// runs it; only herdr-native registration is lost.
func TestLaunchSpecForShellMetacharsFallBackToRaw(t *testing.T) {
	for _, cmd := range []string{
		"claude --flag && echo done",
		"claude -p 'hi'; sleep 1",
		"claude --append-system-prompt \"use $HOME wisely\"",
		"FOO=bar claude --flag",
		"claude | tee log",
		"for i in $(seq 3); do echo $i; done",
	} {
		got := launchSpecFor(cmd)
		if got.Kind != "" || got.Raw != cmd {
			t.Errorf("launchSpecFor(%q) = %+v; want raw fallback", cmd, got)
		}
	}
}

// '=' inside an argument is plain argv, not shell syntax: the codex reviewer's
// "-c model_reasoning_effort=xhigh" must launch as the codex kind so herdr
// registers the agent name and liveness resolves (gc-fogmz3).
func TestLaunchSpecForEqualsInsideArgKeepsKind(t *testing.T) {
	cmd := "codex --dangerously-bypass-hook-trust --model gpt-5.6-luna -c model_reasoning_effort=xhigh"
	got := launchSpecFor(cmd)
	if got.Kind != "codex" {
		t.Fatalf("launchSpecFor(%q).Kind = %q; want codex (spec %+v)", cmd, got.Kind, got)
	}
	want := []string{"--dangerously-bypass-hook-trust", "--model", "gpt-5.6-luna", "-c", "model_reasoning_effort=xhigh"}
	if len(got.Args) != len(want) {
		t.Fatalf("Args = %v; want %v", got.Args, want)
	}
	for i := range want {
		if got.Args[i] != want[i] {
			t.Fatalf("Args[%d] = %q; want %q", i, got.Args[i], want[i])
		}
	}
}

// An env-prefix assignment on the first token still needs a real shell.
func TestLaunchSpecForEnvPrefixAssignmentIsRaw(t *testing.T) {
	for _, cmd := range []string{"FOO=bar claude --flag", "A=1 B=2 codex"} {
		got := launchSpecFor(cmd)
		if got.Kind != "" || got.Raw != cmd {
			t.Errorf("launchSpecFor(%q) = %+v; want raw fallback", cmd, got)
		}
	}
}

// Unknown executables are raw.
func TestLaunchSpecForUnknownExecutableIsRaw(t *testing.T) {
	got := launchSpecFor("python3 worker.py --queue main")
	if got.Kind != "" || got.Raw != "python3 worker.py --queue main" {
		t.Errorf("spec = %+v; want raw", got)
	}
}

// Empty command: the shell pane itself is the session (old /bin/sh behavior).
func TestLaunchSpecForEmptyCommandIsBareShell(t *testing.T) {
	for _, cmd := range []string{"", "   "} {
		got := launchSpecFor(cmd)
		if got.Kind != "" || got.Raw != "" {
			t.Errorf("launchSpecFor(%q) = %+v; want zero spec (bare shell)", cmd, got)
		}
	}
}
