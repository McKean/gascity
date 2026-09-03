package herdr

import (
	"context"
	"os"
	"path/filepath"
	goruntime "runtime"
	"strings"
	"testing"
)

// writeFakeHerdrCLI writes a POSIX shell script that emits stdout/stderr and
// exits with the given status, standing in for the herdr binary.
func writeFakeHerdrCLI(t *testing.T, stdout, stderr string, exit int) string {
	t.Helper()
	if goruntime.GOOS == "windows" {
		t.Skip("fake herdr CLI is a POSIX shell script")
	}
	bin := filepath.Join(t.TempDir(), "herdr")
	script := "#!/bin/sh\n"
	if stdout != "" {
		script += "printf '%s' '" + stdout + "'\n"
	}
	if stderr != "" {
		script += "printf '%s\\n' '" + stderr + "' >&2\n"
	}
	script += "exit " + itoa(exit) + "\n"
	if err := os.WriteFile(bin, []byte(script), 0o755); err != nil {
		t.Fatalf("writing fake herdr: %v", err)
	}
	return bin
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	s := ""
	for n > 0 {
		s = string(rune('0'+n%10)) + s
		n /= 10
	}
	return s
}

// herdr >= 0.8.0 prints the error envelope on stderr and exits 1. The typed
// error must survive so herdrErrorCode-based branching (agent_pane_busy
// retry, agent_name_taken adoption) works.
func TestRunStderrEnvelopeYieldsTypedError(t *testing.T) {
	bin := writeFakeHerdrCLI(t, "",
		`{"error":{"code":"agent_pane_busy","message":"agent target pane p1 is not an available shell"},"id":"cli:agent:start"}`, 1)
	c := &client{session: "t", bin: bin}
	_, err := c.run(context.Background(), "agent", "start", "x")
	if err == nil {
		t.Fatal("expected an error")
	}
	if got := herdrErrorCode(err); got != "agent_pane_busy" {
		t.Fatalf("herdrErrorCode = %q, want agent_pane_busy (err: %v)", got, err)
	}
	if !strings.Contains(err.Error(), "not an available shell") {
		t.Fatalf("error text lost the herdr message: %v", err)
	}
}

// A warning line before the envelope must not hide the envelope.
func TestRunStderrEnvelopeAfterWarning(t *testing.T) {
	bin := writeFakeHerdrCLI(t, "",
		`warning: something noisy
{"error":{"code":"agent_name_taken","message":"name in use"},"id":"cli:agent:start"}`, 1)
	c := &client{session: "t", bin: bin}
	_, err := c.run(context.Background(), "agent", "start", "x")
	if got := herdrErrorCode(err); got != "agent_name_taken" {
		t.Fatalf("herdrErrorCode = %q, want agent_name_taken (err: %v)", got, err)
	}
}

// Plain stderr text (no envelope) keeps the old behavior: raw text in the
// error string, no code.
func TestRunStderrPlainTextKeepsRawError(t *testing.T) {
	bin := writeFakeHerdrCLI(t, "", "error: server_not_running", 1)
	c := &client{session: "t", bin: bin}
	_, err := c.run(context.Background(), "agent", "list")
	if err == nil {
		t.Fatal("expected an error")
	}
	if got := herdrErrorCode(err); got != "" {
		t.Fatalf("herdrErrorCode = %q, want empty", got)
	}
	if !strings.Contains(err.Error(), "server_not_running") {
		t.Fatalf("raw stderr lost: %v", err)
	}
}

// The pre-0.8.0 shape (envelope on stdout, exit 0) still decodes.
func TestRunStdoutEnvelopeStillTyped(t *testing.T) {
	bin := writeFakeHerdrCLI(t,
		`{"error":{"code":"pane_not_found","message":"pane p9 not found"}}`, "", 0)
	c := &client{session: "t", bin: bin}
	_, err := c.run(context.Background(), "pane", "process-info", "p9")
	if got := herdrErrorCode(err); got != "pane_not_found" {
		t.Fatalf("herdrErrorCode = %q, want pane_not_found (err: %v)", got, err)
	}
}
