package sessionlog

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// Line shapes captured from a real kimi-code 0.27.0 session
// (agents/main/wire.jsonl), values sanitized.
const kimiWireFixture = `{"type":"metadata","protocol_version":"1.4","created_at":1784296571917}
{"type":"config.update","profileName":"agent","systemPrompt":"You are Kimi Code CLI"}
{"type":"turn.prompt","input":[{"type":"text","text":"say hi and close"}]}
{"type":"context.append_loop_event","event":{"type":"step.end","uuid":"f2a58f64","turnId":"0","step":1,"usage":{"inputOther":15933,"output":69,"inputCacheRead":19200,"inputCacheCreation":0},"finishReason":"tool_use","messageId":"chatcmpl-abc"},"time":1784296580847}
{"type":"usage.record","model":"kimi-code/k3","usage":{"inputOther":15933,"output":69,"inputCacheRead":19200,"inputCacheCreation":0},"usageScope":"turn","time":1784296580847}
not json at all
{"type":"usage.record","model":"kimi-code/k3","usage":{"inputOther":0,"output":0,"inputCacheRead":0,"inputCacheCreation":0},"usageScope":"turn","time":1784296580900}
{"type":"usage.record","model":"kimi-code/k3","usage":{"inputOther":120,"output":40,"inputCacheRead":35000,"inputCacheCreation":128},"usageScope":"turn","time":1784296590001}
{"type":"usage.record","model":"kimi-code/k3","usage":{"inputOther":121,"output":41,"inputCacheRead":35000,"inputCacheCreation":128},"usageScope":"turn","time":1784296590001}
`

func writeKimiWireFixture(t *testing.T, dir string) string {
	t.Helper()
	path := filepath.Join(dir, "wire.jsonl")
	if err := os.WriteFile(path, []byte(kimiWireFixture), 0o644); err != nil {
		t.Fatalf("writing fixture: %v", err)
	}
	return path
}

func TestExtractKimiTailUsage(t *testing.T) {
	path := writeKimiWireFixture(t, t.TempDir())

	usages, err := ExtractKimiTailUsage(path)
	if err != nil {
		t.Fatalf("ExtractKimiTailUsage: %v", err)
	}
	// step.end usage must NOT be double-counted (usage.record is the source),
	// the all-zero record is skipped, and the duplicated time collapses with
	// last-observed-wins.
	if len(usages) != 2 {
		t.Fatalf("usages = %d entries, want 2: %#v", len(usages), usages)
	}

	first := usages[0]
	if first.MessageID != "time:1784296580847" || first.EntryUUID != first.MessageID {
		t.Errorf("first identity = %q/%q, want time:1784296580847", first.EntryUUID, first.MessageID)
	}
	if first.Model != "kimi-code/k3" {
		t.Errorf("first.Model = %q", first.Model)
	}
	if first.InputTokens != 15933 || first.OutputTokens != 69 ||
		first.CacheReadTokens != 19200 || first.CacheCreationTokens != 0 {
		t.Errorf("first usage mapping = %+v", first)
	}

	second := usages[1]
	if second.MessageID != "time:1784296590001" {
		t.Errorf("second.MessageID = %q", second.MessageID)
	}
	if second.InputTokens != 121 || second.OutputTokens != 41 ||
		second.CacheReadTokens != 35000 || second.CacheCreationTokens != 128 {
		t.Errorf("duplicate collapse should keep last observed: %+v", second)
	}
}

func TestExtractKimiTailUsageFromSearchPaths(t *testing.T) {
	root := t.TempDir()
	path := writeKimiWireFixture(t, root)

	usages, err := ExtractKimiTailUsageFromSearchPaths([]string{root}, path)
	if err != nil {
		t.Fatalf("inside search root: %v", err)
	}
	if len(usages) != 2 {
		t.Fatalf("usages = %d entries, want 2", len(usages))
	}

	outside := writeKimiWireFixture(t, t.TempDir())
	if _, err := ExtractKimiTailUsageFromSearchPaths([]string{root}, outside); err == nil {
		t.Fatalf("outside search root should be rejected")
	}
}

func TestFindKimiWireFileForWorkDir(t *testing.T) {
	configRoot := t.TempDir()
	sessionsRoot := filepath.Join(configRoot, "sessions")

	workDir := "/tmp/kimi-test-workdir/we-test"
	mkSession := func(name string, mod time.Time) string {
		dir := filepath.Join(sessionsRoot, "wd_we-test_abc123def456", name, "agents", "main")
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatalf("mkdir: %v", err)
		}
		wire := filepath.Join(dir, "wire.jsonl")
		if err := os.WriteFile(wire, []byte("{}\n"), 0o644); err != nil {
			t.Fatalf("write: %v", err)
		}
		if err := os.Chtimes(wire, mod, mod); err != nil {
			t.Fatalf("chtimes: %v", err)
		}
		return wire
	}
	older := mkSession("session_older", time.Now().Add(-2*time.Hour))
	newest := mkSession("session_newest", time.Now().Add(-1*time.Minute))

	index := `{"sessionId":"session_older","sessionDir":"` + filepath.Dir(filepath.Dir(filepath.Dir(older))) + `","workDir":"` + workDir + `"}
{"sessionId":"session_newest","sessionDir":"` + filepath.Dir(filepath.Dir(filepath.Dir(newest))) + `","workDir":"` + workDir + `"}
{"sessionId":"session_other","sessionDir":"/nonexistent","workDir":"/tmp/other"}
`
	if err := os.WriteFile(filepath.Join(configRoot, "session_index.jsonl"), []byte(index), 0o644); err != nil {
		t.Fatalf("writing index: %v", err)
	}

	got := FindKimiWireFileForWorkDir([]string{sessionsRoot}, workDir)
	if got != newest {
		t.Errorf("FindKimiWireFileForWorkDir = %q, want newest %q", got, newest)
	}

	if got := FindKimiWireFileForWorkDir([]string{sessionsRoot}, "/tmp/unknown"); got != "" {
		t.Errorf("unknown workdir should return empty, got %q", got)
	}
	if got := FindKimiWireFileForWorkDir([]string{sessionsRoot}, ""); got != "" {
		t.Errorf("empty workdir should return empty, got %q", got)
	}
}
