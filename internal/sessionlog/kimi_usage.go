package sessionlog

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// kimiWireUsageLine mirrors the usage.record events kimi-code 0.26+ writes
// to agents/<name>/wire.jsonl:
//
//	{"type":"usage.record","model":"kimi-code/k3",
//	 "usage":{"inputOther":N,"output":N,"inputCacheRead":N,"inputCacheCreation":N},
//	 "usageScope":"turn","time":<unix-ms>}
type kimiWireUsageLine struct {
	Type  string `json:"type"`
	Model string `json:"model"`
	Usage *struct {
		InputOther         int `json:"inputOther"`
		Output             int `json:"output"`
		InputCacheRead     int `json:"inputCacheRead"`
		InputCacheCreation int `json:"inputCacheCreation"`
	} `json:"usage"`
	Time int64 `json:"time"`
}

// ExtractKimiTailUsage reads the tail of a kimi-code wire transcript and
// returns one TailUsage per usage.record event, in file order. usage.record
// is emitted once per completed LLM step and carries the model, so no
// cross-referencing with step events is needed. The event has no message
// id; the millisecond timestamp keys both EntryUUID and the MessageID
// collapse identity (duplicate emissions of one record share it). All-zero
// usage records and malformed lines are skipped silently. The scan window
// is the last tailChunkSize bytes, so usage that scrolled past the window
// is not returned.
func ExtractKimiTailUsage(path string) ([]TailUsage, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close() //nolint:errcheck // best-effort close on read-only file

	data, _, err := readTail(f, tailChunkSize)
	if err != nil {
		return nil, err
	}

	var usages []TailUsage
	byMessageID := make(map[string]int)
	for _, line := range splitLines(data) {
		var entry kimiWireUsageLine
		if err := json.Unmarshal(line, &entry); err != nil {
			continue
		}
		if entry.Type != "usage.record" || entry.Usage == nil {
			continue
		}
		u := entry.Usage
		if u.InputOther == 0 && u.Output == 0 && u.InputCacheRead == 0 && u.InputCacheCreation == 0 {
			continue
		}
		id := fmt.Sprintf("time:%d", entry.Time)
		usage := TailUsage{
			EntryUUID:           id,
			MessageID:           id,
			Model:               strings.TrimSpace(entry.Model),
			InputTokens:         u.InputOther,
			OutputTokens:        u.Output,
			CacheReadTokens:     u.InputCacheRead,
			CacheCreationTokens: u.InputCacheCreation,
		}
		if idx, ok := byMessageID[id]; ok {
			usages[idx] = usage
			continue
		}
		byMessageID[id] = len(usages)
		usages = append(usages, usage)
	}
	return usages, nil
}

// ExtractKimiTailUsageFromSearchPaths reads kimi wire usage only after
// verifying path resolves under the kimi session roots merged with the
// configured search paths (mirrors the codex wrapper: the configured
// claude-style roots alone would reject real kimi transcript locations).
func ExtractKimiTailUsageFromSearchPaths(searchPaths []string, path string) ([]TailUsage, error) {
	safePath, err := validateSearchPathFile(mergeKimiSearchPaths(searchPaths), path)
	if err != nil {
		return nil, err
	}
	return ExtractKimiTailUsage(safePath)
}

// kimiSessionIndexRecord is one line of kimi-code 0.26+'s
// <config-root>/session_index.jsonl, which maps every session to its
// storage directory and originating workdir.
type kimiSessionIndexRecord struct {
	SessionID  string `json:"sessionId"`
	SessionDir string `json:"sessionDir"`
	WorkDir    string `json:"workDir"`
}

// FindKimiWireFileForWorkDir resolves the newest kimi-code 0.26+ wire
// transcript for workDir via the session index (session_index.jsonl next to
// each sessions root). The index is authoritative for the wd_<slug>_<hash>
// directory scheme, whose hash is not derivable from the workdir path (it
// is not the legacy md5 rule). Returns "" when no index or no match.
func FindKimiWireFileForWorkDir(searchPaths []string, workDir string) string {
	workDir = filepath.Clean(strings.TrimSpace(workDir))
	if workDir == "" || workDir == "." {
		return ""
	}

	var (
		bestPath string
		bestTime time.Time
	)
	for _, root := range mergeKimiSearchPaths(searchPaths) {
		root = canonicalKimiSessionRoot(root)
		if root == "" {
			continue
		}
		indexPath := filepath.Join(filepath.Dir(root), "session_index.jsonl")
		f, err := os.Open(indexPath)
		if err != nil {
			continue
		}
		scanner := bufio.NewScanner(f)
		scanner.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
		for scanner.Scan() {
			line := scanner.Bytes()
			if len(line) == 0 {
				continue
			}
			var rec kimiSessionIndexRecord
			if err := json.Unmarshal(line, &rec); err != nil {
				continue
			}
			if filepath.Clean(strings.TrimSpace(rec.WorkDir)) != workDir {
				continue
			}
			wire := filepath.Join(rec.SessionDir, "agents", "main", "wire.jsonl")
			info, err := os.Stat(wire)
			if err != nil || info.IsDir() {
				continue
			}
			if bestPath == "" || info.ModTime().After(bestTime) {
				bestPath = wire
				bestTime = info.ModTime()
			}
		}
		f.Close() //nolint:errcheck,gosec // read-only file
	}
	return bestPath
}
