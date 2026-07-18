package main

import (
	"strings"
	"testing"
)

func TestComposeNamedWorkNudgeText(t *testing.T) {
	full := composeNamedWorkNudgeText(NamedWorkMatch{
		BeadID: "we-u3nb",
		Title:  "fix(dashboard): Safari overflow",
		Branch: "gc-gastown.furiosa-ec74d66e9dcc",
	})
	for _, want := range []string{
		"we-u3nb",
		"(fix(dashboard): Safari overflow)",
		"Branch: gc-gastown.furiosa-ec74d66e9dcc.",
		"bd show we-u3nb",
		"MUST move",
	} {
		if !strings.Contains(full, want) {
			t.Errorf("nudge text missing %q:\n%s", want, full)
		}
	}

	bare := composeNamedWorkNudgeText(NamedWorkMatch{BeadID: "hq-1"})
	if strings.Contains(bare, "Branch:") || strings.Contains(bare, "()") {
		t.Errorf("bare match should omit branch/title decorations:\n%s", bare)
	}
}
