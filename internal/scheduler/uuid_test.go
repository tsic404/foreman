package scheduler

import (
	"regexp"
	"testing"
)

var uuidShape = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-5[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)

func TestJobRuntimeIDDeterministicAndVersion5(t *testing.T) {
	a := jobRuntimeID("fm-task-1")
	b := jobRuntimeID("fm-task-1")
	if a != b {
		t.Fatalf("not deterministic: %q vs %q", a, b)
	}
	if !uuidShape.MatchString(a) {
		t.Fatalf("not a uuidv5: %q", a)
	}
	if jobRuntimeID("fm-task-2") == a {
		t.Fatal("distinct job names collide")
	}
}
