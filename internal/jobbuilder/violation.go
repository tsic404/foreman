package jobbuilder

import (
	"fmt"
	"strings"
)

// Violation is one rejected field path plus the clause it breaks
// (docs/05-modules/job-template.md §内部结构 `Violation{Path, Rule}`).
type Violation struct {
	Path string
	Rule string
}

func (v Violation) String() string {
	return v.Path + ": " + v.Rule
}

// ValidationError carries every violation of one overlay, so the startup log
// renders (and can be grepped by) each field path instead of only the first
// (03-contracts.md §5.4 两段式校验). It is also the build-time fallback
// signal: the scheduler maps it to failure_reason=invalid_job_template.
type ValidationError struct {
	Source     string
	Violations []Violation
}

func (e *ValidationError) Error() string {
	var b strings.Builder
	fmt.Fprintf(&b, "%s rejected: %d violation(s)", e.Source, len(e.Violations))
	for _, v := range e.Violations {
		b.WriteString("\n  ")
		b.WriteString(v.String())
	}
	return b.String()
}
