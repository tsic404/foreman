// TC-tech-job-09 pins the executable half of AC-35 this repository owns: the
// reference template's agent container starts the daemon under the process
// init tool with the documented prefix and separator, its args still begin
// with the daemon subcommand, and the production builder carries both through
// into the task Job verbatim. The in-pod half of the design case — /proc/1's
// identity, the daemon/agent parent-child chain, the two orphan-injection
// batches and the PID-1 SIGTERM — needs a live task pod and runs from
// tests/acceptance/tech-job-09-cluster.sh (registered there, never skipped
// silently here).
package acceptance

import (
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
)

const (
	// tcJob09InitPrefix is the design's verbatim command prefix: the init
	// tool plus its separator (ADR-022).
	tcJob09InitPrefix = "/usr/bin/dumb-init"
	tcJob09Separator  = "--"
	// tcJob09Multica is the program the prefix hands over to.
	tcJob09Multica = "/usr/local/bin/multica"
)

// tcJob09Container finds the named container in a pod spec.
func tcJob09Container(t *testing.T, containers []corev1.Container, name string) corev1.Container {
	t.Helper()
	for _, container := range containers {
		if container.Name == name {
			return container
		}
	}
	t.Fatalf("the pod template declares no container named %q", name)
	return corev1.Container{}
}

func TestJob09PID1IsTheInitTool(t *testing.T) {
	// Step 1a/1b: the reference template's command prefix and args.
	template := decodeJob(t, []byte(publishedTemplateContent(t)))
	agent := tcJob09Container(t, template.Spec.Template.Spec.Containers, "agent")
	wantCommand := []string{tcJob09InitPrefix, tcJob09Separator, tcJob09Multica}
	if len(agent.Command) < len(wantCommand) {
		t.Fatalf("agent command = %v, want the prefix %v followed by the binary", agent.Command, wantCommand)
	}
	for i, want := range wantCommand {
		if agent.Command[i] != want {
			t.Errorf("agent command[%d] = %q, want %q (verbatim prefix, ADR-022)", i, agent.Command[i], want)
		}
	}
	if len(agent.Args) == 0 || agent.Args[0] != "daemon" {
		t.Errorf("agent args = %v, want the daemon subcommand first (the prefix does not change the args face)", agent.Args)
	}

	// The rendered task Job keeps both faces: Proxy/builder never rewrite a
	// template's command or args.
	h := newHarness(t, false)
	h.useRealBuilder(t)
	tcPrepareRuntimes(t, h)
	tcClaimOnClaim(t, h, tcBaseClaim("task-1"))
	rendered := tcJob09Container(t, h.createdJob(t, "fm-task-1").Spec.Template.Spec.Containers, "agent")
	if strings.Join(rendered.Command, "\x00") != strings.Join(agent.Command, "\x00") {
		t.Errorf("rendered command = %v, want the template's %v verbatim", rendered.Command, agent.Command)
	}
	if strings.Join(rendered.Args, "\x00") != strings.Join(agent.Args, "\x00") {
		t.Errorf("rendered args = %v, want the template's %v verbatim", rendered.Args, agent.Args)
	}
}
