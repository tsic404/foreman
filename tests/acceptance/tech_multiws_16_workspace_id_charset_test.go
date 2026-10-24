// TC-tech-multiws-16 implements AC-105: the workspace identifier's value face.
// A workspace id is a label value and a workspace-placeholder render value, so
// a value outside the label grammar refuses startup (naming the id in both
// credential shapes), while every legal shape starts and renders verbatim —
// the check is never a normalization.
package acceptance

import (
	"bytes"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"testing"

	"github.com/tsic404/foreman/internal/jobbuilder"
	"github.com/tsic404/foreman/internal/observability"
	"github.com/tsic404/foreman/internal/registry"
	"github.com/tsic404/foreman/internal/workspace"
)

// charsetTemplate carries the two downstream faces of the id: a label value
// and a path-shaped placeholder position.
const charsetTemplate = `apiVersion: batch/v1
kind: Job
metadata:
  name: fm-<task_id>
  labels:
    foreman.tsic.top/workspace-id: "<workspace_id>"
spec:
  template:
    metadata:
      labels:
        foreman.tsic.top/workspace-id: "<workspace_id>"
    spec:
      containers:
        - name: agent
          image: <job-image>
      volumes:
        - name: home
          hostPath: { path: /var/lib/foreman/home/<workspace_id>, type: DirectoryOrCreate }
`

func TestTechMultiWS16WorkspaceIDCharset(t *testing.T) {
	workspaceScoped := func(id string) func(string) string {
		return func(key string) string {
			switch key {
			case workspace.EnvWorkspaces:
				return `[{"workspace_id":"` + id + `","daemon_id":"foreman-main","token":"mdt_token"}]`
			}
			return ""
		}
	}

	t.Run("illegal ids refuse startup", func(t *testing.T) {
		cases := map[string]string{
			"path separator": "/var/lib/foreman/home",
			"parent segment": "../../escape",
			"bare parent":    "..",
			"leading dot":    ".hidden",
			"trailing dot":   "trailing.",
			"leading hyphen": "-team",
			"bare hyphen":    "-",
			"space":          "ws id",
			"colon":          "ws:8080",
			"over length":    strings.Repeat("a", 64),
		}
		for name, id := range cases {
			t.Run(name, func(t *testing.T) {
				_, err := workspace.LoadDeployment(workspaceScoped(id))
				if err == nil {
					t.Fatalf("workspace id %q started", id)
				}
				if !strings.Contains(err.Error(), strconv.Quote(id)) {
					t.Errorf("error %q must name %q", err, id)
				}
				if !strings.Contains(err.Error(), "label value") {
					t.Errorf("error %q must state the label-value rule", err)
				}
			})
		}

		_, err := workspace.LoadDeployment(workspaceScoped(""))
		if err == nil {
			t.Fatal("an empty workspace id started")
		}
	})

	// A wrong identifier and a wrong credential in the same identity must not
	// let the credential error hide which id is refused.
	t.Run("illegal ids with a bad credential still name the id", func(t *testing.T) {
		shapes := map[string]string{
			"missing credential":    `{"workspace_id":%s,"daemon_id":"foreman-main"}`,
			"bad credential prefix": `{"workspace_id":%s,"daemon_id":"foreman-main","token":"not-a-token"}`,
		}
		for name, shape := range shapes {
			for _, id := range []string{"ws id", "-", "trailing.", "../../escape"} {
				t.Run(name+"/"+id, func(t *testing.T) {
					identity := fmt.Sprintf(shape, strconv.Quote(id))
					_, err := workspace.LoadDeployment(func(key string) string {
						if key == workspace.EnvWorkspaces {
							return "[" + identity + "]"
						}
						return ""
					})
					if err == nil {
						t.Fatalf("workspace id %q started with a %s", id, name)
					}
					if !strings.Contains(err.Error(), strconv.Quote(id)) {
						t.Errorf("error %q must name %q", err, id)
					}
				})
			}
		}
	})

	t.Run("discovery results obey the same rule", func(t *testing.T) {
		dep, err := workspace.LoadDeployment(func(key string) string {
			if key == workspace.EnvServerToken {
				return "mul_user"
			}
			return ""
		})
		if err != nil {
			t.Fatalf("LoadDeployment: %v", err)
		}
		if _, err := workspace.NewSet(dep.Identities([]workspace.Ref{{ID: "ws/escape"}})); err == nil {
			t.Fatal("a discovered workspace with an illegal id started")
		} else if !strings.Contains(err.Error(), strconv.Quote("ws/escape")) {
			t.Errorf("error %q must name the discovered id", err)
		}
	})

	t.Run("legal ids start and render verbatim", func(t *testing.T) {
		for _, id := range []string{
			"ws-a",
			"11111111-1111-4111-8111-111111111111",
			"WS_A.b",
			strings.Repeat("a", 63),
		} {
			t.Run(id, func(t *testing.T) {
				dep, err := workspace.LoadDeployment(workspaceScoped(id))
				if err != nil {
					t.Fatalf("workspace id %q refused: %v", id, err)
				}
				validated := dep.Set.IDs()
				if len(validated) != 1 {
					t.Fatalf("validated ids = %v, want exactly one", validated)
				}

				cfg, err := jobbuilder.LoadConfig(func(string) string { return "" })
				if err != nil {
					t.Fatalf("LoadConfig: %v", err)
				}
				cfg.TemplatePath = writeTemplateFile(t, charsetTemplate)
				cfg.JobImage = harnessJobImage
				cfg.Issuer = stubIssuer{}
				cfg.WorkspaceIDs = validated
				cfg.Logger = observability.NewLogger(&bytes.Buffer{}, slog.LevelInfo, "")
				builder, err := jobbuilder.NewBuilder(cfg)
				if err != nil {
					t.Fatalf("NewBuilder: %v", err)
				}
				task := taskEntryFor("11111111-1111-4111-8111-111111111111", validated[0], "")
				job, secret, _, err := builder.Build(task)
				if err != nil {
					t.Fatalf("Build: %v", err)
				}
				if got := job.Labels[registry.LabelWorkspaceID]; got != id {
					t.Errorf("Job workspace label = %q, want %q verbatim", got, id)
				}
				if got := job.Spec.Template.Labels[registry.LabelWorkspaceID]; got != id {
					t.Errorf("pod workspace label = %q, want %q verbatim", got, id)
				}
				if got := secret.Labels[registry.LabelWorkspaceID]; got != id {
					t.Errorf("credential Secret workspace label = %q, want %q verbatim", got, id)
				}
				var home string
				for _, volume := range job.Spec.Template.Spec.Volumes {
					if volume.Name == "home" && volume.HostPath != nil {
						home = volume.HostPath.Path
					}
				}
				if want := "/var/lib/foreman/home/" + id; home != want {
					t.Errorf("home hostPath = %q, want %q", home, want)
				}
			})
		}
	})
}
