#!/bin/sh
# TC-tech-auth-05 — the cluster half of AC-21's prepare self-heal: a pod whose
# node-shared home carries each of the two anomalous shapes (a directory
# position occupied by a file, the marker position occupied by a directory)
# still starts. The script renders the deployed reference template
# (ConfigMap foreman-job-template), pins it to one node, seeds the shape on
# that node's home hostPath, and asserts prepare exited 0 and the agent
# container started. The script-level half (the healed file shapes) is
# tests/acceptance/tech_auth_05_prepare_self_heal_test.go.
#
#   NODE=<node> ./tech-auth-05-cluster.sh
#   (NODE is required: the shape is seeded on one node, so the pod must land
#    there. The job image comes from the foreman Deployment's
#    FOREMAN_JOB_IMAGE; the rendered Job's agent credential is a dummy, so
#    the daemon may fail to register after the pod started — outside this
#    case's criterion.)
# Exit: 0 = PASS · 1 = FAIL (raw observation printed) · 2 = blocked.
set -eu

JOB_NS=${FOREMAN_JOB_NAMESPACE:-multica-agents}
FOREMAN_NS=${FOREMAN_NAMESPACE:-foreman}
TEMPLATE_CM=${FOREMAN_TEMPLATE_CONFIGMAP:-foreman-job-template}
NODE=${NODE:-}
HOME_PATH=${FOREMAN_HOME_PATH:-/var/lib/foreman/home}
JOB_IMAGE=${FOREMAN_JOB_IMAGE:-}
NODE_DEBUG_IMAGE=${NODE_DEBUG_IMAGE:-debian:bookworm-slim}
POD_START_TIMEOUT=${POD_START_TIMEOUT:-180}

need() { command -v "$1" >/dev/null 2>&1 || { echo "missing required command: $1" >&2; exit 2; }; }
need kubectl
need jq

blocked() { echo "QA_BLOCKED: $*" >&2; exit 2; }
fail() { echo "FAIL: $*" >&2; exit 1; }
observe() { printf '# observation: %s\n' "$*"; }

[ -n "$NODE" ] || blocked "NODE is required (the shape is seeded on that node's home hostPath)"
kubectl get node "$NODE" >/dev/null 2>&1 || blocked "cannot read node $NODE"

if [ -z "$JOB_IMAGE" ]; then
	JOB_IMAGE=$(kubectl -n "$FOREMAN_NS" get deploy foreman -o json 2>/dev/null |
		jq -r '[.spec.template.spec.containers[].env[]? | select(.name=="FOREMAN_JOB_IMAGE") | .value] | first // empty') ||
		JOB_IMAGE=""
fi
[ -n "$JOB_IMAGE" ] || blocked "FOREMAN_JOB_IMAGE is unset on the foreman Deployment (set JOB_IMAGE or the env var)"

template=$(kubectl -n "$FOREMAN_NS" get configmap "$TEMPLATE_CM" -o jsonpath='{.data.job-overlay\.yaml}' 2>/dev/null) ||
	blocked "cannot read ConfigMap $FOREMAN_NS/$TEMPLATE_CM"
[ -n "$template" ] || blocked "ConfigMap $FOREMAN_NS/$TEMPLATE_CM carries no job-overlay.yaml"

JOB_IDS=""
LAST_ID=""
tmpdir=$(mktemp -d)

cleanup() {
	[ -n "${NODE:-}" ] || return 0
	# Remove only the two shapes this script seeds; a healed path is left
	# alone (the profile directory is real state, not a fixture).
	kubectl debug "node/$NODE" --profile=sysadmin --image="$NODE_DEBUG_IMAGE" -i -q -- sh -c "
		R='/host$HOME_PATH'
		[ -f \"\$R/.multica/profiles\" ] && rm -f \"\$R/.multica/profiles\"
		[ -d \"\$R/.multica/daemon_task_context.json\" ] && rm -rf \"\$R/.multica/daemon_task_context.json\"
		true" >/dev/null 2>&1 || true
	for id in $JOB_IDS; do
		kubectl -n "$JOB_NS" delete job "fm-$id" --wait=false >/dev/null 2>&1 || true
		kubectl -n "$JOB_NS" delete secret "fm-$id-cred" --wait=false >/dev/null 2>&1 || true
	done
	rm -rf "$tmpdir"
}
trap cleanup EXIT

seed_shape() {
	shape=$1
	rc=0
	out=$(kubectl debug "node/$NODE" --profile=sysadmin --image="$NODE_DEBUG_IMAGE" -i -q -- sh -c "
		R='/host$HOME_PATH'
		[ -d \"\$R\" ] || { echo 'QA_BLOCKED: the home hostPath is not visible at /host' >&2; exit 2; }
		mkdir -p \"\$R/.multica\"
		case '$shape' in
		dir-as-file)
			rm -rf \"\$R/.multica/profiles\"
			printf 'stale\n' > \"\$R/.multica/profiles\" ;;
		file-as-dir)
			rm -rf \"\$R/.multica/daemon_task_context.json\"
			mkdir -p \"\$R/.multica/daemon_task_context.json\" ;;
		*) echo \"unknown shape $shape\" >&2; exit 2 ;;
		esac
		echo \"seeded $shape\"" 2>&1) || rc=$?
	if [ "$rc" -ne 0 ]; then
		why=$(printf '%s\n' "$out" | sed -n 's/^QA_BLOCKED: //p' | sed -n '1p')
		[ -n "$why" ] && blocked "$why"
		blocked "cannot seed $shape on node/$NODE (kubectl debug rc=$rc): ${out:-<empty>}"
	fi
	observe "seeded $shape under $HOME_PATH on $NODE"
}

render_job() {
	id=$1
	printf '%s\n' "$template" |
		sed \
			-e "s|<job-image>|$JOB_IMAGE|g" \
			-e "s|<task_id>|$id|g" \
			-e "s|<agent_id>|qa-auth05|g" \
			-e "s|<workspace_id>|qa-auth05|g" \
			-e "s|<job runtime uuid>|00000000-0000-4000-8000-000000000000|g" \
			-e "s|<RFC3339>|$(date -u +%Y-%m-%dT%H:%M:%SZ)|g" \
			-e "s|<preferred node>|$NODE|g" |
		awk -v node="$NODE" '
			{ print }
			/^[[:space:]]*nodeSelector:[[:space:]]*$/ { in_ns = 1; next }
			in_ns && /kubernetes\.io\/os: linux/ {
				match($0, /^[[:space:]]+/)
				print substr($0, 1, RLENGTH) "kubernetes.io/hostname: " node
				in_ns = 0
			}'
}

start_shape() {
	shape=$1
	LAST_ID="qa-auth05-$(date +%s)-$(printf '%s' "$shape" | tr -c 'a-z0-9' '-')"
	job="fm-$LAST_ID"
	JOB_IDS="$JOB_IDS $LAST_ID"

	seed_shape "$shape"
	render_job "$LAST_ID" >"$tmpdir/job.yaml"
	grep -q "kubernetes.io/hostname: $NODE" "$tmpdir/job.yaml" ||
		fail "the rendered Job carries no hostname nodeSelector; the pod may not land on $NODE"
	kubectl -n "$JOB_NS" create secret generic "fm-$LAST_ID-cred" \
		--from-literal=config.json='{"token":"fmj_qa_auth05"}' >/dev/null ||
		fail "cannot create the per-task Secret fm-$LAST_ID-cred"
	kubectl -n "$JOB_NS" create -f "$tmpdir/job.yaml" >/dev/null || fail "cannot create Job $job"
	observe "$shape: created $job on node $NODE (image $JOB_IMAGE)"

	deadline=$(( $(date +%s) + POD_START_TIMEOUT ))
	pod=""
	while :; do
		pod=$(kubectl -n "$JOB_NS" get pods -l "job-name=$job" -o json 2>/dev/null |
			jq -c '[.items[] | select(.metadata.deletionTimestamp == null)] | sort_by(.metadata.creationTimestamp) | last // empty') ||
			pod=""
		if [ -n "$pod" ]; then
			name=$(printf '%s' "$pod" | jq -r .metadata.name)
			phase=$(printf '%s' "$pod" | jq -r '.status.phase // ""')
			prepare_rc=$(printf '%s' "$pod" | jq -r '[.status.initContainerStatuses[]? | select(.name=="prepare") | (.state.terminated.exitCode // .lastState.terminated.exitCode)] | first // empty')
			omp_rc=$(printf '%s' "$pod" | jq -r '[.status.initContainerStatuses[]? | select(.name=="omp") | (.state.terminated.exitCode // .lastState.terminated.exitCode)] | first // empty')
			waiting=$(printf '%s' "$pod" | jq -r '[.status.initContainerStatuses[]?.state.waiting.reason, .status.containerStatuses[]?.state.waiting.reason] | map(select(. != null)) | first // empty')
			started=$(printf '%s' "$pod" | jq -r '[.status.containerStatuses[]? | select(.name=="agent") | (.state.running.startedAt // .state.terminated.startedAt // empty)] | first // empty')
			case $waiting in
			ImagePullBackOff | ErrImagePull | InvalidImageName)
				blocked "$shape: the environment cannot pull the image ($waiting) for pod $name" ;;
			esac
			if [ -n "$omp_rc" ] && [ "$omp_rc" != "0" ]; then
				blocked "$shape: the omp init container failed (exit $omp_rc) — the environment cannot run the case"
			fi
			if [ -n "$prepare_rc" ] && [ "$prepare_rc" != "0" ]; then
				fail "$shape: prepare exited $prepare_rc — the pod did not start (phase $phase): $(printf '%s' "$pod" | jq -c '.status.initContainerStatuses')"
			fi
			# A recorded agent start proves the pod booted past prepare. This
			# judgement comes before the Failed branch: restartPolicy Never
			# turns a post-start exit into pod Failed, and with the dummy
			# credential that is the case's pass, not a failure.
			if [ "$prepare_rc" = "0" ] && [ -n "$started" ]; then
				observe "$shape: pod $name started the agent container at $started (prepare exit 0; phase $phase)"
				return 0
			fi
			if [ "$phase" = "Failed" ]; then
				fail "$shape: pod $name failed before the agent container started: $(printf '%s' "$pod" | jq -c '.status.containerStatuses')"
			fi
		fi
		[ "$(date +%s)" -lt "$deadline" ] ||
			fail "$shape: no started pod within ${POD_START_TIMEOUT}s (last: ${pod:-none})"
		sleep 2
	done
}

start_shape dir-as-file
# The next shape needs the first Job's daemon gone: its writes would race the
# seeded marker. Delete the Job and wait for its pods to disappear.
kubectl -n "$JOB_NS" delete job "fm-$LAST_ID" --cascade=foreground --wait=true --timeout=60s >/dev/null 2>&1 ||
	fail "cannot delete Job fm-$LAST_ID before the second shape"
start_shape file-as-dir

echo "PASS: TC-tech-auth-05 (both anomalous shapes started a pod on node $NODE)"
