# Deploying Foreman

`deploy/` is the whole deployment surface from `docs/03-contracts.md` §5.3.
Everything is plain YAML — `kubectl apply -f deploy/` installs it in one pass.

| File | Objects |
|---|---|
| `00-namespaces.yaml` | `Namespace foreman`, `Namespace multica-agents` |
| `10-secrets.yaml` | `Secret foreman-secret` (ns foreman), `Secret registry-tsic` (ns multica-agents + ns foreman) |
| `20-rbac.yaml` | `ServiceAccount foreman` + `Role`/`RoleBinding` (ns multica-agents), `ServiceAccount multica-omp`, `ServiceAccount foreman-gc` |
| `30-foreman.yaml` | `Deployment foreman` (1 replica) + `Service foreman` (ClusterIP :8080) |
| `40-foreman-gc.yaml` | `DaemonSet foreman-gc` (ns multica-agents) |
| `optional/servicemonitor.yaml` | `ServiceMonitor foreman` — needs the Prometheus Operator CRDs |

```bash
kubectl apply -f deploy/                             # namespaces → secrets → rbac → workload
kubectl apply -f deploy/optional/servicemonitor.yaml # only with the Operator CRDs
kubectl -n foreman rollout status deploy/foreman
```

The Service is reachable inside the cluster as
`foreman.foreman.svc.cluster.local:8080` — that hostname is baked into every
Job by the job builder, so the Service name must stay `foreman` in namespace
`foreman`.

## Secrets

`10-secrets.yaml` carries **placeholders**; `kubectl apply -f deploy/` will
overwrite live values with them, so pick one of:

- edit the file before applying (replace every `REPLACE_ME`), or
- create the Secrets out of band and remove `10-secrets.yaml` from the apply set.

The shipped `FOREMAN_JOB_TOKEN_KEY` placeholder is deliberately not valid hex:
an un-replaced copy makes Foreman fail its startup self-check and CrashLoop,
rather than serve with a key anyone can read from the repository.

```bash
# Server credential: mdt_/mul_ token bound to FOREMAN_DAEMON_ID + workspace.
kubectl -n foreman create secret generic foreman-secret \
  --from-literal=MULTICA_TOKEN='mdt_...' \
  --from-literal=FOREMAN_JOB_TOKEN_KEY="$(head -c 32 /dev/urandom | xxd -p -c 64)" \
  --dry-run=client -o yaml | kubectl apply -f -

# Private registry pull secret (ns multica-agents and ns foreman).
kubectl -n multica-agents create secret docker-registry registry-tsic \
  --docker-server=registry.tsic.top --docker-username=<user> --docker-password=<password> \
  --dry-run=client -o yaml | kubectl apply -f -
kubectl -n foreman create secret docker-registry registry-tsic \
  --docker-server=registry.tsic.top --docker-username=<user> --docker-password=<password> \
  --dry-run=client -o yaml | kubectl apply -f -
```

`FOREMAN_JOB_TOKEN_KEY` must decode to at least 32 bytes of hex (64 hex
characters); `MULTICA_TOKEN` must start with `mdt_` or `mul_`.

## Images

Two images come from this repo (`docs/04-architecture.md` §部署/运行方式):

```bash
make build-foreman        # bin/foreman        → registry.tsic.top/multica/foreman:<ver>
make build-foreman-image  # container for the Deployment above
make build-job-image      # Job image: upstream multica CLI + omp + foreman-gc
```

The Job image is referenced **by digest** in two places that must agree:

- `FOREMAN_JOB_IMAGE_DIGEST` in `30-foreman.yaml` (rendered into every Job as
  `image@sha256:…`), and
- the `image:` of the `foreman-gc` DaemonSet in `40-foreman-gc.yaml`
  (the DaemonSet runs the same image as the Job pods).

CI builds the Job image from upstream release artifacts and publishes it; this
repo only consumes the digest (NG8). `make build-job-image` is the local
equivalent used for staging clusters — it takes the artifact URL + SHA-256 for
the upstream CLI and for `omp`, so the built image can be checked against the
upstream release (AC-13 provenance).

### Publishing (CI)

`.github/workflows/release-images.yml` runs on a `v*.*.*` tag: it builds and
pushes both images, then emits the mapping the deploy surface consumes and
verifies it against the registry. It needs

- a runner that can reach the private registry (the `FOREMAN_RELEASE_RUNNER`
  repository variable overrides the default `["self-hosted","linux"]` labels),
- secrets `MULTICA_CLI_URL`/`MULTICA_CLI_SHA256` and `OMP_URL`/`OMP_SHA256`
  (upstream release artifacts), `REGISTRY_USERNAME`/`REGISTRY_PASSWORD`, and
  `REGISTRY_CA_PEM` when the registry certificate is not publicly trusted.

Every step failure fails the job — a push that never landed, a TLS trust gap or
a digest that is not actually served is a red build, never a silent
half-release.

The same three commands run by hand against any staging registry:

```bash
make publish-images         # build + push, writes dist/release-images.env
make pin-digest FROM=dist/release-images.env
make verify-delivery CLUSTER=1   # + registry API/TLS, published tags, live pods
```

`publish-images` prints the deployment-facing mapping `<sha7>:<digest>` (the
Job image digest of that commit); `dist/release-images.env` carries both image
digests and is uploaded as the workflow artifact. `pin-digest` writes the Job
digest into **both** places above and refuses to run if either file no longer
has exactly one digest reference — run it after every release and commit the
result, so `kubectl apply -f deploy/` never ships the placeholder digest.
`verify-delivery --static` (no network) gates pull requests on exactly that
consistency.

Knobs beyond the defaults: `publish-images` honours `REGISTRY`, `BASE_IMAGE`,
`CONTAINER_TOOL`, `PUSH_LATEST` (publish `foreman:latest` too) and `OUT`;
`verify-delivery` honours `REGISTRY_HOST`, `REGISTRY_PATH`, `VERSION`,
`CA_FILE`, `DEPLOY_DIR`, `DEPLOY_NAMESPACE`, `JOB_NAMESPACE` and `TIMEOUT`;
`qa-registry-trust` takes `--registry-host` and `--no-restart`. Each script
prints its header with `--help`.

## QA cluster registry access

`registry.tsic.top` is a private registry: the k3d QA cluster must *resolve* it
and *trust* its certificate. `--tls-skip-verify` and `imagePullPolicy: Always`
workarounds are not the path — install the CA and let containerd verify:

```bash
# On the docker host of the k3d cluster (CA = PEM that signed registry.tsic.top).
make qa-registry-trust NODE=k3d-test-server-0 \
  CA=/path/registry-ca.crt HOST_IP=<address the node reaches the registry on>
```

The script copies the CA into the node (per-registry `registries.yaml` config +
the node's system trust store), mirrors the registry hostname to that endpoint
and restarts the node so containerd loads it. Node-local resolution of the
registry hostname is added as well, for tools that bypass containerd (`curl`
in the node, `crictl pull` by name); pods resolve the hostname through the
cluster DNS entry added the same way.

If the registry is broken cluster-side — `GET /v2/` answering 404, a default
`ingress.local` certificate, a digest that no longer exists — `make
verify-delivery` fails with the specific `ALERT:` line. That check is the
deploy-pipeline signal for `ErrImagePull`/`ImagePullBackOff`; `CLUSTER=1` adds
the live pod check on top of it.

## Node-local state

Job pods and `foreman-gc` share the node directory `${FOREMAN_STATE_ROOT}`
(default `/var/lib/foreman`):

- Job pods mount `${FOREMAN_STATE_ROOT}/{home,workspaces}`,
- `foreman-gc` mounts `${FOREMAN_STATE_ROOT}` at `/state`

The DaemonSet's `hostPath.path` and the Deployment's `FOREMAN_STATE_ROOT` must
be changed together; the collector's own view is the mount point, passed back
in as `FOREMAN_STATE_ROOT=/state`.

## Configuration

All keys with their contract defaults are spelled out in the Deployment env.
The ones a staging/test cluster usually overrides:

| Key | Staging value | Why |
|---|---|---|
| `MULTICA_SERVER_URL` | `http://stub:9999` | protocol stub instead of the real server |
| `FOREMAN_MAX_INFLIGHT_JOBS` | `2` | concurrency assertions |
| `FOREMAN_JOB_BOOT_TIMEOUT` | `60s` | boot-timeout assertions |
| `FOREMAN_LOG_LEVEL` | `debug` | heartbeat / WS assertions |

## Post-install checks

```bash
kubectl get ns foreman multica-agents
kubectl -n multica-agents get sa multica-omp
kubectl -n multica-agents get secret registry-tsic
kubectl -n foreman get secret foreman-secret
kubectl -n foreman port-forward deploy/foreman 8080:8080 &
curl -s localhost:8080/foreman/healthz
curl -s localhost:8080/metrics | grep -cE '^foreman_'
```
