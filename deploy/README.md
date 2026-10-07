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

# GHCR pull secret (ns multica-agents and ns foreman): a GitHub token with
# read:packages, because the ghcr.io/tsic404/foreman* packages are private.
kubectl -n multica-agents create secret docker-registry registry-tsic \
  --docker-server=ghcr.io --docker-username=<github-user> --docker-password=<token> \
  --dry-run=client -o yaml | kubectl apply -f -
kubectl -n foreman create secret docker-registry registry-tsic \
  --docker-server=ghcr.io --docker-username=<github-user> --docker-password=<token> \
  --dry-run=client -o yaml | kubectl apply -f -
```

`FOREMAN_JOB_TOKEN_KEY` must decode to at least 32 bytes of hex (64 hex
characters); `MULTICA_TOKEN` must start with `mdt_` or `mul_`.

## Images

Two images come from this repo (`docs/04-architecture.md` §部署/运行方式): the
scheduler image for the Deployment, and the Job image (upstream multica CLI +
`omp` + `foreman-gc`) that both the Job pods and the `foreman-gc` DaemonSet run.

`.github/workflows/build-images.yml` builds and publishes both to GHCR — on
every push to `main` and on `v*.*.*` tags:

| Image | Tags |
|---|---|
| `ghcr.io/tsic404/foreman` | `<version>`, `latest` |
| `ghcr.io/tsic404/foreman-job` | `<version>` |

`<version>` is the pushed tag; on `main` it is the newest `v*.*.*` tag
reachable from the commit, falling back to the Makefile `VERSION` (`v0.1.0`) —
the same value `FOREMAN_JOB_IMAGE` below carries. `latest` follows `main` only.
The workflow needs no secrets for the push (`GITHUB_TOKEN`, `packages: write`)
and resolves the upstream Job-image artifacts from their release checksum
manifests; `MULTICA_CLI_VERSION` / `OMP_VERSION` repository variables pin other
upstream versions than the defaults in the workflow. `omp` is a native binary,
so the release asset must match the base image's libc: `OMP_ASSET` defaults to
`omp-linux-musl-x64` for the musl base (`FOREMAN_BASE_IMAGE`, default
`alpine:3.20`); a glibc base needs `OMP_ASSET=omp-linux-x64` as the repository
variable (or in the environment for a local build). A mismatch is caught by the
smoke step below, which runs `omp`.

The Job image is referenced **by digest** in two places that must agree:

- `FOREMAN_JOB_IMAGE_DIGEST` in `30-foreman.yaml` (rendered into every Job as
  `image@sha256:…`), and
- the `image:` of the `foreman-gc` DaemonSet in `40-foreman-gc.yaml`
  (the DaemonSet runs the same image as the Job pods).

CI publishes that digest and prints the deploy-facing mapping `<sha7>:<digest>`
in the workflow summary (and in the `release-images-<version>` artifact) — the
value this repo shares with the registry, so a cluster runs the image CI built:

```bash
make pin-job-image-digest DIGEST=sha256:…   # writes both references above
make check-deploy-digest                    # both exist, parse, and agree
```

`build-images` warns when `deploy/` still pins a digest other than the one it
just published, and `deploy-digest-check` fails any change whose two references
disagree. Until the first publish both carry the all-zero placeholder, so
`kubectl apply -f deploy/` needs the pin step first.

The same commands run by hand against any registry (the delivery entry points
CI calls); `make build-job-image` is the local equivalent used for staging
clusters and takes the artifact URL + SHA-256 for the upstream CLI archive and
for `omp`, so the built image can be checked against the upstream release
(AC-13 provenance):

```bash
make resolve-upstream                   # upstream artifact URL + SHA-256 pairs
make publish-images                     # build + push both images, print <sha7>:<digest>
make smoke-job-image IMAGE=ghcr.io/tsic404/foreman-job@sha256:…
```

`smoke-job-image` pulls the image and verifies three things: the upstream
`multica` binary inside it (AC-13), that the binaries the Job spec and the
DaemonSet exec are present, and that `omp` actually runs — `test -x` cannot
catch a binary built for another libc. With `--expect-digest` it also resolves
the tag and compares the digest with an independently published value, so it
takes a tag reference, not an `@sha256:…` pin (that form would only compare
itself; CI passes the tag plus the digest `publish-images` reported).

Both scripts need only a container CLI on `PATH` (`CONTAINER_TOOL`, default
`docker`) with a reachable daemon: a CI runner provides one, and a nix shell
does with `nix-shell -p docker --run 'make smoke-job-image IMAGE=…'` (or
`nix shell nixpkgs#docker-client` for the client alone, with the daemon on the
usual socket or `DOCKER_HOST`). The scripts have no nix dependency of their own.

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

## Job template overlay (optional)

The Deployment mounts the optional ConfigMap `foreman-job-template` at
`/etc/foreman/job-template/` (`optional: true`, key `job-overlay.yaml`).
Without the ConfigMap, Jobs render from the built-in default template; with it,
the deployment's partial `batchv1.Job` is merged in with strategic merge patch
semantics after a two-stage validation (contract §5.4):

```bash
kubectl -n foreman create configmap foreman-job-template --from-file=job-overlay.yaml=./job-overlay.yaml
kubectl -n foreman rollout restart deploy/foreman
```

- The file is read once at startup: an edit needs a Foreman restart, and the
  startup log records the overlay sha256 plus the applied/refused verdict.
- An illegal overlay refuses startup (CrashLoopBackOff) and logs each
  violation as `path: rule`; there is no env switch, the file's existence is
  the switch.
- `containers` stays a single container (`agent`): auxiliary containers are
  appended as `initContainers` entries — native sidecars (`restartPolicy:
  Always`) and one-shot prepare-init entries (no `restartPolicy`).

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
