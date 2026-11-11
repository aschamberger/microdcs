# Deployment, supply chain and security review (S22)

Session S22, 2026-11-11. Inputs: the S22 block, D-20, D-32, D-51, D-54, the Python manifest, Dockerfile, `docs/security.md` and
`SECURITY.md` on `python-archive`, and `src/microdcs/__init__.py` there (configuration). Maintainer choices at the start: GHCR on
`main` and `v*` tags, cosign keyless sign and attest, a kind job in CI after a local run, everything in one session, and a later
session that reviews the CI runtime.

## Python parity

| Id | Python behaviour | Where | Disposition |
|---|---|---|---|
| K-1 | Two Deployments, processor (2 replicas) and publisher (1), same image | `deploy/k8s.yaml` | kept, by `Roles` (D-51) |
| K-2 | `APP_IS_PROCESSOR_INSTANCE`, `APP_IS_PUBLISHER_INSTANCE` flags | `deploy/k8s.yaml`, `__init__.py` | changed: `MicroDcs:Roles` (D-39, D-51) |
| K-3 | No probes; the process exits when Redis or MQTT is lost | `deploy/k8s.yaml` header | changed: liveness and readiness on the management listener (D-54); the exit stays (D-27) |
| K-4 | Pod hardening: non-root, read-only root, no privilege escalation, drop all, RuntimeDefault seccomp, no API token, `emptyDir` on `/tmp` | `deploy/k8s.yaml` | kept; user 1654 of the chiseled image instead of 65532 of distroless; own ServiceAccount; size limit on `/tmp`; ephemeral-storage requests and limits (Sonar S6870, S6897) |
| K-5 | NetworkPolicy denying all ingress | `deploy/k8s.yaml` | kept and extended: default deny in and out, egress to DNS, Valkey, broker, collector, management ingress from labelled clients |
| K-6 | `POD_ID` from `metadata.uid` | `deploy/k8s.yaml` | changed: `metadata.name`, readable in logs and inspection and unique per running pod |
| K-7 | Shared subscription name `APP_PROCESSING_SHARED_SUBSCRIPTION_NAME` | `__init__.py` | kept: binding `SharedGroup` (D-46) |
| K-8 | Retained topic TTL 48 h (`APP_PUBLISHER_RETAINED_TTL_SECONDS=172800`) | `deploy/k8s.yaml`, `__init__.py` | kept: `MicroDcs:JobPublisher:RetainedTtl` default 48 h, not set in the manifest |
| K-9 | Redis `username`, `password_file` (one password source only), `ssl`, `ssl_ca_certs` | `__init__.py` `RedisConfig` | kept: Valkey `User`, `PasswordFile`, `TlsCaFile`, `RequireTls` (S22); the file is read before every connect (D-49 revised) |
| K-10 | Redis socket timeouts, keepalive, `health_check_interval` | `__init__.py` `RedisConfig` | dropped: StackExchange.Redis heartbeat and timeouts (D-7), readiness check `valkey` (D-54) |
| K-11 | `REQUIRE_TLS` for MQTT, SAT token, password file, client certificate | `docs/security.md` | kept (S13); documented anew |
| K-12 | MessagePack-RPC listener with TLS and client certificates | `docs/security.md` | dropped: MessagePack-RPC is removed (driver 4) |
| K-13 | `APP_LOGGING_LOG_PAYLOADS` and `loggable()` for payloads in `DEBUG` logs | `__init__.py`, `docs/security.md` | changed: no switch; payloads never reach logs, only the inspection service shows them with `IncludePayloads` (D-40) |
| K-14 | `ENFORCE_SUBJECT_TOPIC_MATCH`, nothing checked at level 0 | `__init__.py`, `docs/security.md` | changed: `ScopeCheck` and `Level0Subjects=Reject` by default (D-38, R-6) |
| K-15 | `SECURITY.md` with reporting, support and scope | `SECURITY.md` | kept; scope now names the signed GHCR image |
| K-16 | Distroless Python image with `opentelemetry-instrument` | `Dockerfile` | changed: AOT binary in chiseled runtime-deps, OTLP from the SDK (D-32, D-54) |
| K-17 | File logging off because of the read-only root (`APP_LOGGING_FILENAME`) | `docs/security.md` | dropped: console and OTLP only |
| K-18 | Scaffolder template `scripts/init/k8s.yaml` | `src/microdcs/scripts/init/` | dropped here: `dotnet new` templates in S26 |
| K-19 | Image tag `:latest`, "pin a digest in production" | `deploy/k8s.yaml` | changed: `kustomization.yaml` pins the image; published images are signed (D-55) |

No row is open.

## "Done in the session" behaviours, checked against the code

The list in [review triage](review-triage.md#behaviours-from-done-in-the-session-that-the-net-version-must-reproduce):

| Behaviour | .NET | Evidence |
|---|---|---|
| TLS plus `require_tls` for every transport | MQTT, NATS, Valkey (`RequireTls`) | options validator tests of the three projects |
| MQTT user and password, client certificates | `MqttOptions` | `MqttOptionsValidatorTests`, RV17 compose test |
| Password files | MQTT, NATS, Valkey, re-read per connect (D-49) | RV17 tests of MQTT, NATS and Valkey |
| Subject and topic scope check | `ScopePolicy`, `Level0Subjects=Reject` (D-38) | Core scope tests, transport tests |
| Payloads kept out of logs | no log template takes a payload; inspection redacts (D-40) | template search in S22, inspection redaction tests |
| Valkey timeouts | StackExchange.Redis defaults, `connectTimeout=`/`syncTimeout=`/`asyncTimeout=` in the connection string | `CredentialTests` sets `connectTimeout` |
| Dedupe lease: a crash never loses a message, duplicates are acknowledged | claim with lease, release on fault (D-37) | `DedupeAndLeaseTests`, `PipelineTests` |
| Stable `mdcsactionkey` on push commands | `Commands`, codec extension attribute | `SfcHostTests`, `SfcReplicaTests` |

The acceptance tests that Q-TEST-1/2 ask for are S23's.

## Findings

* **Valkey credentials were missing** (K-9). The S09 options had only a connection string, so a manifest had to put the password
  into an environment variable. StackExchange.Redis 3.4 has no credential callback, but `ConfigurationOptions.BeforeSocketConnect`
  runs before every socket connect and the multiplexer keeps the instance, so setting `Password` there reaches every reconnect.
  The rotation test needs a Valkey that requires authentication: on a server whose `default` user has no password, a rejected AUTH
  leaves the connection working as `default`, and the multiplexer never reconnects (seen on the first attempt). Hence the
  `valkey-acl` harness service.
* **Restricted ACL user.** StackExchange.Redis touches a tiebreaker key and a configuration channel outside any prefix and sends
  `INFO`, `CONFIG GET`, `CLUSTER NODES/SLOTS` on connect; with a prefix-limited user these are denied (it copes, but the ACL log
  fills). With one endpoint (D-12) none is needed, so `ValkeyConnection` turns them off. The SFC host needs `TIME` (D-44), which is
  in no command category of the rule; the restricted test mode found it (three SFC tests fail without `+time`).
* **kind and NetworkPolicy.** kind 0.33 (Kubernetes 1.37) enforces NetworkPolicy with kindnet, and kubelet probes pass a deny-all
  ingress policy. Valkey rejects ACL rules with spaces on the command line; the test passes them in a mounted config file.
* **Fail-fast at startup** produces restarts when the application starts before Valkey (D-27). The kind test therefore checks that
  nothing restarts after the rollout; the deployment page explains the back-off.
* **Trivy KSV-0109** ("ConfigMap with secrets") matched the keys `PasswordFile` and `BearerTokenFile` although the values are paths;
  the paths moved into the Deployments, next to the secret volumes, instead of a waiver.
* **zizmor cache-poisoning:** a `v*` tag trigger turns `ci.yml` into a release workflow for zizmor, which then flags the
  `setup-dotnet` cache. Tag builds now use no caches at all (dotnet and image); `main` builds keep them.
* **Log noise:** the management listener logged every probe at Information, and every start warned that `ASPNETCORE_HTTP_PORTS`
  (set by the base image) is overridden. the listener's web host now holds the `Microsoft.AspNetCore` categories at Warning unless
  `Logging:LogLevel:Microsoft.AspNetCore` says otherwise (`ManagementLoggerFactory`), and it clears the inherited port settings.
* **GHCR:** the image carries `org.opencontainers.image.source`, which links the package to the repository. Whether a new package
  of a personal account is public is checked after the first publish (post-merge).

## Supply chain design (D-55)

`publish` (matrix, native runners as in D-32) runs only on pushes after `dotnet`, `container`, `integration` and `kubernetes`
passed. It builds the image (from the container job's cache on `main`), exports it, creates two CycloneDX SBOMs with the Trivy gate
(`scripts/scan sbom`: the app's lock file, because the AOT binary hides its packages from an image scan, and the image's OS packages),
pushes by digest without provenance or buildx SBOM manifests, signs the digest and attests both SBOMs. `publish-manifest` creates the
index with the tags, signs it and verifies every signature and attestation against the workflow identity before it reports the digest.
cosign 3.1.4 through `sigstore/cosign-installer`; keyless certificates from the job's OIDC token.

Not chosen: GitHub artifact attestations (maintainer chose cosign, which more admission controllers check); provenance attestations
from buildx (they turn each platform push into an index with an `unknown/unknown` manifest, and the maintainer asked for signatures
and SBOMs); pushing from the `container` job (it runs on pull requests too, and should not hold `packages: write`).
