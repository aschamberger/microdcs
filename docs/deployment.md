# Deployment

How to run MicroDCS on Kubernetes, which limits apply, and how islands and tenants are separated. Security settings and the trust
model are in [Security](security.md).

## Shape of a deployment {#shape}

One island is one independent deployment with its own Valkey ([D-12](porting/decisions.md#d-12)): a plant area, a line, a site.
Inside an island the same image runs in two roles ([D-51](porting/decisions.md#d-51)):

| Deployment | `Roles` | Replicas | Does |
|---|---|---|---|
| `microdcs-processor` | `Processor` | 2 or more | Subscribes the bindings (shared subscriptions or queue groups spread the messages), runs the processors and the SFC host. Valkey streams and compare-and-set scripts keep each piece of work with one replica (D-44, D-53) |
| `microdcs-publisher` | `Publisher` | 1 | Writes the retained state topics for the MES. A lease in Valkey keeps a second publisher from overwriting them during a rolling update; a restart is enough failover because the MES reads the retained values meanwhile |

`Roles=All` (the default) runs both in one process, for development and small sites.

## The manifest {#manifest}

[`deploy/`](https://github.com/aschamberger/microdcs/tree/main/deploy) holds an example to adapt, not a chart:

* `config.yaml`: the ConfigMap with the settings both Deployments share.
* `k8s.yaml`: ServiceAccount, the two Deployments, a PodDisruptionBudget for the processors and three NetworkPolicies (default
  deny, egress, management ingress).
* `kustomization.yaml`: pins the image. Set the digest of an image you verified
  ([Verifying the image](security.md#verifying-the-image)):
  `kustomize edit set image ghcr.io/aschamberger/microdcs=ghcr.io/aschamberger/microdcs@sha256:<digest>`. Until then it names the
  moving `main` tag and sets `imagePullPolicy: Always`, so no node keeps an older `main` build; remove that patch with the digest.
* `valkey-island-acl.conf`: the Valkey ACL user of an island ([Tenancy](#tenancy)).

```bash
kubectl create secret generic microdcs-management --from-file=token=./management-token -n <ns>
kubectl create secret generic microdcs-valkey --from-file=password=./valkey-password --from-file=ca.crt=./valkey-ca.crt -n <ns>
kubectl create secret generic microdcs-mqtt --from-file=ca.crt=./broker-ca.crt -n <ns>
kubectl apply -k deploy/ -n <ns>
```

Settings are environment variables with the prefix `APP_` and `__` for `:` ([D-39](porting/decisions.md#d-39)); the shared ones sit
in the ConfigMap, the role, the instance id (`POD_ID` from the pod name) and the paths of the mounted secrets in each Deployment.
The NetworkPolicy expects the labels `app.kubernetes.io/name=valkey`, `=mqtt-broker` and `=otel-collector` on the dependencies;
change the selectors to match yours, and add NATS (4222) when a binding uses it.

### Probes and startup {#probes}

| Probe | Route | Meaning |
|---|---|---|
| Liveness | `/health/live` | The process answers. A lost dependency ends the process itself (D-27), so there is nothing else to check |
| Readiness | `/health/ready` | Every transport is connected and Valkey answers `PING` ([D-54](porting/decisions.md#d-54)) |

The management listener binds the pod address (`Url=http://0.0.0.0:8081`), which needs `BearerTokenFile`; the probes stay open.

A pod that starts before Valkey or the broker accepts connections ends, and Kubernetes starts it again with an increasing back-off
(up to five minutes). That is the fail-fast rule (D-27), not an error, but a long back-off delays the start after the dependency is
up. Start the dependencies first, or delete the waiting pods once they are ready.

`terminationGracePeriodSeconds: 45` is longer than `ShutdownGracePeriod` (30 s), so the drain finishes before the kubelet kills
the process (D-37).

### Resources {#resources}

The requests and limits in the example (processor 100m to 1 CPU, 128 to 256 MiB; publisher 50m to 500m, 96 to 192 MiB; 16 to
64 MiB ephemeral storage) are starting points from the kind test, not measurements under load. Size them with the limits below:
the memory a processor needs grows with `MessageWorkers` and `OutgoingQueueMaxSize` times the largest message.

## Testing the manifest {#kind-test}

`scripts/k8s-test` runs `deploy/` in a kind cluster with the overlay in `tests/k8s` (no TLS, no collector, the image built locally)
and checks:

* both Deployments become ready through their probes in a namespace that enforces the Pod Security Standard `restricted`;
* nothing restarts after the rollout;
* the egress policy lets a MicroDCS pod reach DNS, Valkey and the broker, and nothing else in or outside the cluster;
* the management port answers only pods with the client label, and `/api/v1/jobs` needs the token;
* Valkey runs with the `default` user off and the restricted ACL user from [Tenancy](#tenancy), and denies nothing.

```bash
scripts/k8s-test                 # build, create the cluster, test, delete it
KEEP_CLUSTER=1 scripts/k8s-test  # keep the cluster for debugging; a rerun reuses it
```

It needs Docker, `kind` and `kubectl`; `scripts/k8s-test install <dir>` installs the pinned versions with checksums. CI runs it in
the `kubernetes` job on `main`, daily, and on pull requests that change the manifest or the image inputs
([D-56](porting/decisions.md#d-56)).

### Not verified yet {#not-verified}

These need a real cluster or a long run and are open:

* TLS to the broker and to Valkey inside a cluster, and the Kubernetes service account token (`K8S-SAT`) with a broker that
  supports it.
* An admission policy (Sigstore policy-controller, Kyverno) enforcing the signature identity.
* NetworkPolicy behaviour on other network plugins (Calico, Cilium), including whether kubelet probes are exempt from the
  ingress policy as they are with kindnet.
* Several nodes: the PodDisruptionBudget during a drain, scheduling spread, the publisher lease hand-over in a rolling update under
  load.
* Resource sizing under load, and the arm64 image on a cluster.

## Operational limits {#limits}

MicroDCS sets no message size limit of its own: `custommetadata` is unbounded (D-29), and the OPC UA JSON decoder of the job models
has no string, array or message limit (only its nesting limit), because recipes are opaque and can be large. The transport bounds a
message, so set its limit deliberately.

| Where | Limit | Default | Recommendation |
|---|---|---|---|
| MQTT broker | maximum packet size (Mosquitto `max_packet_size`, `message_size_limit`) | MQTT allows up to 256 MB | Set it to the largest real message with headroom (for example 1 to 4 MB); MicroDCS sends nothing larger than it receives |
| NATS server | `max_payload` | 1 MB (server maximum 64 MB) | Raise only if recipes need it; JetStream `max_msg_size` follows `max_payload` |
| Valkey | `proto-max-bulk-len` per value | 512 MB | Keep the default; documents are hashes with one serialized field |
| Change streams | `ChangeStreamMaxLength` | 5000 entries | Per scope, approximate trimming (D-42); the publisher rebuilds a scope trimmed past its cursor |
| Pipeline | `MessageWorkers`, `OutgoingQueueMaxSize`, per-binding `OutgoingQueueSize` | 5, 1000 | A full queue fails the enqueue (D-28) |
| Management listener (Kestrel) | request line 8 KB, headers 32 KB, body 30 MB | Kestrel defaults | Routes are `GET` only; `RequestsPerMinute` (120) per caller and `MaxResults` (100) bound the work |
| Telemetry | OTLP exporter batch and queue sizes | SDK defaults (`OTEL_BSP_*`, `OTEL_BLRP_*`) | Drops spans and logs instead of blocking when the collector is slow |

Memory bound per processor, roughly: `MessageWorkers` × largest inbound message + outgoing queue entries × largest outbound message.

## Tenancy {#tenancy}

The recommended separation is one island per tenant: own namespace, own Valkey, own broker credentials. Inside shared
infrastructure:

| Component | Separation |
|---|---|
| MQTT | A binding prefix per tenant and broker ACLs that limit each tenant's identities to that prefix. MQTT has no namespaces; the ACL is the boundary |
| NATS | One account per tenant, selected by the credentials; subjects, streams and the state bucket are separate per account, so tenants can use the same prefix ([NATS transport](nats-transport.md#tenancy), D-47) |
| Valkey | A `KeyPrefix` per island and an ACL user limited to it (below). One Valkey per island is still preferred: it also separates load and backups |
| Management API | One bearer token per instance, not per tenant; do not share an instance between tenants |

Valkey ACL user for the island with `KeyPrefix=site1`. The Valkey integration tests run the production code as such a user
(`valkey-acl` in the compose harness), and the kind test runs the application with it:

```text
--8<-- "deploy/valkey-island-acl.conf"
```

All keys, including the dedupe claims, leases and the scope registry, and the nudge channels sit under `{KeyPrefix}:`. MicroDCS
turns off the client's housekeeping that would reach outside the prefix (tiebreaker key, configuration channel) or read server-wide
data (`INFO`, `CONFIG`, `CLUSTER`). The rule takes away every server-wide command that would let one island flush, kill or watch
another; a test checks that the user is refused them. `SCAN` stays because the index and maintenance tools need it, and it lists
the key names of every island: on a shared Valkey the islands see each other's job and scope ids, not their contents.

## Support {#support}

| Item | Supported |
|---|---|
| Platforms | `linux-x64` and `linux-arm64` ([D-32](porting/decisions.md#d-32)), one multi-arch image |
| Kubernetes | The kind test runs Kubernetes 1.37; the manifest uses only stable APIs (`apps/v1`, `policy/v1`, `networking.k8s.io/v1`) |
| Valkey | 9.x (`valkey-bundle`, D-26), single endpoint, no cluster mode |
| Brokers | MQTT 5 brokers (tested with Mosquitto 2), NATS 2.x with JetStream |
| Versions | Pre-1.0: only the latest release and `main` get fixes ([SECURITY.md](https://github.com/aschamberger/microdcs/blob/main/SECURITY.md)) |

The project has one maintainer and no service-level agreement. Operators own the deployment, the dependencies (broker, Valkey,
Kubernetes) and their hardening; the split is in the [IEC 62443 table](security.md#iec-62443).
