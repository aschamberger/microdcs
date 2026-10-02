# Security and Authorization

MicroDCS does not authenticate or authorize callers itself. Identity and access control are
delegated to the transports: the MQTT broker (client identity and topic ACLs), the Redis
server (ACL user and network access) and, for MessagePack-RPC, TLS and the network. This page
describes what the framework trusts, so that broker ACLs can be written to cover the gaps.

## Trust Model

| Boundary | Authentication | Authorization |
|---|---|---|
| MQTT broker | `K8S-SAT` token from `APP_MQTT_SAT_TOKEN_PATH` if the file exists; TLS with the CA from `APP_MQTT_TLS_CERT_PATH` if the file exists | Broker topic ACLs only |
| Redis | `APP_REDIS_USERNAME` / `APP_REDIS_PASSWORD`, optional TLS (`APP_REDIS_SSL`) | Redis ACLs, network policy |
| MessagePack-RPC | TLS with a server certificate (`APP_MSGPACK_TLS_SERVER_CERT_PATH` / `_KEY_PATH`) if present; optional client certificates (`APP_MSGPACK_TLS_CLIENT_AUTH`) | None; any connected client can call every registered method |

## TLS

TLS is switched on by the presence of certificate files, so a missing secret mount results in a
plaintext connection by default. Set `APP_MQTT_REQUIRE_TLS=true` and `APP_MSGPACK_REQUIRE_TLS=true`
in production: startup then fails with a message naming the missing file instead of running in
plaintext. The MQTT client needs the broker CA (`APP_MQTT_TLS_CERT_PATH`). The MessagePack-RPC
server needs a certificate and key (`APP_MSGPACK_TLS_SERVER_CERT_PATH`,
`APP_MSGPACK_TLS_SERVER_KEY_PATH`, by default `tls.crt` and `tls.key` as in a Kubernetes TLS
secret). `APP_MSGPACK_TLS_CLIENT_AUTH=true` additionally requires a client certificate signed by
the CA in `APP_MSGPACK_TLS_CERT_PATH` and implies `require_tls`.

## What the Framework Trusts

These behaviours determine what a broker ACL has to enforce.

1. **CloudEvent attributes come from the publisher.** The MQTT handler reads `id`, `source`,
   `type`, `subject`, `correlationid` and `causationid` from the MQTT user properties.
2. **The scope comes from `subject`, and the MQTT handler checks it against the topic.** The
   scope is the part of `subject` before the first `/` (`@scope_from_subject`). When the topic
   has a path between the prefix and the `[discriminator/]intent`, the subject must map to that
   path (`.` replaced by `/`, the same mapping used when publishing), or its scope part must
   equal it. A mismatch is logged and the message is dropped. With `app/jobs/lineA/commands`, a
   subject of `lineB` is rejected. Nothing is verified when the subject is absent or when the
   topic has no path (level 0), so principals allowed to publish at level 0 are not scope
   limited. Disable the check with `APP_PROCESSING_ENFORCE_SUBJECT_TOPIC_MATCH=false`.
   MessagePack-RPC has no topic and is not covered.
3. **Duplicate suppression is publisher-controlled.** Incoming messages are deduplicated by
   `source` + `id` for `APP_MQTT_DEDUPE_TTL_SECONDS`. A publisher that replays another
   publisher's `source` and `id` causes the genuine message to be dropped as a duplicate.
4. **Responses go to the requester's topic.** The response is published to the MQTT v5
   `ResponseTopic` property of the request. The application's publish permission is what limits
   where a client can direct responses.

## Topic ACL Model

Topics follow the structure in [Operations](operations.md#mqtt-topic-structure). With prefix `P`,
optional discriminator `D` and intent `I`:

| Direction | Application subscribes to | Application publishes to |
|---|---|---|
| `NORTHBOUND` (for example `machinery-jobs`) | `P[/+...]/[D/]commands` | `P/{subject-path}/[D/]data\|events\|metadata`, and responses to the request's `ResponseTopic` |
| `SOUTHBOUND` (for example `greetings`) | `P[/+...]/[D/]data\|events\|metadata` | `P/{subject-path}/[D/]commands` |

Every binding also subscribes to its response topic `{response_topic_base}/{instance_id}`, where the
instance id is the pod UID. With `APP_PROCESSING_SHARED_SUBSCRIPTION_NAME` set, the subscribe
filters are wrapped in `$share/{group}/`. The publisher additionally writes retained messages
under `P/{scope}/state-index`, `P/{scope}/order/{id}` and `P/{scope}/result/{id}`.

The example deployment (`machinery-jobs` on `app/jobs`, three wildcard levels, responses on
`app/jobs/responses`) needs these permissions:

| Principal | Connect | Subscribe | Publish |
|---|---|---|---|
| MicroDCS pods | yes | `app/jobs/commands`, `app/jobs/+/commands`, `app/jobs/+/+/commands`, `app/jobs/+/+/+/commands`, `app/jobs/responses/+` (and the `$share/appsub/` variants, depending on how the broker evaluates shared subscriptions) | `app/jobs/#` only |
| MES or other job-order clients | yes | their own response topic, `app/jobs/{their scope}/#` | `app/jobs/{their scope}/commands` |
| Equipment gateways (southbound) | yes | commands addressed to them | their `data`, `events` and `metadata` topics |

ACL rules for the application:

- **Restrict the application's publish permission to its own prefixes.** Because responses go to
  a client-chosen `ResponseTopic`, a broad publish permission lets any client make the application
  publish to arbitrary topics.
- **Restrict clients to their own response topic**, for example by using the client id in the
  topic (`app/jobs/responses/{clientid}`).
- **Scope the publish permission of each client to its own scope** (`app/jobs/{scope}/commands`).
  The handler then guarantees that the subject, and therefore the scope the processor acts on,
  is that scope. Do not grant publish rights on the level-0 topic (`app/jobs/commands`) to
  scoped clients: nothing is verified there.
- **Use unique client identities per principal.** Do not share credentials between equipment
  gateways and job-order clients; the `source` + `id` deduplication relies on distinct publishers.
- **Make retained topics writable only by the publisher.** Clients should have subscribe-only
  access to `app/jobs/+/state-index`, `app/jobs/+/order/+` and `app/jobs/+/result/+`.

The syntax of these rules is broker specific (Mosquitto ACL files, Azure IoT Operations
`BrokerAuthorization`, EMQX or HiveMQ rules). Verify a rule set by attempting a forbidden publish
and subscribe with a test client before relying on it.

## Redis

Redis holds the authoritative job and SFC execution state. Anyone who can write to it can change
job state, so:

- allow connections only from MicroDCS pods (network policy)
- use a dedicated ACL user limited to the keys under `APP_REDIS_KEY_PREFIX`
- enable `APP_REDIS_SSL` and `APP_REDIS_SSL_CA_CERTS` outside a single trusted node
- pass the password from a mounted secret with `APP_REDIS_PASSWORD_FILE` (for example
  `/var/run/secrets/redis/password`) rather than a literal `APP_REDIS_PASSWORD` in the manifest.
  The password is excluded from the configuration that is logged at `DEBUG`

## Kubernetes Hardening

[deploy/k8s.yaml](https://github.com/aschamberger/microdcs/blob/main/deploy/k8s.yaml) applies these settings to both Deployments:

| Setting | Effect |
|---|---|
| `runAsNonRoot`, `runAsUser: 65532` | Runs as the distroless non-root user |
| `readOnlyRootFilesystem: true` with an `emptyDir` on `/tmp` | The container cannot modify its image |
| `allowPrivilegeEscalation: false`, `capabilities.drop: [ALL]`, `seccompProfile: RuntimeDefault` | No extra privileges or capabilities |
| `automountServiceAccountToken: false` | No Kubernetes API token in the pod. This is separate from the projected `broker-sat` token for MQTT, which you mount yourself |
| `NetworkPolicy` `microdcs-deny-ingress` | Denies all ingress; the MessagePack-RPC port is for a sidecar in the same pod |

The container image runs with these restrictions. A read-only root filesystem is why file logging
is off by default (`APP_LOGGING_FILENAME`). The manifest has no liveness or readiness probes, because
the process exits when Redis or the MQTT broker is lost and Kubernetes restarts it. It uses
`:latest` for the image: pin a digest in production. The `NetworkPolicy` only covers ingress. Add an
egress policy limited to Redis, the MQTT broker and the OTLP endpoint once their labels are known.

## MessagePack-RPC

The listener defaults to `localhost` and is intended for a sidecar in the same pod. There is no
per-method authorization. If it is exposed beyond the pod, enable `APP_MSGPACK_TLS_CLIENT_AUTH`,
restrict access with a network policy, and keep the set of registered methods minimal.

## Known Gaps

| Gap | Mitigation today |
|---|---|
| Subject is only checked against topics that carry a scope path | No publish rights on the level-0 topic for scoped clients |
| Deduplication key is publisher-controlled | Distinct credentials per publisher; broker ACLs |
| TLS and SAT silently skipped when files are missing | Set `APP_MQTT_REQUIRE_TLS` and `APP_MSGPACK_REQUIRE_TLS` so startup fails instead; the SAT token is only sent over TLS if `require_tls` is set (a warning is logged otherwise) |
| No per-method authorization on MessagePack-RPC | Keep it pod-local or require client certificates |
