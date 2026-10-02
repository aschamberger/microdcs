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
| MessagePack-RPC | Optional client certificates (`APP_MSGPACK_TLS_CLIENT_AUTH`) | None; any connected client can call every registered method |

TLS and the SAT token are only used when their files are present. A missing mount results in an
unauthenticated, plaintext connection, so verify the mounts in the deployment.

## What the Framework Trusts

These behaviours determine what a broker ACL has to enforce.

1. **CloudEvent attributes come from the publisher.** The MQTT handler reads `id`, `source`,
   `type`, `subject`, `correlationid` and `causationid` from the MQTT user properties.
2. **The scope comes from `subject`, not from the topic.** `@scope_from_subject` uses the part of
   `subject` before the first `/`. The topic is stored in `transportmetadata` but is not compared
   with the subject. A client allowed to publish to `app/jobs/lineA/commands` can set the subject
   to `lineB` and the processor acts on `lineB`. Topic ACLs therefore do **not** enforce scope
   isolation today.
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
- **Scope the publish permission of each client to its own scope** (`app/jobs/{scope}/commands`),
  and treat that ACL as the only scope boundary until the processor validates the subject against
  the topic.
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
- pass the password from a mounted secret rather than a literal in the manifest

## MessagePack-RPC

The listener defaults to `localhost` and is intended for a sidecar in the same pod. There is no
per-method authorization. If it is exposed beyond the pod, enable `APP_MSGPACK_TLS_CLIENT_AUTH`,
restrict access with a network policy, and keep the set of registered methods minimal.

## Known Gaps

| Gap | Mitigation today |
|---|---|
| Subject is not validated against the topic | Per-scope publish ACLs on the broker |
| Deduplication key is publisher-controlled | Distinct credentials per publisher; broker ACLs |
| TLS and SAT silently skipped when files are missing | Verify the secret and certificate mounts; fail the rollout if they are absent |
| No per-method authorization on MessagePack-RPC | Keep it pod-local or require client certificates |
