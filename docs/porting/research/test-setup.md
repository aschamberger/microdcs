# Integration test setup (docker compose)

Goal (D-15): one compose-based setup that a developer runs locally and CI runs unchanged. Today: `compose.yaml` is a dev stack (Mosquitto, Redis 8, Aspire dashboard, optional app container). The Python integration tests skip themselves when the services are missing (markers in `tests/conftest.py`), and `.github/workflows/ci.yml` has no integration job (it has `test`, `test-next-python` and `audit`).

Proposed shape:

* **Compose is the source of truth.** A separate `compose.test.yaml` (the dev `compose.yaml` stays for interactive work). Services: the system under test (the AOT image built from the repository `Dockerfile`, which also proves that the image builds in CI), Valkey (`valkey-bundle`, D-5), the brokers, and a `tests` service that runs the integration test project. Healthchecks plus `depends_on: condition: service_healthy`; images pinned by digest and updated by Dependabot's docker ecosystem (R-9).
* **Same command everywhere:** `docker compose -f compose.test.yaml up --build --abort-on-container-exit --exit-code-from tests`. CI is a thin wrapper (checkout, buildx cache, the command, upload test results and `docker compose logs` as artifacts on failure).
* **Two modes:** (a) container mode as above for CI and "run exactly what CI runs"; (b) host mode for fast iteration: `docker compose up -d <services>` with published ports, then `dotnet test`. The tests read endpoints from environment variables, so no code differences.
* **Transport matrix through compose profiles:** `mqtt` (Mosquitto, and later a second broker such as EMQX, the AIO-like case), `nats` (JetStream enabled), `amqp` (RabbitMQ 4 or Artemis, decision per Q-ARCH-6), `zenoh` (zenohd router). CI runs a job per profile in parallel; Valkey is always present.
* **Security variants by override files:** `compose.tls.yaml` (TLS, passwords, client certificates) to cover `require_tls` and auth paths, including the "unverified in the session" items of `review.md` (broker-side check that no unacknowledged message remains after dedupe).
* **Isolation:** each test run uses a unique key prefix in Valkey and a unique topic/subject prefix per test, so tests can run in parallel against one stack. Ephemeral volumes (no named volume in the test file).
* **Time-dependent behaviour** (TTL, expiry, dedupe lease): short intervals configured by environment in the test stack, plus a fake clock (`TimeProvider`) where the logic is testable without a broker.
* **Test layers:** unit (no containers), integration (processes in the test host against containers), end-to-end (the real app image driven only through broker messages). The end-to-end scenarios are written against the wire only (publish CloudEvent, expect CloudEvents/state), which keeps them independent of the implementation language. If useful, the same scenarios could run against the Python image for the parity check in Q-TEST-2 (only meaningful for scenarios whose wire format is unchanged; D-2 allows changes, so this is limited).
* **Observability in tests:** an OTLP collector or the Aspire dashboard in the stack to assert trace propagation across transports (traceparent in the metadata) where wanted.
* **Zenoh and native libraries:** the `tests` and app images must contain the zenoh-c native library; part of the spike for Q-ARCH-1.

Open points:

* ~~Where the `tests` service runs~~: decided, inside compose by default (1:1 with CI). Host mode stays as the optional fast-iteration path.
* Testcontainers for .NET is an alternative orchestration. Compose-first keeps one definition for humans and CI; Testcontainers could still start the compose file from code if wanted.
* Runtime of the full matrix in CI (broker count times scenarios); decide which profiles are required checks and which run nightly (`schedule`, as the Python CI does for the next Python version).
